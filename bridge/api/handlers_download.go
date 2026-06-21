package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"strconv"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/media"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// ---- POST /api/download -------------------------------------------------

// DownloadMedia downloads a media file referenced in a stored message and
// returns its local path. Accepts optional output_dir to override the default
// media directory.
func (h *Handler) DownloadMedia(w http.ResponseWriter, r *http.Request) {
	var req DownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "INVALID_JSON")
		return
	}

	if req.MessageID == "" || req.ChatJID == "" {
		writeError(w, http.StatusBadRequest, "message_id and chat_jid are required", "MISSING_FIELD")
		return
	}
	if !jidRe.MatchString(req.ChatJID) {
		writeError(w, http.StatusBadRequest, "invalid chat_jid format", "INVALID_JID")
		return
	}

	msg, err := h.store.GetMessageByID(req.MessageID, req.ChatJID)
	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "message not found", "NOT_FOUND")
			return
		}
		h.log.Error("get message for download", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to retrieve message", "DB_ERROR")
		return
	}

	if msg.MediaType == "" {
		writeError(w, http.StatusBadRequest, "message has no media", "NOT_MEDIA")
		return
	}

	// Resolve output directory: use caller-supplied path (tilde-expanded) or default.
	outDir := h.client.DataDir()
	if req.OutputDir != "" {
		outDir = media.ExpandTilde(req.OutputDir)
	}

	// Cache-first: WhatsApp media is immutable, so a copy we already have on disk
	// is always correct and never expires. A prior on-demand download or the
	// background media-retry worker stamps messages_media.local_path on success;
	// once set, the worker stops retrying (its predicate is local_path IS NULL).
	// Re-downloading instead would hit the now-stale CDN URL and fail with 503,
	// even though the bytes are sitting on disk. So if local_path is set AND the
	// file still exists, serve it directly. Only a genuine cache miss (no
	// local_path, or the file was removed) falls through to the network path.
	served, ok, destErr := h.serveCachedMedia(req, msg, outDir)
	if destErr != nil {
		// The cached bytes provably exist but could not be placed in the
		// caller's output_dir. The media is fine; the destination is not. Report
		// that without touching availability state or re-running the network
		// path (which could falsely mark the media permanently unavailable).
		writeOutputDirUnwritable(w, destErr)
		return
	}
	if ok {
		writeJSON(w, http.StatusOK, served)
		return
	}

	result, err := media.Download(r.Context(), h.client.WA, msg, outDir)
	if err != nil {
		// A LOCAL destination failure (unwritable output_dir, read-only mount,
		// full disk) is not a media-availability failure. Do NOT bump the
		// attempt counter, do NOT mark permanent, and do NOT return 410/503 —
		// that would falsely retire media that is perfectly downloadable. Tell
		// the caller its output_dir is the problem instead.
		if dest, isLocal := media.AsLocalDestError(err); isLocal {
			writeOutputDirUnwritable(w, dest)
			return
		}

		// Seed the retry queue: bump the attempt counter and let the store decide
		// permanence atomically. The background media-retry worker will pick this
		// row up on its next cycle (download_attempts is now > 0). The cross-cycle
		// time gap is the recovery mechanism for transient 403/410 (stale URL).
		structural := media.ClassifyDownloadError(err)
		permanent, derr := h.store.MarkMediaDownloadFailed(
			req.MessageID, req.ChatJID, err.Error(), structural, h.maxDownloadAttempts,
		)
		if derr != nil {
			h.log.Error("record download failure", "message_id", req.MessageID, "err", derr)
		}

		// Log only a short error class — never the URL, direct path, or media key.
		h.log.Warn("download media failed",
			"message_id", req.MessageID,
			"class", media.DownloadErrorClass(err),
			"permanent", permanent,
		)

		if permanent {
			writeError(w, http.StatusGone,
				"media is permanently unavailable and will not be retried",
				"MEDIA_PERMANENTLY_UNAVAILABLE")
			return
		}
		writeError(w, http.StatusServiceUnavailable,
			"media download failed; a background retry is pending",
			"DOWNLOAD_RETRY_PENDING")
		return
	}

	// Success: stamp local_path so the retry worker skips this row.
	if err := h.store.MarkMediaDownloaded(req.MessageID, req.ChatJID, result.Path); err != nil {
		h.log.Warn("record successful download", "message_id", req.MessageID, "err", err)
	}

	writeJSON(w, http.StatusOK, DownloadResponse{
		FilePath:  result.Path,
		MediaType: result.MediaType,
		FileSize:  result.FileSize,
	})
}

// serveCachedMedia returns a DownloadResponse built from the already-downloaded
// copy of the media when one exists on disk, reporting ok == true on a cache
// hit. It consults messages_media.local_path (the canonical location stamped on
// a successful download) rather than re-deriving a path, so it finds the file
// regardless of which output directory the original download targeted.
//
// When the caller requested a specific output_dir that differs from where the
// cached file lives, the file is copied there and the new path is returned, so
// the caller always gets a file at a path it can read. When no override is in
// effect — or the cached file already sits at the requested path — local_path is
// returned as-is and the canonical pointer is left untouched (re-stamping it to
// a caller-supplied transient directory would be wrong and would not change the
// worker's already-satisfied predicate).
//
// The error return distinguishes the third outcome from a plain cache miss:
//   - (resp, true, nil):  cache hit, resp is ready to serve.
//   - ({}, false, nil):   cache miss (no row, no local_path, the file is gone,
//     or the cached source could not be opened); the caller falls through to
//     the network download path with its existing failure handling.
//   - ({}, false, err):   the cached file EXISTS but could not be copied into
//     the caller's output_dir because that destination is unwritable (err is a
//     media.LocalDestError). The caller MUST NOT fall through — the bytes are
//     present, so re-running the network path would be pointless and could
//     falsely mark the media permanently unavailable. The caller reports the
//     unwritable output_dir directly.
//
// On a successful serve the media is provably available, so any stale failure
// state on the row (a download_permanently_failed wrongly set by an earlier
// destination error) is cleared here, letting the row self-heal.
func (h *Handler) serveCachedMedia(req DownloadRequest, msg *store.MessageRow, outDir string) (DownloadResponse, bool, *media.LocalDestError) {
	mediaRow, err := h.store.GetMessageMedia(req.MessageID, req.ChatJID)
	if err != nil {
		// No media row is an expected miss; anything else is logged but still
		// treated as a miss so the request can proceed to the download path.
		if err != sql.ErrNoRows {
			h.log.Warn("lookup cached media", "message_id", req.MessageID, "err", err)
		}
		return DownloadResponse{}, false, nil
	}
	if mediaRow.LocalPath == "" {
		return DownloadResponse{}, false, nil
	}

	// local_path is set but the file may have been removed since: stat it and
	// treat a missing/inaccessible file (or a directory) as a cache miss.
	info, err := os.Stat(mediaRow.LocalPath)
	if err != nil || info.IsDir() {
		return DownloadResponse{}, false, nil
	}

	servedPath := mediaRow.LocalPath
	size := info.Size()

	// If the caller wants the file in a different directory than the cached
	// copy, copy it there and serve the new path.
	wantPath := media.ResolveLocalPath(outDir, msg.ChatJID, msg.Filename)
	if wantPath != servedPath {
		n, cerr := media.CopyFile(servedPath, wantPath)
		if cerr != nil {
			if dest, isLocal := media.AsLocalDestError(cerr); isLocal {
				// The cached bytes exist but the caller's output_dir is
				// unwritable. Surface that as a hard destination error instead
				// of falling through to the network path (which would re-fetch
				// already-present bytes and could mis-flag them permanent).
				h.log.Warn("cached media present but output dir unwritable",
					"message_id", req.MessageID, "dir", dest.Dir)
				return DownloadResponse{}, false, dest
			}
			// The destination was fine but the cached source could not be read
			// (it vanished between stat and open): a genuine cache miss. Fall
			// through to the download path.
			h.log.Warn("copy cached media to output dir", "message_id", req.MessageID, "err", cerr)
			return DownloadResponse{}, false, nil
		}
		servedPath = wantPath
		size = n
	}

	// The serve proves the media is available. If the row still carries stale
	// failure state from an earlier (likely destination-caused) failure, clear
	// it by re-stamping the CANONICAL local_path — never the transient copy
	// path — so download_permanently_failed flips back to FALSE and the attempt
	// counter resets. Guarded so a healthy row incurs no write.
	if mediaRow.DownloadPermanentlyFailed || mediaRow.DownloadAttempts > 0 {
		if herr := h.store.MarkMediaDownloaded(req.MessageID, req.ChatJID, mediaRow.LocalPath); herr != nil {
			// Healing is best-effort; the serve still succeeds.
			h.log.Warn("clear stale media failure state on cache serve",
				"message_id", req.MessageID, "err", herr)
		} else {
			h.log.Info("cleared stale media failure state on cache serve",
				"message_id", req.MessageID)
		}
	}

	h.log.Debug("served cached media", "message_id", req.MessageID, "copied", servedPath != mediaRow.LocalPath)
	return DownloadResponse{
		FilePath:  servedPath,
		MediaType: msg.MediaType,
		FileSize:  size,
	}, true, nil
}

// writeOutputDirUnwritable reports a failure to write the caller-supplied
// output directory. It is a destination problem, not a media-availability
// problem: the response carries HTTP 500 with code OUTPUT_DIR_UNWRITABLE and
// names the directory at fault, and — critically — no media-availability state
// is touched (no attempt bump, no permanent-fail, no 410/503). The directory
// path is operator-supplied configuration, not a secret, so naming it is safe.
func writeOutputDirUnwritable(w http.ResponseWriter, dest *media.LocalDestError) {
	msg := "output directory is not writable"
	if dest != nil && dest.Dir != "" {
		msg = "output directory " + strconv.Quote(dest.Dir) + " is not writable; check the output_dir you requested"
	}
	writeError(w, http.StatusInternalServerError, msg, "OUTPUT_DIR_UNWRITABLE")
}
