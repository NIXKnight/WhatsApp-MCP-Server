package api

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/media"
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

	result, err := media.Download(r.Context(), h.client.WA, msg, outDir)
	if err != nil {
		h.log.Error("download media", "message_id", req.MessageID, "err", err)
		writeError(w, http.StatusInternalServerError, "download failed", "DOWNLOAD_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, DownloadResponse{
		FilePath:  result.Path,
		MediaType: result.MediaType,
		FileSize:  result.FileSize,
	})
}
