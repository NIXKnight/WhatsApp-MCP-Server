package media

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.mau.fi/whatsmeow"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// DownloadResult holds the local file path and metadata for a downloaded media file.
type DownloadResult struct {
	Path      string
	MediaType string
	Filename  string
	FileSize  int64
}

// Download retrieves the media referenced by msg, saves it to
// outDir/<chat_jid_safe>/<filename>, and returns the result.
// outDir defaults to dataDir/media when the caller passes the bridge data dir,
// but can be any writable directory.
// If the file already exists locally it is returned without re-downloading.
func Download(ctx context.Context, client *whatsmeow.Client, msg *store.MessageRow, outDir string) (*DownloadResult, error) {
	if msg.MediaType == "" {
		return nil, fmt.Errorf("message has no media")
	}

	localPath := ResolveLocalPath(outDir, msg.ChatJID, msg.Filename)
	destDir := filepath.Dir(localPath)
	if err := os.MkdirAll(destDir, 0700); err != nil {
		// LOCAL destination failure: the caller-supplied output dir cannot be
		// created (unwritable mount, denied permission, full disk). This is NOT
		// a media-availability failure — return a typed LocalDestError so the
		// handler skips the retry/permanent-fail bookkeeping and tells the
		// caller its output_dir is the problem.
		return nil, newLocalDestError("create media dir", destDir, err)
	}

	// Return the cached file if it already exists.
	if info, err := os.Stat(localPath); err == nil {
		return &DownloadResult{
			Path:      localPath,
			MediaType: msg.MediaType,
			Filename:  msg.Filename,
			FileSize:  info.Size(),
		}, nil
	}

	// Check we have enough info to download.
	if msg.URL == "" || len(msg.MediaKey) == 0 || len(msg.FileSHA256) == 0 || len(msg.FileEncSHA256) == 0 {
		return nil, fmt.Errorf("incomplete media metadata for message %s in chat %s", msg.ID, msg.ChatJID)
	}

	waMediaType := toWAMediaType(msg.MediaType)

	// Build a DownloadableMessage from the stored database fields.
	dl := &storedMediaDownloader{
		url:           msg.URL,
		directPath:    extractDirectPath(msg.URL),
		mediaKey:      msg.MediaKey,
		fileLength:    uint64(msg.FileLength),
		fileSHA256:    msg.FileSHA256,
		fileEncSHA256: msg.FileEncSHA256,
		mediaType:     waMediaType,
	}

	// Check for cancellation before the (potentially slow) network download.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("download cancelled: %w", err)
	}

	data, err := client.Download(ctx, dl)
	if err != nil {
		return nil, fmt.Errorf("whatsmeow download: %w", err)
	}

	if err := os.WriteFile(localPath, data, 0600); err != nil {
		// The media decrypted fine (client.Download succeeded above); only the
		// local write failed. Treat it as a destination problem, not a media
		// failure, so an unwritable target never poisons availability state.
		return nil, newLocalDestError("write file", destDir, err)
	}

	return &DownloadResult{
		Path:      localPath,
		MediaType: msg.MediaType,
		Filename:  msg.Filename,
		FileSize:  int64(len(data)),
	}, nil
}

// storedMediaDownloader implements whatsmeow.DownloadableMessage using fields
// retrieved from the SQLite message store.
type storedMediaDownloader struct {
	url           string
	directPath    string
	mediaKey      []byte
	fileLength    uint64
	fileSHA256    []byte
	fileEncSHA256 []byte
	mediaType     whatsmeow.MediaType
}

func (d *storedMediaDownloader) GetDirectPath() string             { return d.directPath }
func (d *storedMediaDownloader) GetURL() string                    { return d.url }
func (d *storedMediaDownloader) GetMediaKey() []byte               { return d.mediaKey }
func (d *storedMediaDownloader) GetFileLength() uint64             { return d.fileLength }
func (d *storedMediaDownloader) GetFileSHA256() []byte             { return d.fileSHA256 }
func (d *storedMediaDownloader) GetFileEncSHA256() []byte          { return d.fileEncSHA256 }
func (d *storedMediaDownloader) GetMediaType() whatsmeow.MediaType { return d.mediaType }

// extractDirectPath extracts the URL path component from a WhatsApp media URL.
// Example: https://mmg.whatsapp.net/v/t62.7118-24/file.enc?... -> /v/t62.7118-24/file.enc
func extractDirectPath(rawURL string) string {
	parts := strings.SplitN(rawURL, ".net/", 2)
	if len(parts) < 2 {
		return rawURL
	}
	pathPart := parts[1]
	// Remove query string.
	if idx := strings.Index(pathPart, "?"); idx >= 0 {
		pathPart = pathPart[:idx]
	}
	return "/" + pathPart
}

// ResolveLocalPath computes the on-disk path where media for chatJID/filename
// lives under outDir, applying the same rules Download uses to write it:
// outDir gains a trailing "media" segment unless it already ends in one, the
// chat JID is sanitised into a sub-directory, and only the base name of
// filename is used. Sharing this between Download and callers that want to find
// or place a cached copy keeps the two from drifting apart.
func ResolveLocalPath(outDir, chatJID, filename string) string {
	// If outDir looks like the bridge dataDir (no "media" suffix), append it.
	if !strings.HasSuffix(filepath.ToSlash(outDir), "/media") {
		outDir = filepath.Join(outDir, "media")
	}
	return filepath.Join(outDir, sanitizeJID(chatJID), filepath.Base(filename))
}

// sanitizeJID replaces characters that are invalid in file system paths.
func sanitizeJID(jid string) string {
	r := strings.NewReplacer(
		":", "_",
		"/", "_",
		"\\", "_",
		" ", "_",
	)
	return r.Replace(jid)
}
