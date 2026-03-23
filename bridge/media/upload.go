package media

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// UploadResult holds the constructed waE2E.Message and detected media type.
type UploadResult struct {
	Message   *waE2E.Message
	MediaType string
}

// Upload reads the file at path, uploads it to WhatsApp's media servers, and
// constructs the appropriate waE2E.Message proto ready to send. The mediaType
// parameter overrides auto-detection when non-empty. The ptt flag turns audio
// files into voice notes.
func Upload(ctx context.Context, client *whatsmeow.Client, path, caption, mediaType string, ptt bool) (*UploadResult, error) {
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	// Validate path is absolute to prevent directory traversal.
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("path must be absolute")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file %q: %w", path, err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("file is empty: %q", path)
	}

	ext := strings.ToLower(filepath.Ext(path))
	if ext != "" {
		ext = ext[1:] // strip leading dot
	}

	detectedType, mimeType := detectMediaType(ext, mediaType)

	waMediaType := toWAMediaType(detectedType)
	resp, err := client.Upload(ctx, data, waMediaType)
	if err != nil {
		return nil, fmt.Errorf("whatsmeow upload: %w", err)
	}

	msg, err := buildMessage(detectedType, mimeType, caption, path, data, resp, ptt)
	if err != nil {
		return nil, err
	}

	return &UploadResult{
		Message:   msg,
		MediaType: detectedType,
	}, nil
}

// detectMediaType returns the canonical type and MIME type given a file
// extension and an optional override type string.
func detectMediaType(ext, override string) (mediaType, mimeType string) {
	if override != "" {
		mediaType = strings.ToLower(override)
	} else {
		mediaType = extToMediaType(ext)
	}

	switch mediaType {
	case "image":
		mimeType = extToImageMime(ext)
	case "video":
		mimeType = extToVideoMime(ext)
	case "audio":
		mimeType = "audio/ogg; codecs=opus"
	default:
		mediaType = "document"
		mimeType = "application/octet-stream"
	}
	return
}

func extToMediaType(ext string) string {
	switch ext {
	case "jpg", "jpeg", "png", "gif", "webp":
		return "image"
	case "mp4", "avi", "mov", "mkv", "webm":
		return "video"
	case "ogg", "mp3", "m4a", "aac", "wav", "flac":
		return "audio"
	default:
		return "document"
	}
}

func extToImageMime(ext string) string {
	switch ext {
	case "jpg", "jpeg":
		return "image/jpeg"
	case "png":
		return "image/png"
	case "gif":
		return "image/gif"
	case "webp":
		return "image/webp"
	default:
		return "image/jpeg"
	}
}

func extToVideoMime(ext string) string {
	switch ext {
	case "mp4":
		return "video/mp4"
	case "avi":
		return "video/avi"
	case "mov":
		return "video/quicktime"
	case "mkv":
		return "video/x-matroska"
	case "webm":
		return "video/webm"
	default:
		return "video/mp4"
	}
}

func toWAMediaType(t string) whatsmeow.MediaType {
	switch t {
	case "image":
		return whatsmeow.MediaImage
	case "video":
		return whatsmeow.MediaVideo
	case "audio":
		return whatsmeow.MediaAudio
	default:
		return whatsmeow.MediaDocument
	}
}

// buildMessage constructs the appropriate waE2E.Message based on detected type.
func buildMessage(
	mediaType, mimeType, caption, path string,
	data []byte,
	resp whatsmeow.UploadResponse,
	ptt bool,
) (*waE2E.Message, error) {

	switch mediaType {
	case "image":
		return &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				Caption:       proto.String(caption),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
			},
		}, nil

	case "video":
		return &waE2E.Message{
			VideoMessage: &waE2E.VideoMessage{
				Caption:       proto.String(caption),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
			},
		}, nil

	case "audio":
		var seconds uint32 = 30
		var waveform []byte

		// Attempt OGG Opus analysis for accurate duration and waveform.
		if strings.Contains(mimeType, "ogg") || strings.HasSuffix(strings.ToLower(path), ".ogg") {
			s, wf, err := AnalyzeOggOpus(data)
			if err == nil {
				seconds = s
				waveform = wf
			}
		}

		return &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
				Seconds:       proto.Uint32(seconds),
				PTT:           proto.Bool(ptt),
				Waveform:      waveform,
			},
		}, nil

	default: // document
		title := filepath.Base(path)
		return &waE2E.Message{
			DocumentMessage: &waE2E.DocumentMessage{
				Title:         proto.String(title),
				Caption:       proto.String(caption),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(resp.URL),
				DirectPath:    proto.String(resp.DirectPath),
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    proto.Uint64(resp.FileLength),
			},
		}, nil
	}
}
