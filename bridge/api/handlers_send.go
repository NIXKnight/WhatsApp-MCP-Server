package api

import (
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/client"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/media"
)

// ---- POST /api/send -----------------------------------------------------

// SendMessage sends a plain text message to an individual or group JID.
// The request body uses "to"/"text" field names to match the Python MCP client.
func (h *Handler) SendMessage(w http.ResponseWriter, r *http.Request) {
	if h.client.State() != client.StateConnected {
		writeError(w, http.StatusServiceUnavailable, "not connected to WhatsApp", "NOT_CONNECTED")
		return
	}

	var req SendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "INVALID_JSON")
		return
	}

	if req.To == "" {
		writeError(w, http.StatusBadRequest, "to is required", "MISSING_FIELD")
		return
	}

	// Normalise bare phone numbers to JID form.
	toJID := normaliseJID(req.To)

	if !jidRe.MatchString(toJID) {
		writeError(w, http.StatusBadRequest, "invalid JID format", "INVALID_JID")
		return
	}
	if req.Text == "" {
		writeError(w, http.StatusBadRequest, "text is required", "MISSING_FIELD")
		return
	}

	jid, err := types.ParseJID(toJID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot parse JID", "INVALID_JID")
		return
	}

	ephemeral := h.getEphemeralExpiry(r.Context(), jid)
	msg := buildTextMessage(req.Text, req.QuotedMessageID, req.QuotedParticipant, req.Mentions, ephemeral)

	resp, err := h.client.WA.SendMessage(r.Context(), jid, msg)
	if err != nil {
		h.log.Error("send message", "jid", toJID, "err", err)
		writeError(w, http.StatusInternalServerError, "send failed", "SEND_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, SendResponse{
		Success:   true,
		MessageID: resp.ID,
	})
}

// ---- POST /api/send/media -----------------------------------------------

// SendMedia sends a media file (image, video, audio, document, or PTT voice note).
// Accepts multipart/form-data with fields: to, media_type, caption (optional),
// ptt (optional, "true"), and file (the file upload). This matches what the
// Python MCP client sends via post_multipart.
func (h *Handler) SendMedia(w http.ResponseWriter, r *http.Request) {
	if h.client.State() != client.StateConnected {
		writeError(w, http.StatusServiceUnavailable, "not connected to WhatsApp", "NOT_CONNECTED")
		return
	}

	// Parse multipart form (max 100 MB).
	if err := r.ParseMultipartForm(100 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "expected multipart/form-data", "INVALID_FORM")
		return
	}

	toRaw := r.FormValue("to")
	if toRaw == "" {
		writeError(w, http.StatusBadRequest, "to is required", "MISSING_FIELD")
		return
	}
	toJID := normaliseJID(toRaw)
	if !jidRe.MatchString(toJID) {
		writeError(w, http.StatusBadRequest, "invalid JID format", "INVALID_JID")
		return
	}

	jid, err := types.ParseJID(toJID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot parse JID", "INVALID_JID")
		return
	}

	caption := r.FormValue("caption")
	mediaType := r.FormValue("media_type")
	ptt := strings.ToLower(r.FormValue("ptt")) == "true"

	// Retrieve the uploaded file.
	var (
		fileHeader *multipart.FileHeader
	)
	if r.MultipartForm != nil && r.MultipartForm.File != nil {
		if fhs := r.MultipartForm.File["file"]; len(fhs) > 0 {
			fileHeader = fhs[0]
		}
	}
	if fileHeader == nil {
		writeError(w, http.StatusBadRequest, "file upload is required", "MISSING_FIELD")
		return
	}

	// Save the uploaded file to a temp path in the data directory so the
	// media.Upload helper can read it from disk.
	tmpPath, cleanup, err := media.SaveUploadedFile(fileHeader, h.client.DataDir())
	if err != nil {
		h.log.Error("save uploaded file", "name", fileHeader.Filename, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to save uploaded file", "UPLOAD_ERROR")
		return
	}
	defer cleanup()

	// Use the original filename for type detection if media_type was omitted.
	if mediaType == "" {
		mediaType = media.DetectTypeFromFilename(filepath.Base(fileHeader.Filename))
	}

	result, err := media.Upload(r.Context(), h.client.WA, tmpPath, caption, mediaType, ptt)
	if err != nil {
		h.log.Error("upload media", "media_type", mediaType, "err", err)
		writeError(w, http.StatusInternalServerError, "upload failed", "UPLOAD_ERROR")
		return
	}

	ephemeral := h.getEphemeralExpiry(r.Context(), jid)
	if ephemeral > 0 {
		injectEphemeralExpiry(result.Message, ephemeral)
	}

	resp, err := h.client.WA.SendMessage(r.Context(), jid, result.Message)
	if err != nil {
		h.log.Error("send media message", "jid", toJID, "err", err)
		writeError(w, http.StatusInternalServerError, "send failed", "SEND_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, SendMediaResponse{
		Success:   true,
		MessageID: resp.ID,
	})
}

// getEphemeralExpiry returns the disappearing message timer (in seconds) for a
// group JID, or 0 if the JID is not a group or ephemeral messaging is not
// enabled.
func (h *Handler) getEphemeralExpiry(ctx context.Context, jid types.JID) uint32 {
	if jid.Server != types.GroupServer {
		return 0
	}
	info, err := h.client.WA.GetGroupInfo(ctx, jid)
	if err != nil || !info.IsEphemeral {
		return 0
	}
	return info.DisappearingTimer
}

// buildTextMessage constructs the waE2E.Message proto for a text send,
// optionally with a quoted message context and/or mentions.
func buildTextMessage(text, quotedID, quotedParticipant string, mentions []string, ephemeralExpiry uint32) *waE2E.Message {
	needsExtended := quotedID != "" || len(mentions) > 0 || ephemeralExpiry > 0

	if !needsExtended {
		return &waE2E.Message{
			Conversation: proto.String(text),
		}
	}

	ext := &waE2E.ExtendedTextMessage{
		Text: proto.String(text),
	}

	if quotedID != "" || len(mentions) > 0 || ephemeralExpiry > 0 {
		ci := &waE2E.ContextInfo{}
		if quotedID != "" {
			ci.StanzaID = proto.String(quotedID)
			ci.Participant = proto.String(quotedParticipant)
		}
		if len(mentions) > 0 {
			ci.MentionedJID = mentions
		}
		if ephemeralExpiry > 0 {
			ci.Expiration = proto.Uint32(ephemeralExpiry)
		}
		ext.ContextInfo = ci
	}

	return &waE2E.Message{ExtendedTextMessage: ext}
}

// injectEphemeralExpiry sets ContextInfo.Expiration on the inner media message
// proto so that the message participates in the chat's disappearing timer.
func injectEphemeralExpiry(msg *waE2E.Message, expiry uint32) {
	setExpiry := func(ci **waE2E.ContextInfo) {
		if *ci == nil {
			*ci = &waE2E.ContextInfo{}
		}
		(*ci).Expiration = proto.Uint32(expiry)
	}
	switch {
	case msg.ImageMessage != nil:
		setExpiry(&msg.ImageMessage.ContextInfo)
	case msg.AudioMessage != nil:
		setExpiry(&msg.AudioMessage.ContextInfo)
	case msg.VideoMessage != nil:
		setExpiry(&msg.VideoMessage.ContextInfo)
	case msg.DocumentMessage != nil:
		setExpiry(&msg.DocumentMessage.ContextInfo)
	case msg.StickerMessage != nil:
		setExpiry(&msg.StickerMessage.ContextInfo)
	}
}
