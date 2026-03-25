package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/bridge"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/media"
)

// jidRe validates WhatsApp JIDs accepted by the API.
var jidRe = regexp.MustCompile(`^\d+@(s\.whatsapp\.net|g\.us|lid)$`)

// Handler holds references to the bridge client and message store, providing
// all REST endpoint handlers.
type Handler struct {
	client    *bridge.Client
	store     *bridge.Store
	log       *slog.Logger
	startedAt time.Time
}

// NewHandler creates a Handler bound to the given client and store.
func NewHandler(client *bridge.Client, store *bridge.Store, log *slog.Logger) *Handler {
	return &Handler{
		client:    client,
		store:     store,
		log:       log,
		startedAt: time.Now(),
	}
}

// ---- GET /api/status ----------------------------------------------------

// Status returns the connection state, uptime, and aggregate message counts.
func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	msgCount, _ := h.store.CountMessages()
	chatCount, _ := h.store.CountChats()
	state := h.client.State()
	uptime := time.Since(h.startedAt).Truncate(time.Second)

	writeJSON(w, http.StatusOK, StatusResponse{
		State:        state.String(),
		IsConnected:  state == bridge.StateConnected,
		Uptime:       uptime.String(),
		MessageCount: msgCount,
		ChatCount:    chatCount,
		StartedAt:    h.startedAt,
	})
}

// ---- GET /api/messages --------------------------------------------------

// ListMessages returns messages with optional filters: chat_jid, sender,
// after (Unix ms), before (Unix ms), query, limit, page.
func (h *Handler) ListMessages(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := intParam(q.Get("limit"), 50, 1, 500)
	page := intParam(q.Get("page"), 0, 0, 10000)
	offset := page * limit

	var after, before time.Time
	if v := q.Get("after"); v != "" {
		after = parseTimestamp(v)
	}
	if v := q.Get("before"); v != "" {
		before = parseTimestamp(v)
	}

	chatJID := q.Get("chat_jid")
	if chatJID != "" && !jidRe.MatchString(chatJID) {
		writeError(w, http.StatusBadRequest, "invalid chat_jid format", "INVALID_JID")
		return
	}

	sender := q.Get("sender")

	params := bridge.QueryMessagesParams{
		ChatJID: chatJID,
		Sender:  sender,
		After:   after,
		Before:  before,
		Query:   q.Get("query"),
		Limit:   limit,
		Offset:  offset,
	}

	msgs, err := h.store.ListMessages(params)
	if err != nil {
		h.log.Error("list messages", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query messages", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, MessagesResponse{
		Messages: toMessageResponses(msgs),
		Total:    len(msgs),
		Limit:    limit,
		Offset:   offset,
	})
}

// ---- GET /api/messages/{id}/context -------------------------------------

// MessageContext returns messages surrounding the specified message.
func (h *Handler) MessageContext(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	chatJID := r.URL.Query().Get("chat_jid")
	if chatJID == "" {
		writeError(w, http.StatusBadRequest, "chat_jid query parameter required", "MISSING_PARAM")
		return
	}
	if !jidRe.MatchString(chatJID) {
		writeError(w, http.StatusBadRequest, "invalid chat_jid format", "INVALID_JID")
		return
	}

	contextSize := intParam(r.URL.Query().Get("context"), 5, 1, 50)

	msgs, err := h.store.GetMessageContext(id, chatJID, contextSize)
	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "message not found", "NOT_FOUND")
			return
		}
		h.log.Error("message context", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get message context", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, MessagesResponse{
		Messages: toMessageResponses(msgs),
		Total:    len(msgs),
		Limit:    contextSize * 2,
		Offset:   0,
	})
}

// ---- GET /api/chats -----------------------------------------------------

// ListChats returns all chats with optional name search and pagination.
func (h *Handler) ListChats(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := intParam(q.Get("limit"), 50, 1, 500)
	page := intParam(q.Get("page"), 0, 0, 10000)
	offset := page * limit

	chats, err := h.store.ListChats(q.Get("query"), limit, offset, false)
	if err != nil {
		h.log.Error("list chats", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query chats", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, ChatsResponse{
		Chats:  toChatResponses(chats),
		Total:  len(chats),
		Limit:  limit,
		Offset: offset,
	})
}

// ---- GET /api/chats/{jid} -----------------------------------------------

// GetChat returns a single chat by JID.
func (h *Handler) GetChat(w http.ResponseWriter, r *http.Request) {
	jid := chi.URLParam(r, "jid")
	if !jidRe.MatchString(jid) {
		writeError(w, http.StatusBadRequest, "invalid JID format", "INVALID_JID")
		return
	}

	chat, err := h.store.GetChat(jid)
	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "chat not found", "NOT_FOUND")
			return
		}
		h.log.Error("get chat", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get chat", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, toChatResponse(*chat))
}

// ---- GET /api/contacts --------------------------------------------------

// ListContacts returns all contacts.
func (h *Handler) ListContacts(w http.ResponseWriter, r *http.Request) {
	contacts, err := h.store.ListContacts()
	if err != nil {
		h.log.Error("list contacts", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query contacts", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, ContactsResponse{
		Contacts: toContactResponses(contacts),
		Total:    len(contacts),
	})
}

// ---- GET /api/contacts/{jid} --------------------------------------------

// GetContact returns a single contact by JID.
func (h *Handler) GetContact(w http.ResponseWriter, r *http.Request) {
	jid := chi.URLParam(r, "jid")
	if !jidRe.MatchString(jid) {
		writeError(w, http.StatusBadRequest, "invalid JID format", "INVALID_JID")
		return
	}

	contact, err := h.store.GetContact(jid)
	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "contact not found", "NOT_FOUND")
			return
		}
		h.log.Error("get contact", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get contact", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, ContactResponse{
		JID:    contact.JID,
		Name:   contact.Name,
		Notify: contact.Notify,
		Phone:  contact.Phone,
	})
}

// ---- GET /api/groups ----------------------------------------------------

// ListGroups returns group chats only.
func (h *Handler) ListGroups(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := intParam(q.Get("limit"), 50, 1, 500)
	page := intParam(q.Get("page"), 0, 0, 10000)
	offset := page * limit

	chats, err := h.store.ListChats(q.Get("query"), limit, offset, true)
	if err != nil {
		h.log.Error("list groups", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query groups", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, ChatsResponse{
		Chats:  toChatResponses(chats),
		Total:  len(chats),
		Limit:  limit,
		Offset: offset,
	})
}

// ---- GET /api/groups/{jid} ----------------------------------------------

// GetGroup returns a single group with participant metadata.
func (h *Handler) GetGroup(w http.ResponseWriter, r *http.Request) {
	jidStr := chi.URLParam(r, "jid")
	if !jidRe.MatchString(jidStr) {
		writeError(w, http.StatusBadRequest, "invalid JID format", "INVALID_JID")
		return
	}

	chat, err := h.store.GetChat(jidStr)
	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "group not found", "NOT_FOUND")
			return
		}
		h.log.Error("get group chat", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get group", "DB_ERROR")
		return
	}
	if !chat.IsGroup {
		writeError(w, http.StatusNotFound, "not a group JID", "NOT_FOUND")
		return
	}

	jid, err := types.ParseJID(jidStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot parse JID", "INVALID_JID")
		return
	}

	resp := GroupResponse{
		ChatResponse: toChatResponse(*chat),
	}

	if h.client.State() == bridge.StateConnected {
		if gi, err := h.client.WA.GetGroupInfo(r.Context(), jid); err == nil {
			for _, p := range gi.Participants {
				resp.Participants = append(resp.Participants, GroupParticipant{
					JID:          p.JID.String(),
					IsAdmin:      p.IsAdmin,
					IsSuperAdmin: p.IsSuperAdmin,
				})
			}
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

// ---- GET /api/unread ----------------------------------------------------

// ListUnread returns chats with unread_count > 0, each with their recent messages.
// When query param flat=true is present, returns a flat list of all unread
// messages across all chats (used by the Python get_unread_messages tool).
func (h *Handler) ListUnread(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	flat := strings.ToLower(q.Get("flat")) == "true"

	chats, err := h.store.ListUnreadChats()
	if err != nil {
		h.log.Error("list unread chats", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query unread chats", "DB_ERROR")
		return
	}

	msgLimit := intParam(q.Get("msg_limit"), 5, 1, 50)
	// When in flat mode, fetch more messages per chat for the full inbox view.
	if flat {
		msgLimit = intParam(q.Get("msg_limit"), 20, 1, 100)
	}

	if flat {
		// Return a flat sorted list of all unread messages.
		var flatMsgs []UnreadFlatMessage
		for _, chat := range chats {
			msgs, err := h.store.ListMessages(bridge.QueryMessagesParams{
				ChatJID: chat.JID,
				Limit:   msgLimit,
				Offset:  0,
			})
			if err != nil {
				h.log.Warn("unread flat: failed to fetch messages", "jid", chat.JID, "err", err)
				continue
			}
			for _, m := range msgs {
				flatMsgs = append(flatMsgs, UnreadFlatMessage{
					ChatJID:     chat.JID,
					ChatName:    chat.Name,
					IsGroup:     chat.IsGroup,
					MessageID:   m.ID,
					Participant: m.Sender,
					SenderName:  m.SenderName,
					Text:        m.Content,
					Timestamp:   m.Timestamp.UnixMilli(),
				})
			}
		}
		writeJSON(w, http.StatusOK, UnreadFlatResponse{
			TotalUnread: len(flatMsgs),
			Messages:    flatMsgs,
		})
		return
	}

	entries := make([]UnreadChatEntry, 0, len(chats))
	for _, chat := range chats {
		msgs, err := h.store.ListMessages(bridge.QueryMessagesParams{
			ChatJID: chat.JID,
			Limit:   msgLimit,
			Offset:  0,
		})
		if err != nil {
			h.log.Warn("unread: failed to fetch messages", "jid", chat.JID, "err", err)
		}
		entries = append(entries, UnreadChatEntry{
			Chat:     toChatResponse(chat),
			Messages: toMessageResponses(msgs),
		})
	}

	writeJSON(w, http.StatusOK, UnreadResponse{
		Chats: entries,
		Total: len(entries),
	})
}

// ---- GET /api/check -----------------------------------------------------

// CheckNewMessages returns all messages stored after the given Unix milliseconds
// timestamp (since query param). Supports optional jid query param to restrict
// results to a single chat (used by the Python check_new_messages tool).
func (h *Handler) CheckNewMessages(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sinceParam := q.Get("since")
	var since time.Time
	if sinceParam != "" {
		since = parseTimestamp(sinceParam)
	}
	limit := intParam(q.Get("limit"), 100, 1, 500)

	chatJID := q.Get("jid")
	if chatJID != "" && !jidRe.MatchString(chatJID) {
		writeError(w, http.StatusBadRequest, "invalid jid format", "INVALID_JID")
		return
	}

	msgs, err := h.store.ListMessagesSince(since, chatJID, limit)
	if err != nil {
		h.log.Error("check new messages", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query messages", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, CheckResponse{
		Messages:  toMessageResponses(msgs),
		Count:     len(msgs),
		Since:     since,
		Timestamp: time.Now().UTC(),
	})
}

// ---- POST /api/send -----------------------------------------------------

// SendMessage sends a plain text message to an individual or group JID.
// The request body uses "to"/"text" field names to match the Python MCP client.
func (h *Handler) SendMessage(w http.ResponseWriter, r *http.Request) {
	if h.client.State() != bridge.StateConnected {
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
	if h.client.State() != bridge.StateConnected {
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

// ---- Conversion helpers -------------------------------------------------

func toMessageResponse(m bridge.MessageRow) MessageResponse {
	return MessageResponse{
		ID:                m.ID,
		ChatJID:           m.ChatJID,
		Sender:            m.Sender,
		SenderName:        m.SenderName,
		Content:           m.Content,
		Timestamp:         m.Timestamp,
		IsFromMe:          m.IsFromMe,
		MediaType:         m.MediaType,
		Filename:          m.Filename,
		URL:               m.URL,
		FileLength:        m.FileLength,
		PushName:          m.PushName,
		QuotedMessageID:   m.QuotedMessageID,
		QuotedParticipant: m.QuotedParticipant,
	}
}

func toMessageResponses(rows []bridge.MessageRow) []MessageResponse {
	if rows == nil {
		return []MessageResponse{}
	}
	out := make([]MessageResponse, len(rows))
	for i, r := range rows {
		out[i] = toMessageResponse(r)
	}
	return out
}

func toChatResponse(c bridge.ChatRow) ChatResponse {
	return ChatResponse{
		JID:         c.JID,
		Name:        c.Name,
		IsGroup:     c.IsGroup,
		UnreadCount: c.UnreadCount,
		LastMsgTime: c.LastMsgTime,
		LastPreview: c.LastPreview,
	}
}

func toChatResponses(rows []bridge.ChatRow) []ChatResponse {
	if rows == nil {
		return []ChatResponse{}
	}
	out := make([]ChatResponse, len(rows))
	for i, r := range rows {
		out[i] = toChatResponse(r)
	}
	return out
}

func toContactResponses(rows []bridge.ContactRow) []ContactResponse {
	if rows == nil {
		return []ContactResponse{}
	}
	out := make([]ContactResponse, len(rows))
	for i, r := range rows {
		out[i] = ContactResponse{
			JID:    r.JID,
			Name:   r.Name,
			Notify: r.Notify,
			Phone:  r.Phone,
		}
	}
	return out
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

// normaliseJID converts a bare phone number to a WhatsApp individual JID.
// If the input already contains @, it is returned unchanged.
func normaliseJID(s string) string {
	if strings.Contains(s, "@") {
		return s
	}
	// Strip any non-digit characters (spaces, dashes, plus signs).
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
	if digits == "" {
		return s
	}
	return digits + "@s.whatsapp.net"
}

// ---- Query parameter helpers --------------------------------------------

// intParam parses an integer query parameter with min/max bounds.
func intParam(s string, def, min, max int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// parseTimestamp parses a Unix milliseconds timestamp string.
func parseTimestamp(s string) time.Time {
	if s == "" || s == "0" {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}
