package api

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/client"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// Handler holds references to the bridge client and message store, providing
// all REST endpoint handlers.
type Handler struct {
	client    *client.Client
	store     *store.Store
	log       *slog.Logger
	startedAt time.Time
}

// NewHandler creates a Handler bound to the given client and store.
func NewHandler(c *client.Client, s *store.Store, log *slog.Logger) *Handler {
	return &Handler{
		client:    c,
		store:     s,
		log:       log,
		startedAt: time.Now(),
	}
}

// ---- Conversion helpers -------------------------------------------------

func toMessageResponse(m store.MessageRow) MessageResponse {
	resp := MessageResponse{
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
		Snippet:           m.Snippet,
		Transcription:     m.Transcription,
	}
	if !m.TranscribedAt.IsZero() {
		t := m.TranscribedAt
		resp.TranscribedAt = &t
	}
	return resp
}

func toMessageResponses(rows []store.MessageRow) []MessageResponse {
	if rows == nil {
		return []MessageResponse{}
	}
	out := make([]MessageResponse, len(rows))
	for i, r := range rows {
		out[i] = toMessageResponse(r)
	}
	return out
}

func toChatResponse(c store.ChatRow) ChatResponse {
	return ChatResponse{
		JID:         c.JID,
		Name:        c.Name,
		IsGroup:     c.IsGroup,
		UnreadCount: c.UnreadCount,
		LastMsgTime: c.LastMsgTime,
		LastPreview: c.LastPreview,
	}
}

func toChatResponses(rows []store.ChatRow) []ChatResponse {
	if rows == nil {
		return []ChatResponse{}
	}
	out := make([]ChatResponse, len(rows))
	for i, r := range rows {
		out[i] = toChatResponse(r)
	}
	return out
}

func toContactResponses(rows []store.ContactRow) []ContactResponse {
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
