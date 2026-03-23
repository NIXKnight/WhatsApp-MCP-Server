// Package api implements the REST HTTP layer for the WhatsApp bridge.
package api

import "time"

// ---- Error response -----------------------------------------------------

// ErrorResponse is the JSON body returned for all error responses.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// ---- Status -------------------------------------------------------------

// StatusResponse is returned by GET /api/status.
type StatusResponse struct {
	State         string    `json:"state"`
	IsConnected   bool      `json:"is_connected"`
	Uptime        string    `json:"uptime"`
	MessageCount  int64     `json:"message_count"`
	ChatCount     int64     `json:"chat_count"`
	StartedAt     time.Time `json:"started_at"`
}

// ---- Messages -----------------------------------------------------------

// MessageResponse is the JSON representation of a single message.
type MessageResponse struct {
	ID                string    `json:"id"`
	ChatJID           string    `json:"chat_jid"`
	Sender            string    `json:"sender"`
	SenderName        string    `json:"sender_name"`
	Content           string    `json:"content"`
	Timestamp         time.Time `json:"timestamp"`
	IsFromMe          bool      `json:"is_from_me"`
	MediaType         string    `json:"media_type,omitempty"`
	Filename          string    `json:"filename,omitempty"`
	URL               string    `json:"url,omitempty"`
	FileLength        int64     `json:"file_length,omitempty"`
	PushName          string    `json:"push_name,omitempty"`
	QuotedMessageID   string    `json:"quoted_message_id,omitempty"`
	QuotedParticipant string    `json:"quoted_participant,omitempty"`
}

// MessagesResponse wraps a list of messages with pagination metadata.
type MessagesResponse struct {
	Messages []MessageResponse `json:"messages"`
	Total    int               `json:"total"`
	Limit    int               `json:"limit"`
	Offset   int               `json:"offset"`
}

// ---- Chats --------------------------------------------------------------

// ChatResponse is the JSON representation of a single chat.
type ChatResponse struct {
	JID         string    `json:"jid"`
	Name        string    `json:"name"`
	IsGroup     bool      `json:"is_group"`
	UnreadCount int       `json:"unread_count"`
	LastMsgTime time.Time `json:"last_message_time"`
	LastPreview string    `json:"last_message_preview"`
}

// ChatsResponse wraps a list of chats.
type ChatsResponse struct {
	Chats  []ChatResponse `json:"chats"`
	Total  int            `json:"total"`
	Limit  int            `json:"limit"`
	Offset int            `json:"offset"`
}

// UnreadResponse is returned by GET /api/unread.
type UnreadResponse struct {
	Chats []UnreadChatEntry `json:"chats"`
	Total int               `json:"total"`
}

// UnreadChatEntry pairs a chat with its most recent messages.
type UnreadChatEntry struct {
	Chat     ChatResponse      `json:"chat"`
	Messages []MessageResponse `json:"messages"`
}

// UnreadFlatMessage is a single entry in the flat unread messages list.
type UnreadFlatMessage struct {
	ChatJID     string `json:"chatJid"`
	ChatName    string `json:"chatName"`
	IsGroup     bool   `json:"isGroup"`
	MessageID   string `json:"messageId"`
	Participant string `json:"participant"`
	SenderName  string `json:"senderName"`
	Text        string `json:"text"`
	Timestamp   int64  `json:"timestamp"`
}

// UnreadFlatResponse is returned by GET /api/unread?flat=true.
type UnreadFlatResponse struct {
	TotalUnread int                 `json:"totalUnread"`
	Messages    []UnreadFlatMessage `json:"messages"`
}

// ---- Contacts -----------------------------------------------------------

// ContactResponse is the JSON representation of a contact.
type ContactResponse struct {
	JID    string `json:"jid"`
	Name   string `json:"name"`
	Notify string `json:"notify,omitempty"`
	Phone  string `json:"phone,omitempty"`
}

// ContactsResponse wraps a list of contacts.
type ContactsResponse struct {
	Contacts []ContactResponse `json:"contacts"`
	Total    int               `json:"total"`
}

// ---- Groups -------------------------------------------------------------

// GroupParticipant represents a participant in a group.
type GroupParticipant struct {
	JID     string `json:"jid"`
	IsAdmin bool   `json:"is_admin"`
	IsSuperAdmin bool `json:"is_super_admin"`
}

// GroupResponse extends ChatResponse with participant information.
type GroupResponse struct {
	ChatResponse
	Participants []GroupParticipant `json:"participants,omitempty"`
}

// ---- Check (new messages) -----------------------------------------------

// CheckResponse is returned by GET /api/check.
type CheckResponse struct {
	Messages  []MessageResponse `json:"messages"`
	Count     int               `json:"count"`
	Since     time.Time         `json:"since"`
	Timestamp time.Time         `json:"timestamp"`
}

// ---- Send ---------------------------------------------------------------

// SendRequest is the body for POST /api/send.
// Field names match what the Python MCP tools send: "to" / "text".
type SendRequest struct {
	// To is the recipient JID (individual or group).
	To string `json:"to"`
	// Text is the plain-text message body.
	Text string `json:"text"`
	// QuotedMessageID is the ID of the message to quote/reply to.
	QuotedMessageID string `json:"quotedMessageId,omitempty"`
	// QuotedParticipant is the JID of the quoted message sender.
	QuotedParticipant string `json:"quotedParticipant,omitempty"`
	// Mentions is an optional list of JIDs tagged in the message.
	Mentions []string `json:"mentions,omitempty"`
}

// SendResponse is returned by POST /api/send.
// Returns success/message_id to match Python MCP expectations.
type SendResponse struct {
	Success   bool   `json:"success"`
	MessageID string `json:"message_id"`
}

// SendMediaResponse is returned by POST /api/send/media.
// Returns success/message_id to match Python MCP expectations.
type SendMediaResponse struct {
	Success   bool   `json:"success"`
	MessageID string `json:"message_id"`
}

// DownloadRequest is the body for POST /api/download.
type DownloadRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
	OutputDir string `json:"output_dir,omitempty"`
}

// DownloadResponse is returned by POST /api/download.
type DownloadResponse struct {
	FilePath  string `json:"file_path"`
	MediaType string `json:"media_type"`
	FileSize  int64  `json:"file_size"`
}
