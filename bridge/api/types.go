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
	State        string    `json:"state"`
	IsConnected  bool      `json:"is_connected"`
	Uptime       string    `json:"uptime"`
	MessageCount int64     `json:"message_count"`
	ChatCount    int64     `json:"chat_count"`
	StartedAt    time.Time `json:"started_at"`
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
	Snippet           string    `json:"snippet,omitempty"`
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

// SimilarContactPairResponse is a single pair of contacts with similar names.
type SimilarContactPairResponse struct {
	JIDA       string  `json:"jid_a"`
	NameA      string  `json:"name_a"`
	JIDB       string  `json:"jid_b"`
	NameB      string  `json:"name_b"`
	Similarity float64 `json:"similarity"`
}

// SimilarContactsResponse is returned by GET /api/contacts/similar.
type SimilarContactsResponse struct {
	Pairs []SimilarContactPairResponse `json:"pairs"`
	Total int                          `json:"total"`
}

// ---- Groups -------------------------------------------------------------

// GroupParticipant represents a participant in a group.
type GroupParticipant struct {
	JID          string `json:"jid"`
	IsAdmin      bool   `json:"is_admin"`
	IsSuperAdmin bool   `json:"is_super_admin"`
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

// ---- Reaction / edit / revoke -------------------------------------------

// ReactRequest is the body for POST /api/send/reaction.
type ReactRequest struct {
	// ChatJID is the chat containing the target message.
	ChatJID string `json:"chat_jid"`
	// MessageID is the ID of the message being reacted to.
	MessageID string `json:"message_id"`
	// Emoji is the reaction; an empty string removes a prior reaction.
	Emoji string `json:"emoji"`
	// Sender is the author of the target message. Empty means it is our own
	// message.
	Sender string `json:"sender,omitempty"`
}

// EditRequest is the body for POST /api/send/edit.
type EditRequest struct {
	// ChatJID is the chat containing the message to edit.
	ChatJID string `json:"chat_jid"`
	// MessageID is the ID of the message to edit (must be one we sent).
	MessageID string `json:"message_id"`
	// NewText is the replacement body.
	NewText string `json:"new_text"`
}

// RevokeRequest is the body for POST /api/send/revoke.
type RevokeRequest struct {
	// ChatJID is the chat containing the message to revoke.
	ChatJID string `json:"chat_jid"`
	// MessageID is the ID of the message to revoke.
	MessageID string `json:"message_id"`
	// Sender is the author of the target message. Empty revokes our own
	// message; set it when an admin revokes another member's message.
	Sender string `json:"sender,omitempty"`
}

// ---- Trigger check ------------------------------------------------------

// TriggerFilters controls optional filtering for the batch trigger check.
type TriggerFilters struct {
	// MentionJID, when set, keeps only messages referencing that JID.
	MentionJID string `json:"mention_jid,omitempty"`
	// SenderJIDs, when non-empty, keeps only messages from those senders.
	SenderJIDs []string `json:"sender_jids,omitempty"`
}

// TriggerRequest is the body for POST /api/check/triggers.
type TriggerRequest struct {
	// JIDs is the list of chats to check (required, non-empty).
	JIDs []string `json:"jids"`
	// Filters narrows which messages are returned.
	Filters TriggerFilters `json:"filters"`
	// Limit caps the messages returned per chat (default 100).
	Limit int `json:"limit,omitempty"`
	// DryRun reports unseen messages without advancing watermarks.
	DryRun bool `json:"dry_run,omitempty"`
}

// TriggerGroupResult holds the unseen messages for a single chat in the
// trigger response.
type TriggerGroupResult struct {
	Count    int               `json:"count"`
	Messages []MessageResponse `json:"messages"`
}

// TriggerResponse is returned by POST /api/check/triggers.
type TriggerResponse struct {
	Total  int                           `json:"total"`
	Groups map[string]TriggerGroupResult `json:"groups"`
}

// ---- Telemetry ----------------------------------------------------------

// ToolCallRequest is the body for POST /api/telemetry/tool. It records one MCP
// tool invocation's latency and outcome.
type ToolCallRequest struct {
	ToolName   string `json:"tool_name"`
	DurationMs int    `json:"duration_ms"`
	Success    bool   `json:"success"`
	ErrorMsg   string `json:"error_msg,omitempty"`
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

// ---- Hybrid search ------------------------------------------------------

// HybridSearchRequest is the body for POST /api/search.
type HybridSearchRequest struct {
	Query     string    `json:"query"`
	Embedding []float32 `json:"embedding"`
	ChatJID   string    `json:"chat_jid,omitempty"`
	Limit     int       `json:"limit,omitempty"`
}

// SearchResultResponse is a single result from hybrid search.
type SearchResultResponse struct {
	ID         string    `json:"id"`
	ChatJID    string    `json:"chat_jid"`
	Content    string    `json:"content"`
	Timestamp  time.Time `json:"timestamp"`
	SenderName string    `json:"sender_name"`
	Score      float64   `json:"score"`
	Snippet    string    `json:"snippet,omitempty"`
	MatchType  string    `json:"match_type"`
}

// HybridSearchResponse is returned by POST /api/search.
type HybridSearchResponse struct {
	Results []SearchResultResponse `json:"results"`
	Total   int                    `json:"total"`
}

// ---- Chat topic search --------------------------------------------------

// ChatTopicSearchRequest is the body for POST /api/chats/search.
type ChatTopicSearchRequest struct {
	Embedding []float32 `json:"embedding"`
	Limit     int       `json:"limit,omitempty"`
}

// ChatWithScoreResponse is a chat paired with its relevance score.
type ChatWithScoreResponse struct {
	ChatResponse
	Relevance float64 `json:"relevance"`
}

// ChatTopicSearchResponse is returned by POST /api/chats/search.
type ChatTopicSearchResponse struct {
	Results []ChatWithScoreResponse `json:"results"`
	Total   int                     `json:"total"`
}

// ---- Similar messages ---------------------------------------------------

// MessageWithScoreResponse is a message paired with a distance score.
type MessageWithScoreResponse struct {
	ID         string    `json:"id"`
	ChatJID    string    `json:"chat_jid"`
	Content    string    `json:"content"`
	Timestamp  time.Time `json:"timestamp"`
	SenderName string    `json:"sender_name"`
	Distance   float64   `json:"distance"`
}

// SimilarMessagesResponse is returned by GET /api/messages/{id}/similar.
type SimilarMessagesResponse struct {
	Results []MessageWithScoreResponse `json:"results"`
	Total   int                        `json:"total"`
}

// ---- Compact messages ---------------------------------------------------

// CompactMessageResponse is a reduced-field message projection
// for token-efficient context windows. Used when ?compact=true.
type CompactMessageResponse struct {
	ID          string `json:"id"`
	SenderName  string `json:"name"`
	Content     string `json:"text"`
	Timestamp   string `json:"ts"`
	QuotedMsgID string `json:"quoted_id,omitempty"`
	QuotedBy    string `json:"quoted_by,omitempty"`
}

// ---- Embeddings ---------------------------------------------------------

// UpsertEmbeddingRequest is the payload for PUT /api/messages/{id}/embedding.
type UpsertEmbeddingRequest struct {
	ChatJID   string    `json:"chat_jid"`
	Embedding []float32 `json:"embedding"`
}
