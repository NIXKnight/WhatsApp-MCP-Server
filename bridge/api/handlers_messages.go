package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// ---- GET /api/messages --------------------------------------------------

// ListMessages returns messages with optional filters: chat_jid, sender,
// after (Unix ms), before (Unix ms), query, search_mode, limit, page.
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

	searchMode := q.Get("search_mode")
	if searchMode == "" {
		searchMode = "ilike"
	}

	params := store.QueryMessagesParams{
		ChatJID:    chatJID,
		Sender:     sender,
		After:      after,
		Before:     before,
		Query:      q.Get("query"),
		SearchMode: searchMode,
		Limit:      limit,
		Offset:     offset,
	}

	msgs, err := h.store.ListMessages(params)
	if err != nil {
		h.log.Error("list messages", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query messages", "DB_ERROR")
		return
	}

	if q.Get("compact") == "true" {
		compact := make([]CompactMessageResponse, len(msgs))
		for i, m := range msgs {
			compact[i] = CompactMessageResponse{
				ID:            m.ID,
				SenderName:    m.SenderName,
				Content:       m.Content,
				Timestamp:     m.Timestamp.Format(time.RFC3339),
				QuotedMsgID:   m.QuotedMessageID,
				QuotedBy:      m.QuotedParticipant,
				Transcription: m.Transcription,
			}
		}
		writeJSON(w, http.StatusOK, compact)
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

// ---- GET /api/messages/{id}/similar -------------------------------------

// SimilarMessages returns messages with embeddings closest to the specified message.
func (h *Handler) SimilarMessages(w http.ResponseWriter, r *http.Request) {
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

	limit := intParam(r.URL.Query().Get("limit"), 10, 1, 100)

	results, err := h.store.FindSimilarMessages(id, chatJID, limit)
	if err != nil {
		h.log.Error("similar messages", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to find similar messages", "DB_ERROR")
		return
	}

	out := make([]MessageWithScoreResponse, len(results))
	for i, r := range results {
		out[i] = MessageWithScoreResponse{
			ID:         r.ID,
			ChatJID:    r.ChatJID,
			Content:    r.Content,
			Timestamp:  r.Timestamp,
			SenderName: r.SenderName,
			Distance:   r.Distance,
		}
	}

	writeJSON(w, http.StatusOK, SimilarMessagesResponse{
		Results: out,
		Total:   len(out),
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

	var msgs []store.MessageRow
	var err error

	jidsParam := q.Get("jids")
	if jidsParam != "" {
		jids := strings.Split(jidsParam, ",")
		msgs, err = h.store.ListMessagesSinceMultiChat(since, jids, limit)
	} else {
		msgs, err = h.store.ListMessagesSince(since, chatJID, limit)
	}
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

// ---- POST /api/check/triggers -------------------------------------------

// CheckTriggers performs a batch trigger check across multiple chats. For each
// JID it returns the inbound messages received since that chat's server-side
// watermark, optionally filtered by sender and mention, then advances the
// watermark. When dry_run is true the watermarks are not advanced, so the same
// messages would be returned on a subsequent call.
func (h *Handler) CheckTriggers(w http.ResponseWriter, r *http.Request) {
	var req TriggerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "INVALID_JSON")
		return
	}
	if len(req.JIDs) == 0 {
		writeError(w, http.StatusBadRequest, "jids is required and must not be empty", "MISSING_FIELD")
		return
	}
	for _, jid := range req.JIDs {
		if !jidRe.MatchString(jid) {
			writeError(w, http.StatusBadRequest, "invalid jid: "+jid, "INVALID_JID")
			return
		}
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}

	filters := store.TriggerFilters{
		MentionJID: req.Filters.MentionJID,
		SenderJIDs: req.Filters.SenderJIDs,
	}

	res, err := h.store.CheckTriggersMulti(req.JIDs, filters, limit, req.DryRun)
	if err != nil {
		h.log.Error("check triggers", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to check triggers", "DB_ERROR")
		return
	}

	groups := make(map[string]TriggerGroupResult, len(res.Groups))
	for jid, gr := range res.Groups {
		groups[jid] = TriggerGroupResult{
			Count:    gr.Count,
			Messages: toMessageResponses(gr.Messages),
		}
	}

	writeJSON(w, http.StatusOK, TriggerResponse{
		Total:  res.Total,
		Groups: groups,
	})
}

// ---- PUT /api/messages/{id}/embedding -----------------------------------

// UpsertEmbedding stores or replaces the vector embedding for a message.
// Accepts JSON body: {"chat_jid":"...","embedding":[...384 floats...]}
func (h *Handler) UpsertEmbedding(w http.ResponseWriter, r *http.Request) {
	messageID := chi.URLParam(r, "id")
	if messageID == "" {
		writeError(w, http.StatusBadRequest, "message ID required", "MISSING_ID")
		return
	}

	var req UpsertEmbeddingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "BAD_REQUEST")
		return
	}

	if req.ChatJID == "" {
		writeError(w, http.StatusBadRequest, "chat_jid required", "MISSING_CHAT_JID")
		return
	}
	if len(req.Embedding) != store.EmbeddingDim {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("embedding must have %d dimensions", store.EmbeddingDim), "BAD_EMBEDDING")
		return
	}

	// Note: The store method for upserting embeddings will be added in a future PR.
	// For now, return 501 Not Implemented.
	writeError(w, http.StatusNotImplemented, "embedding storage not yet implemented", "NOT_IMPLEMENTED")
}
