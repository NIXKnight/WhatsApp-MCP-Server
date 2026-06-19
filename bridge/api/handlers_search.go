package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// ---- POST /api/search ---------------------------------------------------

// HybridSearch performs RRF over FTS and/or vector embeddings.
// Accepts JSON body: {"query":"...","embedding":[...],"chat_jid":"...","limit":20}
func (h *Handler) HybridSearch(w http.ResponseWriter, r *http.Request) {
	var req HybridSearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "INVALID_JSON")
		return
	}

	if req.Query == "" && len(req.Embedding) == 0 {
		writeError(w, http.StatusBadRequest, "at least one of query or embedding is required", "MISSING_FIELD")
		return
	}
	if len(req.Embedding) > 0 && len(req.Embedding) != store.EmbeddingDim {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("embedding must have exactly %d dimensions", store.EmbeddingDim), "INVALID_EMBEDDING")
		return
	}
	if req.ChatJID != "" && !jidRe.MatchString(req.ChatJID) {
		writeError(w, http.StatusBadRequest, "invalid chat_jid format", "INVALID_JID")
		return
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	results, err := h.store.SearchHybrid(req.Query, req.Embedding, req.ChatJID, limit)
	if err != nil {
		h.log.Error("hybrid search", "err", err)
		writeError(w, http.StatusInternalServerError, "search failed", "DB_ERROR")
		return
	}

	out := make([]SearchResultResponse, len(results))
	for i, sr := range results {
		out[i] = SearchResultResponse{
			ID:         sr.ID,
			ChatJID:    sr.ChatJID,
			Content:    sr.Content,
			Timestamp:  sr.Timestamp,
			SenderName: sr.SenderName,
			Score:      sr.Score,
			Snippet:    sr.Snippet,
			MatchType:  sr.MatchType,
		}
	}

	writeJSON(w, http.StatusOK, HybridSearchResponse{
		Results: out,
		Total:   len(out),
	})
}

// ---- POST /api/chats/search ---------------------------------------------

// SearchChatsByTopic returns chats ranked by cosine similarity to the provided
// topic embedding. Accepts JSON body: {"embedding":[...],"limit":10}
func (h *Handler) SearchChatsByTopic(w http.ResponseWriter, r *http.Request) {
	var req ChatTopicSearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "INVALID_JSON")
		return
	}

	if len(req.Embedding) == 0 {
		writeError(w, http.StatusBadRequest, "embedding is required", "MISSING_FIELD")
		return
	}
	if len(req.Embedding) != store.EmbeddingDim {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("embedding must have exactly %d dimensions", store.EmbeddingDim), "INVALID_EMBEDDING")
		return
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}

	results, err := h.store.SearchChatsByTopic(req.Embedding, limit)
	if err != nil {
		h.log.Error("chat topic search", "err", err)
		writeError(w, http.StatusInternalServerError, "search failed", "DB_ERROR")
		return
	}

	out := make([]ChatWithScoreResponse, len(results))
	for i, cs := range results {
		out[i] = ChatWithScoreResponse{
			ChatResponse: toChatResponse(cs.ChatRow),
			Relevance:    cs.Relevance,
		}
	}

	writeJSON(w, http.StatusOK, ChatTopicSearchResponse{
		Results: out,
		Total:   len(out),
	})
}
