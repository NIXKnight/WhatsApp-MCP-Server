package api

import (
	"encoding/json"
	"net/http"
)

// ---- POST /api/media/analyze --------------------------------------------

// HandleMediaAnalyze is a thin proxy that forwards a single message reference to
// the L3 transcriber's /analyze endpoint and returns its JSON verbatim. It does
// no media (ffmpeg) work itself: extraction and transcription happen in the
// transcriber. The route is exempted from the global 60s timeout and the server
// WriteTimeout is widened so that a slow analysis is not truncated.
func (h *Handler) HandleMediaAnalyze(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ChatJID   string `json:"chat_jid"`
		MessageID string `json:"message_id"`
	}
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

	res, err := h.analyzer.Analyze(r.Context(), req.ChatJID, req.MessageID)
	if err != nil {
		h.log.Error("analyze media", "message_id", req.MessageID, "err", err)
		writeError(w, http.StatusBadGateway, "analysis failed", "ANALYZE_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, res)
}
