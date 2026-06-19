package api

import (
	"encoding/json"
	"net/http"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// ---- POST /api/telemetry/tool -------------------------------------------

// RecordToolCall records one MCP tool invocation's latency and outcome. The
// endpoint is fire-and-forget from the caller's perspective: it always returns
// 200 once the body parses, even if the underlying insert fails (the failure is
// logged, not surfaced). Only a malformed body or a missing tool_name yields a
// 4xx.
func (h *Handler) RecordToolCall(w http.ResponseWriter, r *http.Request) {
	var req ToolCallRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "INVALID_JSON")
		return
	}
	if req.ToolName == "" {
		writeError(w, http.StatusBadRequest, "tool_name is required", "MISSING_FIELD")
		return
	}

	if err := h.store.RecordToolCall(&store.ToolCall{
		ToolName:   req.ToolName,
		DurationMs: req.DurationMs,
		Success:    req.Success,
		ErrorMsg:   req.ErrorMsg,
	}); err != nil {
		// Telemetry is best-effort; never fail the caller on a write error.
		h.log.Warn("record tool call", "tool", req.ToolName, "err", err)
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
