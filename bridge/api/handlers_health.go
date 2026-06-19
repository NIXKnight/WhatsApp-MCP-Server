package api

import (
	"net/http"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/client"
)

// ---- GET /api/status ----------------------------------------------------

// Status returns the connection state, uptime, and aggregate message counts.
func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	msgCount, _ := h.store.CountMessages()
	chatCount, _ := h.store.CountChats()
	state := h.client.State()
	uptime := time.Since(h.startedAt).Truncate(time.Second)

	writeJSON(w, http.StatusOK, StatusResponse{
		State:        state.String(),
		IsConnected:  state == client.StateConnected,
		Uptime:       uptime.String(),
		MessageCount: msgCount,
		ChatCount:    chatCount,
		StartedAt:    h.startedAt,
	})
}
