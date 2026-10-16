package api

import (
	"net/http"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/client"
)

// liveWhatsAppState is the narrow read-only transport boundary needed by the
// health endpoint.
type liveWhatsAppState interface {
	IsConnected() bool
	IsLoggedIn() bool
}

func isLiveConnected(wa liveWhatsAppState) bool {
	return wa != nil && wa.IsConnected() && wa.IsLoggedIn()
}

func statusResponse(state client.ConnectionState, wa liveWhatsAppState, uptime time.Duration, msgCount, chatCount int64, startedAt time.Time) StatusResponse {
	return StatusResponse{
		State:        state.String(),
		IsConnected:  isLiveConnected(wa),
		Uptime:       uptime.String(),
		MessageCount: msgCount,
		ChatCount:    chatCount,
		StartedAt:    startedAt,
	}
}

// ---- GET /api/status ----------------------------------------------------

// Status returns the connection state, uptime, and aggregate message counts.
func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	msgCount, _ := h.store.CountMessages()
	chatCount, _ := h.store.CountChats()
	state := client.StateDisconnected
	if h.client != nil {
		state = h.client.State()
	}
	uptime := time.Since(h.startedAt).Truncate(time.Second)

	writeJSON(w, http.StatusOK, statusResponse(
		state, h.client, uptime, msgCount, chatCount, h.startedAt,
	))
}
