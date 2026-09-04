package api

import (
	"database/sql"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.mau.fi/whatsmeow/types"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/client"
)

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

	if h.client.State() == client.StateConnected {
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
