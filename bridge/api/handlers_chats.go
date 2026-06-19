package api

import (
	"database/sql"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// ---- GET /api/chats -----------------------------------------------------

// ListChats returns all chats with optional name search and pagination.
func (h *Handler) ListChats(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := intParam(q.Get("limit"), 50, 1, 500)
	page := intParam(q.Get("page"), 0, 0, 10000)
	offset := page * limit

	chats, err := h.store.ListChats(q.Get("query"), limit, offset, false)
	if err != nil {
		h.log.Error("list chats", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query chats", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, ChatsResponse{
		Chats:  toChatResponses(chats),
		Total:  len(chats),
		Limit:  limit,
		Offset: offset,
	})
}

// ---- GET /api/chats/{jid} -----------------------------------------------

// GetChat returns a single chat by JID.
func (h *Handler) GetChat(w http.ResponseWriter, r *http.Request) {
	jid := chi.URLParam(r, "jid")
	if !jidRe.MatchString(jid) {
		writeError(w, http.StatusBadRequest, "invalid JID format", "INVALID_JID")
		return
	}

	chat, err := h.store.GetChat(jid)
	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "chat not found", "NOT_FOUND")
			return
		}
		h.log.Error("get chat", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get chat", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, toChatResponse(*chat))
}

// ---- GET /api/unread ----------------------------------------------------

// ListUnread returns chats with unread_count > 0, each with their recent messages.
// When query param flat=true is present, returns a flat list of all unread
// messages across all chats (used by the Python get_unread_messages tool).
func (h *Handler) ListUnread(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	flat := strings.ToLower(q.Get("flat")) == "true"

	chats, err := h.store.ListUnreadChats()
	if err != nil {
		h.log.Error("list unread chats", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query unread chats", "DB_ERROR")
		return
	}

	msgLimit := intParam(q.Get("msg_limit"), 5, 1, 50)
	// When in flat mode, fetch more messages per chat for the full inbox view.
	if flat {
		msgLimit = intParam(q.Get("msg_limit"), 20, 1, 100)
	}

	if flat {
		// Return a flat sorted list of all unread messages.
		var flatMsgs []UnreadFlatMessage
		for _, chat := range chats {
			msgs, err := h.store.ListMessages(store.QueryMessagesParams{
				ChatJID: chat.JID,
				Limit:   msgLimit,
				Offset:  0,
			})
			if err != nil {
				h.log.Warn("unread flat: failed to fetch messages", "jid", chat.JID, "err", err)
				continue
			}
			for _, m := range msgs {
				flatMsgs = append(flatMsgs, UnreadFlatMessage{
					ChatJID:     chat.JID,
					ChatName:    chat.Name,
					IsGroup:     chat.IsGroup,
					MessageID:   m.ID,
					Participant: m.Sender,
					SenderName:  m.SenderName,
					Text:        m.Content,
					Timestamp:   m.Timestamp.UnixMilli(),
				})
			}
		}
		writeJSON(w, http.StatusOK, UnreadFlatResponse{
			TotalUnread: len(flatMsgs),
			Messages:    flatMsgs,
		})
		return
	}

	entries := make([]UnreadChatEntry, 0, len(chats))
	for _, chat := range chats {
		msgs, err := h.store.ListMessages(store.QueryMessagesParams{
			ChatJID: chat.JID,
			Limit:   msgLimit,
			Offset:  0,
		})
		if err != nil {
			h.log.Warn("unread: failed to fetch messages", "jid", chat.JID, "err", err)
		}
		entries = append(entries, UnreadChatEntry{
			Chat:     toChatResponse(chat),
			Messages: toMessageResponses(msgs),
		})
	}

	writeJSON(w, http.StatusOK, UnreadResponse{
		Chats: entries,
		Total: len(entries),
	})
}
