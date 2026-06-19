package api

import (
	"database/sql"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
)

// ---- GET /api/contacts --------------------------------------------------

// ListContacts returns all contacts.
func (h *Handler) ListContacts(w http.ResponseWriter, r *http.Request) {
	contacts, err := h.store.ListContacts()
	if err != nil {
		h.log.Error("list contacts", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to query contacts", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, ContactsResponse{
		Contacts: toContactResponses(contacts),
		Total:    len(contacts),
	})
}

// ---- GET /api/contacts/similar ------------------------------------------

// SimilarContacts returns pairs of contacts with similar name embeddings.
func (h *Handler) SimilarContacts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	thresholdStr := q.Get("threshold")
	threshold := 0.85
	if thresholdStr != "" {
		if v, err := strconv.ParseFloat(thresholdStr, 64); err == nil && v >= 0 && v <= 1 {
			threshold = v
		}
	}

	limit := intParam(q.Get("limit"), 20, 1, 200)

	pairs, err := h.store.FindSimilarContacts(threshold, limit)
	if err != nil {
		h.log.Error("similar contacts", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to find similar contacts", "DB_ERROR")
		return
	}

	out := make([]SimilarContactPairResponse, len(pairs))
	for i, p := range pairs {
		out[i] = SimilarContactPairResponse{
			JIDA:       p.JIDA,
			NameA:      p.NameA,
			JIDB:       p.JIDB,
			NameB:      p.NameB,
			Similarity: p.Similarity,
		}
	}

	writeJSON(w, http.StatusOK, SimilarContactsResponse{
		Pairs: out,
		Total: len(out),
	})
}

// ---- GET /api/contacts/{jid} --------------------------------------------

// GetContact returns a single contact by JID.
func (h *Handler) GetContact(w http.ResponseWriter, r *http.Request) {
	jid := chi.URLParam(r, "jid")
	if !jidRe.MatchString(jid) {
		writeError(w, http.StatusBadRequest, "invalid JID format", "INVALID_JID")
		return
	}

	contact, err := h.store.GetContact(jid)
	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "contact not found", "NOT_FOUND")
			return
		}
		h.log.Error("get contact", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get contact", "DB_ERROR")
		return
	}

	writeJSON(w, http.StatusOK, ContactResponse{
		JID:    contact.JID,
		Name:   contact.Name,
		Notify: contact.Notify,
		Phone:  contact.Phone,
	})
}
