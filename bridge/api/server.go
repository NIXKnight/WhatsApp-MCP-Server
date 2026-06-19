package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Server is the HTTP API server.
type Server struct {
	srv     *http.Server
	log     *slog.Logger
	handler *Handler
}

// NewServer constructs and configures the HTTP server with all routes and
// middleware. It does not start listening.
func NewServer(addr string, h *Handler, log *slog.Logger) *Server {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(newSlogMiddleware(log))
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	// Token-bucket limiter applied only to outbound send routes. WhatsApp
	// penalises bursts of automated sends, so the bridge self-throttles and
	// adds small human-timing jitter. Conservative anti-ban defaults: ~2 sends
	// per second sustained, burst of 5, up to 300 ms jitter.
	rl := NewRateLimiter(2.0, 5, 300)

	r.Route("/api", func(r chi.Router) {
		r.Get("/status", h.Status)
		r.Get("/messages", h.ListMessages)
		r.Get("/messages/{id}/context", h.MessageContext)
		r.Get("/messages/{id}/similar", h.SimilarMessages)
		r.Put("/messages/{id}/embedding", h.UpsertEmbedding)
		r.Post("/search", h.HybridSearch)
		r.Get("/chats", h.ListChats)
		r.Post("/chats/search", h.SearchChatsByTopic)
		r.Get("/chats/{jid}", h.GetChat)
		r.Get("/contacts", h.ListContacts)
		r.Get("/contacts/similar", h.SimilarContacts)
		r.Get("/contacts/{jid}", h.GetContact)
		r.Get("/groups", h.ListGroups)
		r.Get("/groups/{jid}", h.GetGroup)
		r.Get("/unread", h.ListUnread)
		r.Get("/check", h.CheckNewMessages)
		r.Post("/check/triggers", h.CheckTriggers)
		r.Post("/download", h.DownloadMedia)
		r.Post("/telemetry/tool", h.RecordToolCall)

		// Send routes (rate-limited, anti-ban).
		r.Group(func(r chi.Router) {
			r.Use(rl.Middleware)
			r.Post("/send", h.SendMessage)
			r.Post("/send/media", h.SendMedia)
			r.Post("/send/reaction", h.SendReaction)
			r.Post("/send/edit", h.EditMessage)
			r.Post("/send/revoke", h.RevokeMessage)
		})
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return &Server{
		srv:     srv,
		log:     log,
		handler: h,
	}
}

// Start begins listening for HTTP connections. It returns when the server
// shuts down.
func (s *Server) Start() error {
	s.log.Info("HTTP API server starting", "addr", s.srv.Addr)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully drains in-flight requests within the timeout.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// ---- JSON helpers -------------------------------------------------------

// writeJSON writes v as JSON with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		// Can't write headers at this point; just log.
		slog.Error("failed to encode JSON response", "err", err)
	}
}

// writeError writes a structured error JSON response.
func writeError(w http.ResponseWriter, status int, msg, code string) {
	writeJSON(w, status, ErrorResponse{Error: msg, Code: code})
}

// ---- Logging middleware -------------------------------------------------

func newSlogMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			log.Info("http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"duration", time.Since(start),
				"request_id", middleware.GetReqID(r.Context()),
			)
		})
	}
}
