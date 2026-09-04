// Package mediaretry implements L3 background media re-download (enrichment):
// it polls the database for previously-requested media that failed to download
// and retries with bounded attempts. It is co-resident with the bridge process
// because downloading requires the live whatsmeow session (client.Download),
// which only the bridge holds. The worker integrates purely through the database
// (poll-and-update on the messages_media download_* columns) and issues no DDL;
// the schema is owned by bridge/migrations.
//
// Scope is REQUESTED-ONLY: only media already requested on-demand and failed
// (download_attempts > 0) is retried. Captured-but-never-requested media is left
// untouched. The poll interval is itself the cross-cycle backoff — a transient
// 403/410 is usually a stale signed URL that a later download refreshes, so each
// eligible row is retried at most once per cycle (no tight in-loop retry).
package mediaretry

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/client"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/media"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// Worker re-downloads media that was requested on-demand and failed.
type Worker struct {
	client      *client.Client
	store       *store.Store
	log         *slog.Logger
	interval    time.Duration
	maxAttempts int
	batchSize   int
}

// New constructs a media-retry worker. interval is both the poll cadence and the
// per-row cross-cycle backoff; maxAttempts bounds retries before a row is
// flagged permanently failed; batchSize bounds rows processed per cycle.
func New(c *client.Client, s *store.Store, interval time.Duration, maxAttempts, batchSize int, log *slog.Logger) *Worker {
	if interval <= 0 {
		interval = 3 * time.Minute
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if batchSize <= 0 {
		batchSize = 10
	}
	return &Worker{
		client:      c,
		store:       s,
		log:         log,
		interval:    interval,
		maxAttempts: maxAttempts,
		batchSize:   batchSize,
	}
}

// Run drives the retry loop until ctx is cancelled. It blocks; callers start it
// on its own goroutine and cancel ctx to shut it down cleanly. A cycle is
// skipped while the WhatsApp session is not connected so the worker never burns
// attempts on rows it cannot possibly download.
func (w *Worker) Run(ctx context.Context) {
	w.log.Info("media-retry worker started",
		"interval", w.interval.String(),
		"max_attempts", w.maxAttempts,
		"batch_size", w.batchSize,
	)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Info("media-retry worker stopped")
			return
		case <-ticker.C:
			w.runCycle(ctx)
		}
	}
}

// runCycle processes one bounded batch of retry-eligible rows.
func (w *Worker) runCycle(ctx context.Context) {
	if w.client.State() != client.StateConnected {
		w.log.Debug("media-retry: skipping cycle, WhatsApp session not connected")
		return
	}

	rows, err := w.store.ListRetryableMedia(w.maxAttempts, w.batchSize)
	if err != nil {
		w.log.Warn("media-retry: failed to list retryable media", "err", err)
		return
	}
	if len(rows) == 0 {
		return
	}

	w.log.Debug("media-retry: cycle start", "candidates", len(rows))
	for i := range rows {
		if ctx.Err() != nil {
			return // shutdown requested between rows
		}
		w.retryOne(ctx, &rows[i])
	}
}

// retryOne attempts a single re-download and records the outcome. On success it
// stamps local_path; on failure it bumps the attempt counter and lets the store
// decide permanence atomically. Logs carry only the message ID, attempt count,
// and a short error class — never a URL, direct path, or media key.
func (w *Worker) retryOne(ctx context.Context, m *store.MediaRow) {
	msg := mediaRowToMessage(m)

	res, err := media.Download(ctx, w.client.WA, msg, w.client.DataDir())
	if err != nil {
		// A cancellation mid-download is shutdown, not a media failure: do not
		// burn an attempt for it.
		if errors.Is(err, context.Canceled) {
			return
		}

		structural := media.ClassifyDownloadError(err)
		permanent, derr := w.store.MarkMediaDownloadFailed(
			m.MessageID, m.ChatJID, err.Error(), structural, w.maxAttempts,
		)
		if derr != nil {
			w.log.Warn("media-retry: failed to record download failure",
				"message_id", m.MessageID, "err", derr)
			return
		}

		w.log.Warn("media-retry: download failed",
			"message_id", m.MessageID,
			"attempt", m.DownloadAttempts+1,
			"max_attempts", w.maxAttempts,
			"class", media.DownloadErrorClass(err),
			"permanent", permanent,
		)
		return
	}

	if err := w.store.MarkMediaDownloaded(m.MessageID, m.ChatJID, res.Path); err != nil {
		w.log.Warn("media-retry: download succeeded but recording it failed",
			"message_id", m.MessageID, "err", err)
		return
	}

	w.log.Info("media-retry: download recovered",
		"message_id", m.MessageID,
		"attempt", m.DownloadAttempts+1,
	)
}

// mediaRowToMessage adapts a messages_media row to the MessageRow shape that
// media.Download consumes. Only the fields Download reads are populated; the
// direct path is recomputed by Download from the URL.
func mediaRowToMessage(m *store.MediaRow) *store.MessageRow {
	return &store.MessageRow{
		ID:            m.MessageID,
		ChatJID:       m.ChatJID,
		MediaType:     m.MediaType,
		Filename:      m.Filename,
		URL:           m.URL,
		MediaKey:      m.MediaKey,
		FileSHA256:    m.FileSHA256,
		FileEncSHA256: m.FileEncSHA256,
		FileLength:    m.FileLength,
	}
}
