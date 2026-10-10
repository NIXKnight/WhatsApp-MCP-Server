package notify

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/config"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// Repository is the narrow durable-outbox boundary used by the dispatcher.
type Repository interface {
	ClaimNotification(context.Context, time.Time, []string, int, time.Duration) (*store.Notification, error)
	FinishNotification(context.Context, store.Notification, store.NotificationOutcome, time.Time) (bool, error)
	MaintainNotifications(context.Context, time.Time, bool, []string, int, time.Duration, int) error
}

// DeliverySender makes a single attempt and never returns unsanitized errors.
type DeliverySender interface {
	Deliver(context.Context, store.Notification) DeliveryResult
}

// Worker owns a single bounded dispatcher. Disabled mode runs only minute-wise
// local expiry/pruning, without loading a secret or constructing an HTTP client.
type Worker struct {
	store           Repository
	cfg             config.Notifications
	sender          DeliverySender
	log             *slog.Logger
	now             func() time.Time
	wait            func(context.Context, time.Duration) bool
	jitter          func() float64
	nextSend        time.Time
	nextMaintenance time.Time
}

// NewWorker constructs a worker; Run owns its state on one goroutine. The
// configuration is a startup snapshot; changes require a graceful restart.
func NewWorker(st Repository, cfg config.Notifications, sender DeliverySender, log *slog.Logger) *Worker {
	cfg.ChatAllowlist = append([]string(nil), cfg.ChatAllowlist...)
	// Never retain the endpoint or secret in the dispatcher or store layer.
	cfg.Secret, cfg.URL = "", ""
	return &Worker{store: st, cfg: cfg, sender: sender, log: log, now: time.Now, wait: waitContext, jitter: rand.Float64}
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Run performs bounded cycles until cancellation. No HTTP or store transaction
// is held across a backoff wait. Callers must cancel and wait before store close.
func (w *Worker) Run(ctx context.Context) {
	interval := time.Minute
	if w.cfg.Enabled {
		interval = w.cfg.PollInterval
	}
	if interval <= 0 {
		interval = time.Second
	}
	for ctx.Err() == nil {
		w.runCycle(ctx)
		if !w.wait(ctx, interval) {
			return
		}
	}
}

func (w *Worker) allowed(chat string) bool {
	if !w.cfg.Enabled {
		return false
	}
	for _, jid := range w.cfg.ChatAllowlist {
		if jid == chat {
			return true
		}
	}
	return false
}

func (w *Worker) runCycle(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	now := w.now()
	if !now.Before(w.nextMaintenance) {
		maintenanceCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := w.store.MaintainNotifications(maintenanceCtx, now, w.cfg.Enabled, w.cfg.ChatAllowlist, w.cfg.MaxAttempts, w.cfg.Retention, 250)
		cancel()
		w.nextMaintenance = now.Add(time.Minute)
		if err != nil {
			w.log.Warn("notification maintenance failed", "class", "store_failed")
			return
		}
	}
	if !w.cfg.Enabled || w.sender == nil || len(w.cfg.ChatAllowlist) == 0 {
		return
	}
	for i := 0; i < w.cfg.BatchSize && ctx.Err() == nil; i++ {
		if !w.wait(ctx, w.nextSend.Sub(w.now())) {
			return
		}
		if !w.cfg.Enabled {
			return
		}
		claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		// Lease one row at a time, never a batch whose leases might expire
		// while earlier rows are in flight. Include DB bookkeeping headroom.
		lease := w.cfg.Timeout + 10*time.Second
		if lease < 30*time.Second {
			lease = 30 * time.Second
		}
		n, err := w.store.ClaimNotification(claimCtx, w.now(), w.cfg.ChatAllowlist, w.cfg.MaxAttempts, lease)
		cancel()
		if err != nil {
			w.log.Warn("notification claim failed", "class", "store_failed")
			return
		}
		if n == nil {
			return
		}
		w.attempt(ctx, *n)
	}
}

func (w *Worker) attempt(ctx context.Context, n store.Notification) {
	// Recheck the current worker policy immediately before every attempt,
	// including resumed/previously claimed work. In-flight requests cannot be
	// retracted by shutdown or a configuration change at the receiver.
	now := w.now()
	outcome := store.NotificationOutcome{State: "dead", Class: "expired", NextAttemptAt: now}
	if !w.allowed(n.ChatJID) {
		// Use a fixed class supported by the persistence boundary; maintenance
		// also retires policy-removed work before claiming it.
		outcome.Class = "policy_removed"
	} else if now.Before(n.ExpiresAt) {
		requestCtx, cancel := context.WithTimeout(ctx, w.cfg.Timeout)
		requestCtx, expiryCancel := context.WithDeadline(requestCtx, n.ExpiresAt)
		r := w.sender.Deliver(requestCtx, n)
		expiryCancel()
		cancel()
		now = w.now()
		outcome.State, outcome.Class, outcome.HTTPStatus = r.State, r.Class, r.HTTPStatus
		outcome.NextAttemptAt = now
		w.nextSend = now.Add(w.cfg.MinInterval)
		if r.State == "pending" {
			delay := retryDelay(n.Attempts, w.jitter())
			if r.RetryAfter > delay {
				delay = r.RetryAfter
			}
			outcome.NextAttemptAt = now.Add(delay)
			if r.Class == "rate_limited" {
				w.nextSend = outcome.NextAttemptAt
			}
			if n.Attempts >= w.cfg.MaxAttempts {
				outcome.State, outcome.Class = "dead", "attempts_exhausted"
			} else if !outcome.NextAttemptAt.Before(n.ExpiresAt) {
				outcome.State, outcome.Class = "dead", "expired"
			}
		}
	}
	// Shutdown may leave the HTTP outcome ambiguous. Try a short independent
	// bookkeeping operation; if it fails, lease recovery retries the same ID.
	finishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	changed, err := w.store.FinishNotification(finishCtx, n, outcome, now)
	cancel()
	class := outcome.Class
	if err != nil {
		class = "store_failed"
	} else if !changed {
		class = "lease_lost"
	}
	// Do not trust even a malformed database identifier as a log field.
	if opaqueID.MatchString(n.EventID) {
		w.log.Info("notification attempt recorded", "event_id", n.EventID, "class", class, "attempt", n.Attempts, "http_status", outcome.HTTPStatus)
	} else {
		w.log.Warn("notification metadata invalid", "class", "metadata_invalid")
	}
}

func retryDelay(attempt int, jitter float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 7 {
		attempt = 7
	}
	delay := 5 * time.Second * time.Duration(1<<uint(attempt-1))
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	delay = time.Duration(float64(delay) * (0.8 + 0.4*jitter))
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}
