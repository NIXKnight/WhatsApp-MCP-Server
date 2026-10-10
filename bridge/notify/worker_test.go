package notify

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

type fakeRepository struct {
	rows                                []store.Notification
	claims, maintenance                 int
	maintenanceEnabled                  bool
	outcomes                            []store.NotificationOutcome
	changed                             bool
	finishErr, claimErr, maintenanceErr error
	onClaim                             func()
}

func (f *fakeRepository) ClaimNotification(ctx context.Context, now time.Time, allowed []string, maxAttempts int, lease time.Duration) (*store.Notification, error) {
	f.claims++
	if f.onClaim != nil {
		f.onClaim()
	}
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if len(f.rows) == 0 {
		return nil, nil
	}
	n := f.rows[0]
	f.rows = f.rows[1:]
	return &n, nil
}
func (f *fakeRepository) FinishNotification(ctx context.Context, n store.Notification, out store.NotificationOutcome, now time.Time) (bool, error) {
	f.outcomes = append(f.outcomes, out)
	return f.changed, f.finishErr
}
func (f *fakeRepository) MaintainNotifications(ctx context.Context, now time.Time, enabled bool, allowed []string, maxAttempts int, retention time.Duration, limit int) error {
	f.maintenance++
	f.maintenanceEnabled = enabled
	return f.maintenanceErr
}

type senderFunc func(context.Context, store.Notification) DeliveryResult

func (f senderFunc) Deliver(ctx context.Context, n store.Notification) DeliveryResult {
	return f(ctx, n)
}

func deterministicWorker(f *fakeRepository, sender DeliverySender) *Worker {
	cfg := testConfig()
	cfg.BatchSize = 1
	w := NewWorker(f, cfg, sender, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := testNotification().ReceivedAt
	w.now = func() time.Time { return now }
	w.wait = func(ctx context.Context, d time.Duration) bool {
		if ctx.Err() != nil {
			return false
		}
		if d > 0 {
			now = now.Add(d)
		}
		return true
	}
	w.jitter = func() float64 { return 0.5 }
	return w
}

func TestWorkerRetryAndTerminalBounds(t *testing.T) {
	for _, tt := range []struct {
		name         string
		attempts     int
		life         time.Duration
		result       DeliveryResult
		state, class string
		delay        time.Duration
	}{
		{"acceptance", 1, time.Hour, DeliveryResult{State: "accepted", HTTPStatus: 202}, "accepted", "", 0},
		{"ignored", 1, time.Hour, DeliveryResult{State: "ignored", Class: "receiver_ignored", HTTPStatus: 200}, "ignored", "receiver_ignored", 0},
		{"retry", 1, time.Hour, DeliveryResult{State: "pending", Class: "network"}, "pending", "network", 5 * time.Second},
		{"retry after", 1, time.Hour, DeliveryResult{State: "pending", Class: "rate_limited", HTTPStatus: 429, RetryAfter: time.Minute}, "pending", "rate_limited", time.Minute},
		{"attempt bound", 10, time.Hour, DeliveryResult{State: "pending", Class: "network"}, "dead", "attempts_exhausted", 5 * time.Minute},
		{"retry past expiry", 1, time.Minute, DeliveryResult{State: "pending", Class: "rate_limited", RetryAfter: time.Hour}, "dead", "expired", time.Hour},
		{"permanent", 1, time.Hour, DeliveryResult{State: "dead", Class: "http_permanent", HTTPStatus: 401}, "dead", "http_permanent", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n := testNotification()
			n.Attempts = tt.attempts
			n.ExpiresAt = n.ReceivedAt.Add(tt.life)
			f := &fakeRepository{rows: []store.Notification{n}, changed: true}
			calls := 0
			w := deterministicWorker(f, senderFunc(func(context.Context, store.Notification) DeliveryResult { calls++; return tt.result }))
			w.runCycle(context.Background())
			if calls != 1 || len(f.outcomes) != 1 {
				t.Fatal("attempt not recorded")
			}
			got := f.outcomes[0]
			if got.State != tt.state || got.Class != tt.class || got.NextAttemptAt.Sub(n.ReceivedAt) != tt.delay {
				t.Fatal("wrong terminal/retry policy")
			}
			if tt.result.Class == "rate_limited" && w.nextSend.Before(got.NextAttemptAt) {
				t.Fatal("route cooldown missing")
			}
		})
	}
}

func TestPolicyRecheckedAfterClaim(t *testing.T) {
	for _, disable := range []bool{false, true} {
		t.Run(map[bool]string{false: "allowlist removed", true: "disabled"}[disable], func(t *testing.T) {
			f := &fakeRepository{rows: []store.Notification{testNotification()}, changed: true}
			w := deterministicWorker(f, senderFunc(func(context.Context, store.Notification) DeliveryResult {
				t.Fatal("policy-removed work sent")
				return DeliveryResult{}
			}))
			f.onClaim = func() {
				if disable {
					w.cfg.Enabled = false
				} else {
					w.cfg.ChatAllowlist = nil
				}
			}
			w.runCycle(context.Background())
			if len(f.outcomes) != 1 || f.outcomes[0].Class != "policy_removed" {
				t.Fatal("policy change not applied")
			}
		})
	}
}

func TestDisabledCleanupIsBoundedAndSecretFree(t *testing.T) {
	f := &fakeRepository{rows: []store.Notification{testNotification()}}
	w := deterministicWorker(f, senderFunc(func(context.Context, store.Notification) DeliveryResult {
		t.Fatal("disabled mode delivered")
		return DeliveryResult{}
	}))
	w.cfg.Enabled = false
	for i := 0; i < 10; i++ {
		w.runCycle(context.Background())
	}
	if f.maintenance != 1 || f.maintenanceEnabled || f.claims != 0 || len(f.rows) != 1 {
		t.Fatal("disabled mode flooded or claimed")
	}
	if w.cfg.Secret != "" || w.cfg.URL != "" {
		t.Fatal("dispatcher retained auth/destination")
	}
}

func TestWorkerCancelledInFlight(t *testing.T) {
	f := &fakeRepository{rows: []store.Notification{testNotification()}, changed: true}
	entered := make(chan struct{})
	sender := senderFunc(func(ctx context.Context, n store.Notification) DeliveryResult {
		close(entered)
		<-ctx.Done()
		return DeliveryResult{State: "pending", Class: "cancelled"}
	})
	w := deterministicWorker(f, sender)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	<-entered
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	if len(f.outcomes) != 1 || f.outcomes[0].State != "pending" || f.outcomes[0].Class != "cancelled" {
		t.Fatal("ambiguous shutdown not recoverable")
	}
}

func TestExpiredNotificationNeverSent(t *testing.T) {
	n := testNotification()
	n.ExpiresAt = n.ReceivedAt
	f := &fakeRepository{changed: true}
	w := deterministicWorker(f, senderFunc(func(context.Context, store.Notification) DeliveryResult {
		t.Fatal("expired event sent")
		return DeliveryResult{}
	}))
	w.attempt(context.Background(), n)
	if len(f.outcomes) != 1 || f.outcomes[0].Class != "expired" {
		t.Fatal("expiry not terminal")
	}
}

func TestAmbiguousAcknowledgementAndRedactedLogs(t *testing.T) {
	for _, tt := range []struct {
		name    string
		changed bool
		err     error
		class   string
	}{
		{"ack commit failed", false, errors.New("TEST_RAW_RESPONSE_URL_AUTH_MARKER"), "store_failed"},
		{"stale lease", false, nil, "lease_lost"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n := testNotification()
			f := &fakeRepository{rows: []store.Notification{n}, changed: tt.changed, finishErr: tt.err}
			w := deterministicWorker(f, senderFunc(func(context.Context, store.Notification) DeliveryResult {
				return DeliveryResult{State: "accepted", HTTPStatus: 202}
			}))
			var logs bytes.Buffer
			w.log = slog.New(slog.NewJSONHandler(&logs, nil))
			w.runCycle(context.Background())
			text := logs.String()
			if !strings.Contains(text, tt.class) || !strings.Contains(text, n.EventID) {
				t.Fatal("fixed-class diagnostic missing")
			}
			for _, marker := range []string{n.ChatJID, n.MessageID, n.LeaseToken, testOnlySecret, "TEST_RAW_RESPONSE_URL_AUTH_MARKER", "/webhooks/", "http://"} {
				if strings.Contains(text, marker) {
					t.Fatal("sensitive diagnostic leaked")
				}
			}
		})
	}
}

func TestClaimAndMaintenanceFailuresAreSanitized(t *testing.T) {
	for _, claim := range []bool{false, true} {
		t.Run(map[bool]string{false: "maintenance", true: "claim"}[claim], func(t *testing.T) {
			f := &fakeRepository{}
			if claim {
				f.claimErr = errors.New("TEST_PRIVATE_MARKER")
			} else {
				f.maintenanceErr = errors.New("TEST_PRIVATE_MARKER")
			}
			w := deterministicWorker(f, nil)
			if claim {
				w.sender = senderFunc(func(context.Context, store.Notification) DeliveryResult { return DeliveryResult{} })
			}
			var logs bytes.Buffer
			w.log = slog.New(slog.NewJSONHandler(&logs, nil))
			w.runCycle(context.Background())
			if strings.Contains(logs.String(), "TEST_PRIVATE_MARKER") || !strings.Contains(logs.String(), "store_failed") {
				t.Fatal("raw store diagnostic leaked")
			}
		})
	}
}

func TestBatchIsBoundedAndPaced(t *testing.T) {
	f := &fakeRepository{rows: []store.Notification{testNotification(), testNotification(), testNotification()}, changed: true}
	w := deterministicWorker(f, senderFunc(func(context.Context, store.Notification) DeliveryResult { return DeliveryResult{State: "accepted"} }))
	w.cfg.BatchSize = 2
	w.runCycle(context.Background())
	if f.claims != 2 || len(f.rows) != 1 || len(f.outcomes) != 2 {
		t.Fatal("batch bound ignored")
	}
	if !w.now().Equal(testNotification().ReceivedAt.Add(w.cfg.MinInterval)) {
		t.Fatal("global pacing absent")
	}
}

func TestBackoffBounds(t *testing.T) {
	if retryDelay(1, 0.5) != 5*time.Second || retryDelay(2, 0.5) != 10*time.Second {
		t.Fatal("backoff progression wrong")
	}
	for _, attempt := range []int{-1, 1, 7, 100} {
		for _, jitter := range []float64{-1, 0, 0.5, 1, 2} {
			d := retryDelay(attempt, jitter)
			if d < 4*time.Second || d > 5*time.Minute {
				t.Fatal("backoff unbounded")
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitContext(ctx, 0) {
		t.Fatal("cancelled wait continued")
	}
}
