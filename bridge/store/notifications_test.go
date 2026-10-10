package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/migrations"
)

func mockStore(t *testing.T) (*Store, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual), sqlmock.ValueConverterOption(mockArrayConverter{}))
	if err != nil {
		t.Fatal("create mock")
	}
	s := &Store{db: db, writeCh: make(chan WriteOp, 1000), done: make(chan struct{}), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.writerWG.Add(1)
	go s.writerLoop()
	t.Cleanup(func() {
		_ = s.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	return s, mock
}

// pgx accepts []string arrays through CheckNamedValue. sqlmock's default
// converter does not; normalize only these synthetic test arrays for matching.
type mockArrayConverter struct{}

func (mockArrayConverter) ConvertValue(v any) (driver.Value, error) {
	if a, ok := v.([]string); ok {
		return strings.Join(a, "\x00"), nil
	}
	return driver.DefaultParameterConverter.ConvertValue(v)
}

type opaqueArgument struct{}

func (opaqueArgument) Match(v driver.Value) bool {
	s, ok := v.(string)
	return ok && regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(s)
}

func testMessage() *MessageRow {
	return &MessageRow{ID: "TEST_M1", ChatJID: "100@g.us", Sender: "TEST_SENDER", SenderName: "TEST_NAME", Content: "TEST_CONTENT_NOT_FOR_EVENT", Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}
func messageArgs(m *MessageRow) []driver.Value {
	return []driver.Value{m.ID, m.ChatJID, m.Sender, m.SenderName, m.Content, m.Timestamp, m.IsFromMe, m.MediaType, m.Filename, m.URL, m.MediaKey, m.FileSHA256, m.FileEncSHA256, m.FileLength, m.PushName, m.QuotedMessageID, m.QuotedParticipant}
}
func expectInserted(mock sqlmock.Sqlmock, m *MessageRow) {
	mock.ExpectQuery(insertMessageSQL).WithArgs(messageArgs(m)...).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(m.ID))
}
func expectEvent(mock sqlmock.Sqlmock, m *MessageRow, req *NotificationRequest) *sqlmock.ExpectedExec {
	return mock.ExpectExec(insertNotificationSQL).WithArgs(opaqueArgument{}, m.ChatJID, m.ID, m.Timestamp, req.ReceivedAt, req.ReceivedAt.Add(req.MaxAge))
}

func TestSaveMessageOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode string
		want MessageWriteResult
		fail bool
	}{
		{"insert event", "event", MessageInserted, false}, {"legacy insert", "legacy", MessageInserted, false},
		{"duplicate update", "duplicate", MessageUpdated, false}, {"self insert", "self", MessageInserted, false},
		{"unsupported", "skip", MessageSkipped, false}, {"message failure", "message_error", "", true},
		{"outbox failure", "event_error", "", true}, {"update failure", "update_error", "", true},
		{"update absent", "update_zero", "", true},
		{"update unexpected count", "update_multiple", "", true},
		{"update rows affected failure", "update_rows_error", "", true},
		{"commit unknown", "commit_error", "", true}, {"begin failure", "begin_error", "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, mock := mockStore(t)
			m := testMessage()
			req := &NotificationRequest{ReceivedAt: m.Timestamp, MaxAge: time.Hour}
			if tt.mode == "self" {
				m.IsFromMe = true
			}
			if tt.mode == "legacy" {
				req = nil
			}
			if tt.mode == "skip" {
				m.Content = ""
			}
			failure := errors.New("TEST_RAW_ERROR_MUST_NOT_ESCAPE")
			if tt.mode != "skip" {
				begin := mock.ExpectBegin()
				if tt.mode == "begin_error" {
					begin.WillReturnError(failure)
				} else {
					switch tt.mode {
					case "message_error":
						mock.ExpectQuery(insertMessageSQL).WithArgs(messageArgs(m)...).WillReturnError(failure)
					case "duplicate", "update_error", "update_zero", "update_multiple", "update_rows_error":
						mock.ExpectQuery(insertMessageSQL).WithArgs(messageArgs(m)...).WillReturnRows(sqlmock.NewRows([]string{"id"}))
						e := mock.ExpectExec(updateMessageSQL).WithArgs(messageArgs(m)...)
						switch tt.mode {
						case "update_error":
							e.WillReturnError(failure)
						case "update_zero":
							e.WillReturnResult(sqlmock.NewResult(0, 0))
						case "update_multiple":
							e.WillReturnResult(sqlmock.NewResult(0, 2))
						case "update_rows_error":
							e.WillReturnResult(sqlmock.NewErrorResult(failure))
						default:
							e.WillReturnResult(sqlmock.NewResult(0, 1))
						}
					default:
						expectInserted(mock, m)
						if req != nil && !m.IsFromMe {
							e := expectEvent(mock, m, req)
							if tt.mode == "event_error" {
								e.WillReturnError(failure)
							} else {
								e.WillReturnResult(sqlmock.NewResult(0, 1))
							}
						}
					}
					if tt.mode == "commit_error" {
						mock.ExpectCommit().WillReturnError(failure)
					} else if tt.fail {
						mock.ExpectRollback()
					} else {
						mock.ExpectCommit()
					}
				}
			}
			got, err := s.SaveMessage(context.Background(), m, req)
			if got != tt.want || (err != nil) != tt.fail {
				t.Fatalf("unexpected write outcome: %s", got)
			}
			if err != nil && err.Error() != "notification_store_failed" {
				t.Fatal("raw error escaped")
			}
		})
	}
}

func TestLegacyUpsertNeverNotifies(t *testing.T) {
	s, mock := mockStore(t)
	m := testMessage()
	mock.ExpectBegin()
	expectInserted(mock, m)
	mock.ExpectCommit()
	if s.UpsertMessage(m) != nil {
		t.Fatal("legacy upsert failed")
	}
}

func TestConcurrentDuplicatesUseInsertionIdentity(t *testing.T) {
	s, mock := mockStore(t)
	m := testMessage()
	req := &NotificationRequest{ReceivedAt: m.Timestamp, MaxAge: time.Hour}
	mock.ExpectBegin()
	expectInserted(mock, m)
	expectEvent(mock, m, req).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(insertMessageSQL).WithArgs(messageArgs(m)...).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectExec(updateMessageSQL).WithArgs(messageArgs(m)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	results := make(chan MessageWriteResult, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.SaveMessage(context.Background(), m, req)
			if err != nil {
				t.Error("duplicate save failed")
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	counts := map[MessageWriteResult]int{}
	for r := range results {
		counts[r]++
	}
	if counts[MessageInserted] != 1 || counts[MessageUpdated] != 1 {
		t.Fatal("duplicate outcome wrong")
	}
}

func notificationRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"event_id", "schema_version", "event_type", "chat_jid", "message_id", "event_timestamp", "received_at", "expires_at", "attempts", "lease_token"})
}
func notificationFixture() Notification {
	now := testMessage().Timestamp
	return Notification{EventID: strings.Repeat("a", 32), SchemaVersion: 1, EventType: "whatsapp.message.received", ChatJID: "100@g.us", MessageID: "TEST_M1", EventTimestamp: now, ReceivedAt: now, ExpiresAt: now.Add(time.Hour), Attempts: 1, LeaseToken: strings.Repeat("b", 32)}
}

func TestClaimNotification(t *testing.T) {
	for _, tt := range []struct {
		name                         string
		empty, queryFail, commitFail bool
	}{
		{"work", false, false, false}, {"empty", true, false, false}, {"query failure", false, true, false}, {"unknown commit", false, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, mock := mockStore(t)
			n := notificationFixture()
			now := n.ReceivedAt
			mock.ExpectBegin()
			q := mock.ExpectQuery(claimNotificationSQL).WithArgs(10, []string{n.ChatJID}, opaqueArgument{}, int64(30_000))
			if tt.queryFail {
				q.WillReturnError(errors.New("TEST_RAW_QUERY_ERROR"))
				mock.ExpectRollback()
			} else {
				rows := notificationRows()
				if !tt.empty {
					rows.AddRow(n.EventID, n.SchemaVersion, n.EventType, n.ChatJID, n.MessageID, n.EventTimestamp, n.ReceivedAt, n.ExpiresAt, n.Attempts, n.LeaseToken)
				}
				q.WillReturnRows(rows)
				if tt.commitFail {
					mock.ExpectCommit().WillReturnError(errors.New("TEST_RAW_COMMIT_ERROR"))
				} else {
					mock.ExpectCommit()
				}
			}
			got, err := s.ClaimNotification(context.Background(), now, []string{n.ChatJID}, 10, 30*time.Second)
			if (err != nil) != (tt.queryFail || tt.commitFail) {
				t.Fatal("unexpected claim error")
			}
			if tt.empty || tt.queryFail || tt.commitFail {
				if got != nil {
					t.Fatal("uncommitted work returned")
				}
			} else if got == nil || got.EventID != n.EventID {
				t.Fatal("claim missing")
			}
		})
	}
}

func TestFencedFinish(t *testing.T) {
	for _, rows := range []int64{0, 1} {
		t.Run(string(rune('0'+rows)), func(t *testing.T) {
			s, mock := mockStore(t)
			n := notificationFixture()
			now := n.ReceivedAt
			out := NotificationOutcome{State: "accepted", NextAttemptAt: now}
			mock.ExpectBegin()
			mock.ExpectExec(finishNotificationSQL).WithArgs(n.EventID, n.LeaseToken, out.State, out.Class, 0, now).WillReturnResult(sqlmock.NewResult(0, rows))
			mock.ExpectCommit()
			changed, err := s.FinishNotification(context.Background(), n, out, now)
			if err != nil || changed != (rows == 1) {
				t.Fatal("incorrect lease fencing")
			}
		})
	}
}

func TestDisabledMaintenance(t *testing.T) {
	s, mock := mockStore(t)
	now := testMessage().Timestamp
	mock.ExpectBegin()
	mock.ExpectExec(maintainNotificationsSQL).WithArgs(false, 10, []string{}, 250).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(pruneNotificationsSQL).WithArgs((7 * 24 * time.Hour).Milliseconds(), 250).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if s.MaintainNotifications(context.Background(), now, false, nil, 10, 7*24*time.Hour, 250) != nil {
		t.Fatal("maintenance failed")
	}
}

func TestWriterCancellationAndClosedAdmission(t *testing.T) {
	s, mock := mockStore(t)
	mock.ExpectBegin()
	mock.ExpectCommit()
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() { _ = s.submit(func(*sql.Tx) error { close(entered); <-release; return nil }); close(finished) }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	queued := make(chan error, 1)
	go func() {
		queued <- s.submitContext(ctx, func(*sql.Tx) error { t.Error("cancelled queued operation executed"); return nil })
	}()
	// Wait for admission without sleeps or touching driver state.
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(s.writeCh) == 0 {
		select {
		case <-deadline.C:
			t.Fatal("admission timed out")
		default:
			runtime.Gosched()
		}
	}
	cancel()
	if !errors.Is(<-queued, context.Canceled) {
		t.Fatal("queue cancellation did not return")
	}
	close(release)
	<-finished
	_ = s.Close()
	if err := s.submit(func(*sql.Tx) error { return nil }); err == nil {
		t.Fatal("write admitted after close")
	}
}

func TestMetadataValidationAndSafeClasses(t *testing.T) {
	s, _ := mockStore(t)
	m := testMessage()
	m.ID = ""
	if _, err := s.SaveMessage(context.Background(), m, &NotificationRequest{ReceivedAt: m.Timestamp, MaxAge: time.Hour}); err == nil {
		t.Fatal("invalid metadata admitted")
	}
	if _, err := s.SaveMessage(context.Background(), nil, nil); err == nil {
		t.Fatal("nil message admitted")
	}
	if _, err := s.FinishNotification(context.Background(), notificationFixture(), NotificationOutcome{State: "dead", Class: "TEST_RAW_MARKER"}, time.Now()); err == nil {
		t.Fatal("unsafe class admitted")
	}
	if n, err := s.ClaimNotification(context.Background(), time.Now(), nil, 10, time.Second); n != nil || err != nil {
		t.Fatal("empty allowlist claimed work")
	}
}

func TestMigrationIsAdditiveAndReversible(t *testing.T) {
	up, err := migrations.FS.ReadFile("000011_notification_outbox.up.sql")
	if err != nil {
		t.Fatal("migration missing")
	}
	for _, s := range []string{
		"UNIQUE (event_type, chat_jid, message_id)",
		"REFERENCES messages(id, chat_jid) ON DELETE CASCADE",
		"idx_notification_due",
		"idx_notification_terminal",
		"notification_lease_fields_coherent",
		"notification_terminal_timestamp_coherent",
		"notification_http_status_valid",
		"notification_expiry_valid",
	} {
		if !strings.Contains(string(up), s) {
			t.Fatal("migration constraint missing")
		}
	}
	down, err := migrations.FS.ReadFile("000011_notification_outbox.down.sql")
	if err != nil {
		t.Fatal("down migration missing")
	}
	if !strings.Contains(string(down), "DROP TABLE IF EXISTS notification_outbox;") || strings.Contains(string(down), "DROP TABLE messages") || strings.Contains(string(up), "ALTER TABLE") {
		t.Fatal("migration is not narrowly additive")
	}
}

func TestFinishNotificationFailureInjection(t *testing.T) {
	for _, mode := range []string{"execute", "rows_affected", "commit"} {
		t.Run(mode, func(t *testing.T) {
			s, mock := mockStore(t)
			n := notificationFixture()
			now := n.ReceivedAt
			out := NotificationOutcome{State: "accepted", NextAttemptAt: now}
			failure := errors.New("TEST_RAW_FINISH_ERROR_MUST_NOT_ESCAPE")
			mock.ExpectBegin()
			e := mock.ExpectExec(finishNotificationSQL).WithArgs(n.EventID, n.LeaseToken, out.State, out.Class, 0, now)
			switch mode {
			case "execute":
				e.WillReturnError(failure)
				mock.ExpectRollback()
			case "rows_affected":
				e.WillReturnResult(sqlmock.NewErrorResult(failure))
				mock.ExpectRollback()
			case "commit":
				e.WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit().WillReturnError(failure)
			}
			changed, err := s.FinishNotification(context.Background(), n, out, now)
			if changed || err == nil || err.Error() != "notification_store_failed" {
				t.Fatal("failed acknowledgement reported success or leaked error")
			}
		})
	}
	if !strings.Contains(finishNotificationSQL, "lease_until > clock_timestamp()") {
		t.Fatal("expiry fence uses a stale app timestamp")
	}
}

func TestNotificationSQLUsesPostgreSQLClock(t *testing.T) {
	checks := []struct {
		name  string
		query string
		want  []string
	}{
		{"claim", claimNotificationSQL, []string{
			"SELECT clock_timestamp() AS now",
			"n.next_attempt_at <= c.now AND n.expires_at > c.now",
			"lease_until = c.now + ($4::bigint * interval '1 millisecond')",
		}},
		{"finish", finishNotificationSQL, []string{
			"terminal_at = CASE WHEN $3 = 'pending' THEN NULL ELSE clock_timestamp() END",
			"lease_until > clock_timestamp()",
		}},
		{"maintenance", maintainNotificationsSQL, []string{
			"SELECT clock_timestamp() AS now",
			"n.lease_until <= c.now",
			"terminal_at = CASE WHEN n.expires_at <= c.now",
		}},
		{"prune", pruneNotificationsSQL, []string{
			"terminal_at <= clock_timestamp() - ($1::bigint * interval '1 millisecond')",
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			for _, want := range check.want {
				if !strings.Contains(check.query, want) {
					t.Fatalf("database clock expression missing: %s", want)
				}
			}
		})
	}
}

func TestMaintenanceFailureInjection(t *testing.T) {
	retention := 7 * 24 * time.Hour
	for _, mode := range []string{"maintenance", "prune", "commit"} {
		t.Run(mode, func(t *testing.T) {
			s, mock := mockStore(t)
			failure := errors.New("TEST_RAW_MAINTENANCE_ERROR_MUST_NOT_ESCAPE")
			mock.ExpectBegin()
			maintenance := mock.ExpectExec(maintainNotificationsSQL).WithArgs(true, 10, []string{"100@g.us"}, 250)
			if mode == "maintenance" {
				maintenance.WillReturnError(failure)
				mock.ExpectRollback()
			} else {
				maintenance.WillReturnResult(sqlmock.NewResult(0, 1))
				prune := mock.ExpectExec(pruneNotificationsSQL).WithArgs(retention.Milliseconds(), 250)
				if mode == "prune" {
					prune.WillReturnError(failure)
					mock.ExpectRollback()
				} else {
					prune.WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectCommit().WillReturnError(failure)
				}
			}
			err := s.MaintainNotifications(context.Background(), testMessage().Timestamp, true, []string{"100@g.us"}, 10, retention, 250)
			if err == nil || err.Error() != "notification_store_failed" {
				t.Fatal("maintenance failure missing or leaked")
			}
		})
	}
}
