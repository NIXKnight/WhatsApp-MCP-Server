package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/migrations"
	"github.com/jackc/pgx/v5"
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
	return &MessageRow{ID: "TEST_M1", ChatJID: "100@g.us", Sender: "500@s.whatsapp.net", SenderName: "TEST_NAME", Content: "TEST_CONTENT_NOT_FOR_EVENT", Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}
func messageArgs(m *MessageRow) []driver.Value {
	return []driver.Value{m.ID, m.ChatJID, m.Sender, m.SenderName, m.Content, m.Timestamp, m.IsFromMe, m.MediaType, m.Filename, m.URL, m.MediaKey, m.FileSHA256, m.FileEncSHA256, m.FileLength, m.PushName, m.QuotedMessageID, m.QuotedParticipant}
}
func expectInserted(mock sqlmock.Sqlmock, m *MessageRow) {
	mock.ExpectQuery(insertMessageSQL).WithArgs(messageArgs(m)...).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(m.ID))
}
func testNotificationRequest(m *MessageRow) *NotificationRequest {
	return &NotificationRequest{
		ReceivedAt:       m.Timestamp,
		MaxAge:           time.Hour,
		CanonicalChatJID: m.ChatJID,
		SenderJID:        "500@s.whatsapp.net",
		ChatKind:         "group",
		IsFromMe:         m.IsFromMe,
	}
}

func expectEvent(mock sqlmock.Sqlmock, m *MessageRow, req *NotificationRequest, sequence int64) *sqlmock.ExpectedExec {
	mock.ExpectQuery(allocateNotificationSequenceSQL).WithArgs(req.CanonicalChatJID).
		WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(sequence))
	return mock.ExpectExec(insertNotificationSQL).WithArgs(
		opaqueArgument{}, m.ChatJID, req.CanonicalChatJID, m.ID, m.Timestamp,
		req.ReceivedAt, req.ReceivedAt.Add(req.MaxAge), sequence, req.SenderJID,
		nil, req.ChatKind, req.IsFromMe,
	)
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
		{"sequence failure", "sequence_error", "", true}, {"outbox failure", "event_error", "", true}, {"update failure", "update_error", "", true},
		{"update absent", "update_zero", "", true},
		{"update unexpected count", "update_multiple", "", true},
		{"update rows affected failure", "update_rows_error", "", true},
		{"commit unknown", "commit_error", "", true}, {"begin failure", "begin_error", "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, mock := mockStore(t)
			m := testMessage()
			req := testNotificationRequest(m)
			if tt.mode == "self" {
				m.IsFromMe = true
				req.IsFromMe = true
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
						if req != nil {
							if tt.mode == "sequence_error" {
								mock.ExpectQuery(allocateNotificationSequenceSQL).WithArgs(req.CanonicalChatJID).WillReturnError(failure)
							} else {
								e := expectEvent(mock, m, req, 1)
								if tt.mode == "event_error" {
									e.WillReturnError(failure)
								} else {
									e.WillReturnResult(sqlmock.NewResult(0, 1))
								}
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
	req := testNotificationRequest(m)
	mock.ExpectBegin()
	expectInserted(mock, m)
	expectEvent(mock, m, req, 1).WillReturnResult(sqlmock.NewResult(0, 1))
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

func TestSequencesAreAllocatedOnlyForFirstInsertions(t *testing.T) {
	s, mock := mockStore(t)
	first := testMessage()
	second := testMessage()
	second.ID = "TEST_M2"
	second.Timestamp = second.Timestamp.Add(time.Second)
	for i, message := range []*MessageRow{first, second} {
		req := testNotificationRequest(message)
		mock.ExpectBegin()
		expectInserted(mock, message)
		expectEvent(mock, message, req, int64(i+1)).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		if result, err := s.SaveMessage(context.Background(), message, req); err != nil || result != MessageInserted {
			t.Fatal("sequenced insert failed")
		}
	}
}

func TestOperatorAliasesShareCanonicalSequenceKey(t *testing.T) {
	s, mock := mockStore(t)
	for i, actualChat := range []string{"400@s.whatsapp.net", "300@lid"} {
		message := testMessage()
		message.ID = []string{"TEST_PRIVATE_1", "TEST_PRIVATE_2"}[i]
		message.ChatJID = actualChat
		message.Timestamp = message.Timestamp.Add(time.Duration(i) * time.Second)
		request := &NotificationRequest{
			ReceivedAt:       message.Timestamp,
			MaxAge:           time.Hour,
			CanonicalChatJID: "300@lid",
			SenderJID:        actualChat,
			ChatKind:         "operator_private",
		}
		mock.ExpectBegin()
		expectInserted(mock, message)
		expectEvent(mock, message, request, int64(i+1)).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		if result, err := s.SaveMessage(context.Background(), message, request); err != nil || result != MessageInserted {
			t.Fatal("operator alias sequence insert failed")
		}
	}
}

func TestNotificationSequencePostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	parsed, err := pgx.ParseConfig(databaseURL)
	if err != nil || !strings.Contains(strings.ToLower(parsed.Database), "test") {
		t.Fatal("TEST_DATABASE_URL must target a disposable database whose name contains test")
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := NewStore(databaseURL, log)
	if err != nil {
		t.Fatal("open first disposable store")
	}
	second, err := NewStore(databaseURL, log)
	if err != nil {
		_ = first.Close()
		t.Fatal("open second disposable store")
	}

	const chatJID = "120363000000000001@g.us"
	messageIDs := []string{"TEST_PG_SEQUENCE_1", "TEST_PG_SEQUENCE_2"}
	cleanup := func() {
		if _, err := first.db.Exec(`DELETE FROM notification_outbox WHERE canonical_chat_jid = $1`, chatJID); err != nil {
			t.Fatal("clean synthetic outbox rows")
		}
		if _, err := first.db.Exec(`DELETE FROM messages WHERE chat_jid = $1 AND id = ANY($2::text[])`, chatJID, messageIDs); err != nil {
			t.Fatal("clean synthetic message rows")
		}
		if _, err := first.db.Exec(`DELETE FROM notification_chat_sequences WHERE canonical_chat_jid = $1`, chatJID); err != nil {
			t.Fatal("clean synthetic sequence row")
		}
		if _, err := first.db.Exec(`DELETE FROM chats WHERE jid = $1`, chatJID); err != nil {
			t.Fatal("clean synthetic chat row")
		}
	}
	cleanup()
	t.Cleanup(func() {
		cleanup()
		_ = second.Close()
		_ = first.Close()
	})

	if err := first.UpsertChat(chatJID, "TEST_SEQUENCE_CHAT", true, 0, time.Time{}, ""); err != nil {
		t.Fatal("create synthetic chat")
	}

	rollbackMarker := errors.New("TEST_SEQUENCE_ROLLBACK")
	var rolledBackSequence int64
	err = first.submit(func(tx *sql.Tx) error {
		if err := tx.QueryRow(allocateNotificationSequenceSQL, chatJID).Scan(&rolledBackSequence); err != nil {
			return err
		}
		return rollbackMarker
	})
	if !errors.Is(err, rollbackMarker) || rolledBackSequence != 1 {
		t.Fatal("synthetic sequence allocation did not roll back")
	}

	type saveResult struct {
		result MessageWriteResult
		err    error
	}
	results := make(chan saveResult, len(messageIDs))
	stores := []*Store{first, second}
	var wg sync.WaitGroup
	for i := range messageIDs {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			message := &MessageRow{
				ID:        messageIDs[index],
				ChatJID:   chatJID,
				Sender:    "500@s.whatsapp.net",
				Content:   "TEST_SEQUENCE_CONTENT",
				Timestamp: time.Date(2026, 1, 1, 0, 0, index, 0, time.UTC),
			}
			request := testNotificationRequest(message)
			result, saveErr := stores[index].SaveMessage(context.Background(), message, request)
			results <- saveResult{result: result, err: saveErr}
		}(i)
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.err != nil || result.result != MessageInserted {
			t.Fatal("concurrent synthetic insertion failed")
		}
	}

	rows, err := first.db.Query(`SELECT message_id, chat_sequence FROM notification_outbox
		WHERE canonical_chat_jid = $1 AND message_id = ANY($2::text[])`, chatJID, messageIDs)
	if err != nil {
		t.Fatal("query synthetic sequences")
	}
	defer rows.Close()
	sequenceByMessage := make(map[string]int64, len(messageIDs))
	for rows.Next() {
		var messageID string
		var sequence int64
		if err := rows.Scan(&messageID, &sequence); err != nil {
			t.Fatal("scan synthetic sequence")
		}
		sequenceByMessage[messageID] = sequence
	}
	if err := rows.Err(); err != nil || len(sequenceByMessage) != 2 {
		t.Fatal("synthetic sequence rows missing")
	}
	seenSequences := map[int64]bool{}
	sequenceTwoMessage := ""
	for messageID, sequence := range sequenceByMessage {
		seenSequences[sequence] = true
		if sequence == 2 {
			sequenceTwoMessage = messageID
		}
	}
	if !seenSequences[1] || !seenSequences[2] || sequenceTwoMessage == "" {
		t.Fatal("concurrent allocations were not distinct, contiguous, and rollback-reusing")
	}

	if _, err := first.db.Exec(`UPDATE notification_outbox SET chat_sequence = 1
		WHERE canonical_chat_jid = $1 AND message_id = $2`, chatJID, sequenceTwoMessage); err == nil {
		t.Fatal("database accepted duplicate canonical chat sequence")
	}
}

func notificationRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"event_id", "schema_version", "event_type", "chat_jid", "canonical_chat_jid",
		"message_id", "event_timestamp", "received_at", "expires_at", "chat_sequence",
		"sender_jid", "sender_alt_jid", "chat_kind", "is_from_me", "attempts", "lease_token",
	})
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
					rows.AddRow(n.EventID, n.SchemaVersion, n.EventType, n.ChatJID, nil, n.MessageID, n.EventTimestamp, n.ReceivedAt, n.ExpiresAt, nil, nil, nil, nil, nil, n.Attempts, n.LeaseToken)
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

func TestClaimNotificationV2AndNullability(t *testing.T) {
	v2 := notificationFixture()
	v2.SchemaVersion = 2
	v2.ChatJID = "400@s.whatsapp.net"
	v2.CanonicalChatJID = "300@lid"
	v2.ChatSequence = 41
	v2.SenderJID = "400@s.whatsapp.net"
	v2.SenderAltJID = "300@lid"
	v2.ChatKind = "operator_private"
	v2.IsFromMe = false

	t.Run("v2", func(t *testing.T) {
		s, mock := mockStore(t)
		mock.ExpectBegin()
		mock.ExpectQuery(claimNotificationSQL).
			WithArgs(10, []string{v2.ChatJID}, opaqueArgument{}, int64(30_000)).
			WillReturnRows(notificationRows().AddRow(
				v2.EventID, v2.SchemaVersion, v2.EventType, v2.ChatJID, v2.CanonicalChatJID,
				v2.MessageID, v2.EventTimestamp, v2.ReceivedAt, v2.ExpiresAt, v2.ChatSequence,
				v2.SenderJID, v2.SenderAltJID, v2.ChatKind, v2.IsFromMe, v2.Attempts, v2.LeaseToken,
			))
		mock.ExpectCommit()
		got, err := s.ClaimNotification(context.Background(), v2.ReceivedAt, []string{v2.ChatJID}, 10, 30*time.Second)
		if err != nil || got == nil || got.ChatJID != v2.ChatJID || got.CanonicalChatJID != v2.CanonicalChatJID || got.ChatSequence != 41 || got.SenderAltJID != v2.SenderAltJID {
			t.Fatal("v2 claim metadata changed")
		}
	})

	for _, tt := range []struct {
		name    string
		version int
		values  []any
	}{
		{"v1 with v2 metadata", 1, []any{"100@g.us", nil, nil, nil, nil, nil}},
		{"v2 missing canonical", 2, []any{nil, int64(1), "500@s.whatsapp.net", nil, "group", false}},
		{"v2 missing sequence", 2, []any{"100@g.us", nil, "500@s.whatsapp.net", nil, "group", false}},
		{"v2 missing own boolean", 2, []any{"100@g.us", int64(1), "500@s.whatsapp.net", nil, "group", nil}},
		{"unsupported version", 3, []any{nil, nil, nil, nil, nil, nil}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, mock := mockStore(t)
			n := notificationFixture()
			mock.ExpectBegin()
			mock.ExpectQuery(claimNotificationSQL).
				WithArgs(10, []string{n.ChatJID}, opaqueArgument{}, int64(30_000)).
				WillReturnRows(notificationRows().AddRow(
					n.EventID, tt.version, n.EventType, n.ChatJID, tt.values[0], n.MessageID,
					n.EventTimestamp, n.ReceivedAt, n.ExpiresAt, tt.values[1], tt.values[2],
					tt.values[3], tt.values[4], tt.values[5], n.Attempts, n.LeaseToken,
				))
			mock.ExpectRollback()
			got, err := s.ClaimNotification(context.Background(), n.ReceivedAt, []string{n.ChatJID}, 10, 30*time.Second)
			if got != nil || err == nil || err.Error() != "notification_store_failed" {
				t.Fatal("incoherent row escaped claim validation")
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
	if _, err := s.SaveMessage(context.Background(), m, testNotificationRequest(m)); err == nil {
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

func TestNotificationV2MigrationContract(t *testing.T) {
	upBytes, err := migrations.FS.ReadFile("000012_notification_v2.up.sql")
	if err != nil {
		t.Fatal("v2 up migration missing")
	}
	up := string(upBytes)
	for _, required := range []string{
		"DROP CONSTRAINT notification_outbox_schema_version_check",
		"CREATE TABLE notification_chat_sequences",
		"canonical_chat_jid TEXT PRIMARY KEY",
		"notification_schema_version_supported",
		"notification_v2_metadata_coherent",
		"notification_v2_chat_sequence_unique",
		"UNIQUE (canonical_chat_jid, chat_sequence)",
		"schema_version = 1",
		"schema_version = 2",
		"sender_alt_jid IS NULL",
	} {
		if !strings.Contains(up, required) {
			t.Fatalf("v2 up migration contract missing: %s", required)
		}
	}
	if strings.Contains(up, "CREATE UNIQUE INDEX") || strings.Contains(up, "UNIQUE (event_type, chat_jid, message_id)") ||
		strings.Count(up, "UNIQUE (canonical_chat_jid, chat_sequence)") != 1 {
		t.Fatal("v2 migration uniqueness constraints are missing or redundant")
	}

	downBytes, err := migrations.FS.ReadFile("000012_notification_v2.down.sql")
	if err != nil {
		t.Fatal("v2 down migration missing")
	}
	down := string(downBytes)
	guard := strings.Index(down, "IF EXISTS (SELECT 1 FROM notification_outbox WHERE schema_version = 2)")
	drop := strings.Index(down, "DROP COLUMN is_from_me")
	if guard < 0 || drop < 0 || guard > drop || !strings.Contains(down, "ERRCODE = '55000'") || strings.Contains(down, "DELETE FROM notification_outbox") {
		t.Fatal("v2 down migration does not fail safely before destructive rollback")
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
