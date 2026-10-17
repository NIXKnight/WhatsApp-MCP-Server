package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/migrations"
)

func expectOriginLookup(mock sqlmock.Sqlmock, messageID, chatJID string, matched bool) {
	rows := sqlmock.NewRows([]string{"message_id"})
	if matched {
		rows.AddRow(messageID)
	}
	mock.ExpectQuery(consumeOutboundOriginSQL).WithArgs(messageID, chatJID).WillReturnRows(rows)
	if matched {
		mock.ExpectExec(markMessageBridgeOriginSQL).WithArgs(messageID, chatJID).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
}

func TestRegisterOutboundOriginCommitsBeforeReturning(t *testing.T) {
	s, mock := mockStore(t)
	mock.ExpectBegin()
	mock.ExpectExec(registerOutboundOriginSQL).
		WithArgs("TEST_BRIDGE_ID", "100@g.us").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := s.RegisterOutboundOrigin(context.Background(), "TEST_BRIDGE_ID", "100@g.us"); err != nil {
		t.Fatal("origin registration failed")
	}
}

func TestRegisterOutboundOriginsCommitsAliasesAtomicallyAndDeduplicates(t *testing.T) {
	s, mock := mockStore(t)
	mock.ExpectBegin()
	mock.ExpectExec(registerOutboundOriginSQL).
		WithArgs("TEST_BRIDGE_ID", "100@s.whatsapp.net").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(registerOutboundOriginSQL).
		WithArgs("TEST_BRIDGE_ID", "200@lid").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := s.RegisterOutboundOrigins(context.Background(), "TEST_BRIDGE_ID", []string{
		"100@s.whatsapp.net", "200@lid", "100@s.whatsapp.net",
	}); err != nil {
		t.Fatal("atomic alias registration failed")
	}
}

func TestRegisterOutboundOriginsRollsBackAllAliases(t *testing.T) {
	s, mock := mockStore(t)
	mock.ExpectBegin()
	mock.ExpectExec(registerOutboundOriginSQL).
		WithArgs("TEST_BRIDGE_ID", "100@s.whatsapp.net").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(registerOutboundOriginSQL).
		WithArgs("TEST_BRIDGE_ID", "200@lid").
		WillReturnError(errors.New("TEST_PRIVATE_DATABASE_ERROR"))
	mock.ExpectRollback()
	if err := s.RegisterOutboundOrigins(context.Background(), "TEST_BRIDGE_ID", []string{
		"100@s.whatsapp.net", "200@lid",
	}); err == nil || err.Error() != "outbound_origin_store_failed" {
		t.Fatal("partial alias registration did not fail closed")
	}
}

func TestRegisterOutboundOriginFailsClosed(t *testing.T) {
	for _, tt := range []struct {
		name               string
		messageID, chatJID string
		databaseFailure    bool
	}{
		{name: "empty message ID", chatJID: "100@g.us"},
		{name: "invalid chat", messageID: "TEST_BRIDGE_ID", chatJID: "not-a-jid"},
		{name: "database failure", messageID: "TEST_BRIDGE_ID", chatJID: "100@g.us", databaseFailure: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, mock := mockStore(t)
			if tt.databaseFailure {
				mock.ExpectBegin()
				mock.ExpectExec(registerOutboundOriginSQL).WithArgs(tt.messageID, tt.chatJID).
					WillReturnError(errors.New("TEST_PRIVATE_DATABASE_ERROR"))
				mock.ExpectRollback()
			}
			err := s.RegisterOutboundOrigin(context.Background(), tt.messageID, tt.chatJID)
			if err == nil {
				t.Fatal("invalid or failed registration succeeded")
			}
			if tt.databaseFailure && err.Error() != "outbound_origin_store_failed" {
				t.Fatal("database detail escaped registration boundary")
			}
		})
	}
}

func TestBridgeOriginEchoPersistsWithoutNotificationSequence(t *testing.T) {
	s, mock := mockStore(t)
	message := testMessage()
	message.IsFromMe = true
	request := testNotificationRequest(message)
	// A matched bridge origin never needs notification metadata. This also
	// proves suppression precedes notification admission.
	request.SenderJID = "TEST_INVALID_NOTIFICATION_SENDER"

	mock.ExpectBegin()
	expectInserted(mock, message)
	expectOriginLookup(mock, message.ID, message.ChatJID, true)
	mock.ExpectCommit()

	result, err := s.SaveMessage(context.Background(), message, request)
	if err != nil || result != MessageInserted {
		t.Fatal("bridge echo was not persisted")
	}
}

func TestManualOwnEchoStillCreatesNotification(t *testing.T) {
	s, mock := mockStore(t)
	message := testMessage()
	message.IsFromMe = true
	request := testNotificationRequest(message)

	mock.ExpectBegin()
	expectInserted(mock, message)
	expectOriginLookup(mock, message.ID, message.ChatJID, false)
	expectEvent(mock, message, request, 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := s.SaveMessage(context.Background(), message, request)
	if err != nil || result != MessageInserted {
		t.Fatal("manual own echo did not create its event")
	}
}

func TestOutboundOriginMatchingIsExactAndOwnOnly(t *testing.T) {
	for _, tt := range []struct {
		name      string
		messageID string
		chatJID   string
		isFromMe  bool
		queryArgs []driver.Value
	}{
		{name: "mismatched message ID", messageID: "TEST_OTHER_ID", chatJID: "100@g.us", isFromMe: true, queryArgs: []driver.Value{"TEST_OTHER_ID", "100@g.us"}},
		{name: "mismatched chat", messageID: "TEST_M1", chatJID: "200@g.us", isFromMe: true, queryArgs: []driver.Value{"TEST_M1", "200@g.us"}},
		{name: "inbound exact identity", messageID: "TEST_M1", chatJID: "100@g.us", isFromMe: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, mock := mockStore(t)
			message := testMessage()
			message.ID = tt.messageID
			message.ChatJID = tt.chatJID
			message.IsFromMe = tt.isFromMe
			request := testNotificationRequest(message)

			mock.ExpectBegin()
			expectInserted(mock, message)
			if tt.isFromMe {
				mock.ExpectQuery(consumeOutboundOriginSQL).WithArgs(tt.queryArgs...).
					WillReturnRows(sqlmock.NewRows([]string{"message_id"}))
			}
			expectEvent(mock, message, request, 1).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()

			result, err := s.SaveMessage(context.Background(), message, request)
			if err != nil || result != MessageInserted {
				t.Fatal("non-matching event was suppressed")
			}
		})
	}
}

func TestBridgeOriginDuplicateDoesNotNotify(t *testing.T) {
	s, mock := mockStore(t)
	message := testMessage()
	message.IsFromMe = true
	request := testNotificationRequest(message)

	mock.ExpectBegin()
	expectInserted(mock, message)
	expectOriginLookup(mock, message.ID, message.ChatJID, true)
	mock.ExpectCommit()
	mock.ExpectBegin()
	expectUpdated(mock, message, true)
	mock.ExpectCommit()

	first, firstErr := s.SaveMessage(context.Background(), message, request)
	second, secondErr := s.SaveMessage(context.Background(), message, request)
	if firstErr != nil || secondErr != nil || first != MessageInserted || second != MessageUpdated {
		t.Fatal("duplicate bridge echo handling changed")
	}
}

func TestManualHistoryFirstLiveSecondCreatesOneNotification(t *testing.T) {
	s, mock := mockStore(t)
	message := testMessage()
	message.IsFromMe = true
	request := testNotificationRequest(message)

	mock.ExpectBegin()
	expectInserted(mock, message)
	expectOriginLookup(mock, message.ID, message.ChatJID, false)
	mock.ExpectCommit()

	mock.ExpectBegin()
	expectUpdated(mock, message, false)
	expectOriginLookup(mock, message.ID, message.ChatJID, false)
	expectNotificationExists(mock, message, false)
	expectEvent(mock, message, request, 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	expectUpdated(mock, message, false)
	expectOriginLookup(mock, message.ID, message.ChatJID, false)
	expectNotificationExists(mock, message, true)
	mock.ExpectCommit()

	if result, err := s.SaveMessage(context.Background(), message, nil); err != nil || result != MessageInserted {
		t.Fatal("manual history import failed")
	}
	if result, err := s.SaveMessage(context.Background(), message, request); err != nil || result != MessageUpdated {
		t.Fatal("manual live duplicate did not create its first notification")
	}
	if result, err := s.SaveMessage(context.Background(), message, request); err != nil || result != MessageUpdated {
		t.Fatal("notified duplicate was not idempotent")
	}
}

func TestBridgeHistoryFirstLiveSecondRemainsSuppressedAfterMarkerPrune(t *testing.T) {
	s, mock := mockStore(t)
	message := testMessage()
	message.IsFromMe = true
	request := testNotificationRequest(message)

	mock.ExpectBegin()
	expectInserted(mock, message)
	expectOriginLookup(mock, message.ID, message.ChatJID, true)
	mock.ExpectCommit()

	// This duplicate consults the durable messages classification. No origin
	// marker lookup is expected, which models processing after tombstone prune.
	mock.ExpectBegin()
	expectUpdated(mock, message, true)
	mock.ExpectCommit()

	if result, err := s.SaveMessage(context.Background(), message, nil); err != nil || result != MessageInserted {
		t.Fatal("bridge history import failed")
	}
	if result, err := s.SaveMessage(context.Background(), message, request); err != nil || result != MessageUpdated {
		t.Fatal("bridge live duplicate was not durably suppressed")
	}
}

func TestOutboundOriginMigrationAndRetentionContract(t *testing.T) {
	upBytes, err := migrations.FS.ReadFile("000013_bridge_outbound_origins.up.sql")
	if err != nil {
		t.Fatal("outbound-origin migration missing")
	}
	up := string(upBytes)
	for _, required := range []string{
		"ADD COLUMN is_bridge_origin BOOLEAN NOT NULL DEFAULT FALSE",
		"PRIMARY KEY (message_id, chat_jid)",
		"created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()",
		"observed_at TIMESTAMPTZ",
		"idx_bridge_outbound_origins_unobserved",
		"WHERE observed_at IS NULL",
		"idx_bridge_outbound_origins_observed",
		"WHERE observed_at IS NOT NULL",
	} {
		if !strings.Contains(up, required) {
			t.Fatalf("outbound-origin migration contract missing: %s", required)
		}
	}
	if strings.Contains(up, "REFERENCES messages") {
		t.Fatal("pre-send origin registration incorrectly depends on a message row")
	}
	if UnobservedOutboundOriginRetention != 90*24*time.Hour || ObservedOutboundOriginRetention != 7*24*time.Hour {
		t.Fatal("local origin retention horizons changed")
	}
	for name, query := range map[string]string{
		"unobserved": pruneUnobservedOutboundOriginsSQL,
		"observed":   pruneObservedOutboundOriginsSQL,
	} {
		if !strings.Contains(query, "clock_timestamp()") || !strings.Contains(query, "SKIP LOCKED LIMIT $2") {
			t.Fatalf("%s cleanup is not execution-clock bounded", name)
		}
	}
	if !strings.Contains(pruneUnobservedOutboundOriginsSQL, "observed_at IS NULL") ||
		!strings.Contains(pruneUnobservedOutboundOriginsSQL, "created_at <=") {
		t.Fatal("unobserved retention predicate changed")
	}
	if !strings.Contains(pruneObservedOutboundOriginsSQL, "candidate.observed_at IS NOT NULL") ||
		!strings.Contains(pruneObservedOutboundOriginsSQL, "candidate.observed_at <=") ||
		!strings.Contains(pruneObservedOutboundOriginsSQL, "m.is_bridge_origin = TRUE") {
		t.Fatal("observed retention lacks durable-classification gate")
	}

	downBytes, err := migrations.FS.ReadFile("000013_bridge_outbound_origins.down.sql")
	if err != nil {
		t.Fatal("outbound-origin down migration missing")
	}
	down := string(downBytes)
	dropTable := strings.Index(down, "DROP TABLE IF EXISTS bridge_outbound_origins")
	dropColumn := strings.Index(down, "DROP COLUMN IF EXISTS is_bridge_origin")
	if !strings.Contains(down, "restore (but do not start) the old binary") ||
		!strings.Contains(down, "loses all bridge") || dropTable < 0 || dropColumn < 0 || dropTable > dropColumn {
		t.Fatal("destructive rollback ordering or evidence-loss warning missing")
	}
}

func TestFailedEchoTransactionRetainsOriginForRetry(t *testing.T) {
	s, mock := mockStore(t)
	message := testMessage()
	message.IsFromMe = true
	request := testNotificationRequest(message)

	mock.ExpectBegin()
	expectInserted(mock, message)
	expectOriginLookup(mock, message.ID, message.ChatJID, true)
	mock.ExpectCommit().WillReturnError(errors.New("TEST_COMMIT_OUTCOME_UNKNOWN"))

	if _, err := s.SaveMessage(context.Background(), message, request); err == nil {
		t.Fatal("failed transaction reported success")
	}

	// The first transaction's marker observation rolls back with its message
	// insert, so a later delivery can still match and suppress the same origin.
	mock.ExpectBegin()
	expectInserted(mock, message)
	expectOriginLookup(mock, message.ID, message.ChatJID, true)
	mock.ExpectCommit()
	if result, err := s.SaveMessage(context.Background(), message, request); err != nil || result != MessageInserted {
		t.Fatal("retry did not retain suppression safety")
	}
}
