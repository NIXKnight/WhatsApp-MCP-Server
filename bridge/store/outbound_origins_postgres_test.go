package store

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestOutboundOriginRestartDurabilityPostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	parsed, err := pgx.ParseConfig(databaseURL)
	if err != nil || !strings.Contains(strings.ToLower(parsed.Database), "test") {
		t.Fatal("TEST_DATABASE_URL must target a disposable database whose name contains test")
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := NewStore(databaseURL, logger)
	if err != nil {
		t.Fatal("open first disposable store")
	}
	const chatJID = "120363000000000013@g.us"
	const otherChatJID = "120363000000000014@g.us"
	const bridgeID = "TEST_RESTART_BRIDGE_ORIGIN"
	const manualID = "TEST_RESTART_MANUAL_OWN"
	const exactOriginID = "TEST_EXACT_ORIGIN"
	const differentID = "TEST_DIFFERENT_ORIGIN_ID"
	const observedAgedID = "TEST_OBSERVED_AGED_ORIGIN"
	const observedUnclassifiedID = "TEST_OBSERVED_UNCLASSIFIED_ORIGIN"
	const unobservedRecentID = "TEST_UNOBSERVED_RECENT_ORIGIN"
	const unobservedAgedID = "TEST_UNOBSERVED_AGED_ORIGIN"
	messageIDs := []string{
		bridgeID, manualID, exactOriginID, differentID, observedAgedID,
		observedUnclassifiedID, unobservedRecentID, unobservedAgedID,
	}
	chatJIDs := []string{chatJID, otherChatJID}
	cleanup := func(s *Store) {
		_, _ = s.db.Exec(`DELETE FROM notification_outbox WHERE chat_jid = ANY($1::text[])`, chatJIDs)
		_, _ = s.db.Exec(`DELETE FROM messages WHERE chat_jid = ANY($1::text[]) AND id = ANY($2::text[])`, chatJIDs, messageIDs)
		_, _ = s.db.Exec(`DELETE FROM bridge_outbound_origins WHERE chat_jid = ANY($1::text[]) AND message_id = ANY($2::text[])`, chatJIDs, messageIDs)
		_, _ = s.db.Exec(`DELETE FROM notification_chat_sequences WHERE canonical_chat_jid = ANY($1::text[])`, chatJIDs)
		_, _ = s.db.Exec(`DELETE FROM chats WHERE jid = ANY($1::text[])`, chatJIDs)
	}
	cleanup(first)
	if err := first.UpsertChat(chatJID, "TEST_ORIGIN_RESTART_CHAT", true, 0, time.Time{}, ""); err != nil {
		_ = first.Close()
		t.Fatal("create synthetic chat")
	}
	if err := first.UpsertChat(otherChatJID, "TEST_ORIGIN_EXACT_CHAT", true, 0, time.Time{}, ""); err != nil {
		_ = first.Close()
		t.Fatal("create second synthetic chat")
	}
	if err := first.RegisterOutboundOrigin(context.Background(), bridgeID, chatJID); err != nil {
		_ = first.Close()
		t.Fatal("register synthetic outbound origin")
	}
	if err := first.Close(); err != nil {
		t.Fatal("close first store")
	}

	second, err := NewStore(databaseURL, logger)
	if err != nil {
		t.Fatal("reopen disposable store")
	}
	concurrent, err := NewStore(databaseURL, logger)
	if err != nil {
		_ = second.Close()
		t.Fatal("open concurrent disposable store")
	}
	t.Cleanup(func() {
		cleanup(second)
		_ = concurrent.Close()
		_ = second.Close()
	})

	timestamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bridgeMessage := &MessageRow{
		ID: bridgeID, ChatJID: chatJID, Sender: "500@s.whatsapp.net",
		Content: "TEST_BRIDGE_ORIGIN_CONTENT", Timestamp: timestamp, IsFromMe: true,
	}
	bridgeRequest := testNotificationRequest(bridgeMessage)
	if result, err := second.SaveMessage(context.Background(), bridgeMessage, nil); err != nil || result != MessageInserted {
		t.Fatal("history-first restart echo persistence failed")
	}
	persisted, err := second.GetMessageByID(bridgeID, chatJID)
	if err != nil || persisted.ID != bridgeID || !persisted.IsFromMe || !persisted.IsBridgeOrigin {
		t.Fatal("suppressed echo lacks durable message classification")
	}
	// Model observed-tombstone pruning directly. The later live duplicate must
	// still consult messages.is_bridge_origin and remain suppressed.
	if _, err := second.db.Exec(`DELETE FROM bridge_outbound_origins WHERE chat_jid = $1 AND message_id = $2`, chatJID, bridgeID); err != nil {
		t.Fatal("remove synthetic observed marker")
	}
	if result, err := second.SaveMessage(context.Background(), bridgeMessage, bridgeRequest); err != nil || result != MessageUpdated {
		t.Fatal("history-first bridge live duplicate was not suppressed")
	}

	var outboxCount, sequenceCount int
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM notification_outbox WHERE chat_jid = $1 AND message_id = $2`, chatJID, bridgeID).Scan(&outboxCount); err != nil {
		t.Fatal("count bridge notification")
	}
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM notification_chat_sequences WHERE canonical_chat_jid = $1`, chatJID).Scan(&sequenceCount); err != nil {
		t.Fatal("count bridge sequence")
	}
	if outboxCount != 0 || sequenceCount != 0 {
		t.Fatal("bridge echo created notification or consumed sequence")
	}

	manualMessage := &MessageRow{
		ID: manualID, ChatJID: chatJID, Sender: "500@s.whatsapp.net",
		Content: "TEST_MANUAL_OWN_CONTENT", Timestamp: timestamp.Add(time.Second), IsFromMe: true,
	}
	manualRequest := testNotificationRequest(manualMessage)
	if result, err := second.SaveMessage(context.Background(), manualMessage, nil); err != nil || result != MessageInserted {
		t.Fatal("manual history persistence failed")
	}
	type concurrentResult struct {
		result MessageWriteResult
		err    error
	}
	results := make(chan concurrentResult, 2)
	for _, current := range []*Store{second, concurrent} {
		go func(s *Store) {
			result, saveErr := s.SaveMessage(context.Background(), manualMessage, manualRequest)
			results <- concurrentResult{result: result, err: saveErr}
		}(current)
	}
	for range 2 {
		got := <-results
		if got.err != nil || got.result != MessageUpdated {
			t.Fatal("concurrent manual live duplicate failed")
		}
	}
	var manualSequence int64
	if err := second.db.QueryRow(`SELECT chat_sequence FROM notification_outbox WHERE chat_jid = $1 AND message_id = $2`, chatJID, manualID).Scan(&manualSequence); err != nil {
		t.Fatal("manual own notification missing")
	}
	if manualSequence != 1 {
		t.Fatal("suppressed bridge echo consumed a sequence")
	}
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM notification_outbox WHERE chat_jid = $1 AND message_id = $2`, chatJID, manualID).Scan(&outboxCount); err != nil {
		t.Fatal("count manual notifications")
	}
	if err := second.db.QueryRow(`SELECT last_sequence FROM notification_chat_sequences WHERE canonical_chat_jid = $1`, chatJID).Scan(&sequenceCount); err != nil {
		t.Fatal("read manual sequence allocation")
	}
	if outboxCount != 1 || sequenceCount != 1 {
		t.Fatal("manual duplicate allocated a second outbox event or sequence")
	}

	if err := second.RegisterOutboundOrigin(context.Background(), exactOriginID, chatJID); err != nil {
		t.Fatal("register exact-match fixture")
	}
	mismatchedID := &MessageRow{
		ID: differentID, ChatJID: chatJID, Sender: "500@s.whatsapp.net",
		Content: "TEST_DIFFERENT_ID_CONTENT", Timestamp: timestamp.Add(2 * time.Second), IsFromMe: true,
	}
	if result, err := second.SaveMessage(context.Background(), mismatchedID, testNotificationRequest(mismatchedID)); err != nil || result != MessageInserted {
		t.Fatal("different message ID was incorrectly suppressed")
	}
	mismatchedChat := &MessageRow{
		ID: exactOriginID, ChatJID: otherChatJID, Sender: "500@s.whatsapp.net",
		Content: "TEST_DIFFERENT_CHAT_CONTENT", Timestamp: timestamp.Add(3 * time.Second), IsFromMe: true,
	}
	if result, err := second.SaveMessage(context.Background(), mismatchedChat, testNotificationRequest(mismatchedChat)); err != nil || result != MessageInserted {
		t.Fatal("different chat was incorrectly suppressed")
	}
	inboundExact := &MessageRow{
		ID: exactOriginID, ChatJID: chatJID, Sender: "500@s.whatsapp.net",
		Content: "TEST_INBOUND_EXACT_CONTENT", Timestamp: timestamp.Add(4 * time.Second), IsFromMe: false,
	}
	if result, err := second.SaveMessage(context.Background(), inboundExact, testNotificationRequest(inboundExact)); err != nil || result != MessageInserted {
		t.Fatal("inbound exact identity was incorrectly suppressed")
	}

	var exactEvents, unobservedOrigins int
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM notification_outbox
		WHERE (chat_jid = $1 AND message_id IN ($2, $3)) OR (chat_jid = $4 AND message_id = $2)`,
		chatJID, exactOriginID, differentID, otherChatJID).Scan(&exactEvents); err != nil {
		t.Fatal("count exact-match notifications")
	}
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM bridge_outbound_origins
		WHERE chat_jid = $1 AND message_id = $2 AND observed_at IS NULL`, chatJID, exactOriginID).Scan(&unobservedOrigins); err != nil {
		t.Fatal("check unmatched origin marker")
	}
	if exactEvents != 3 || unobservedOrigins != 1 {
		t.Fatal("origin matching was not exact on own-message ID and chat")
	}

	for _, id := range []string{observedAgedID, observedUnclassifiedID, unobservedRecentID, unobservedAgedID} {
		if err := second.RegisterOutboundOrigin(context.Background(), id, chatJID); err != nil {
			t.Fatal("register retention fixture")
		}
	}
	observedMessage := &MessageRow{
		ID: observedAgedID, ChatJID: chatJID, Sender: "500@s.whatsapp.net",
		Content: "TEST_OBSERVED_AGED_CONTENT", Timestamp: timestamp.Add(5 * time.Second), IsFromMe: true,
	}
	if result, err := second.SaveMessage(context.Background(), observedMessage, nil); err != nil || result != MessageInserted {
		t.Fatal("classify observed retention fixture")
	}
	agedObservedAt := time.Now().UTC().Add(-ObservedOutboundOriginRetention - time.Hour)
	if _, err := second.db.Exec(`UPDATE bridge_outbound_origins SET created_at = $3, observed_at = $3
		WHERE chat_jid = $1 AND message_id IN ($2, $4)`, chatJID, observedAgedID, agedObservedAt, observedUnclassifiedID); err != nil {
		t.Fatal("age observed retention fixtures")
	}
	recentUnobservedAt := time.Now().UTC().Add(-ObservedOutboundOriginRetention - time.Hour)
	agedUnobservedAt := time.Now().UTC().Add(-UnobservedOutboundOriginRetention - time.Hour)
	if _, err := second.db.Exec(`UPDATE bridge_outbound_origins SET created_at = CASE message_id WHEN $2 THEN $3 ELSE $4 END
		WHERE chat_jid = $1 AND message_id IN ($2, $5)`, chatJID, unobservedRecentID, recentUnobservedAt, agedUnobservedAt, unobservedAgedID); err != nil {
		t.Fatal("age unobserved retention fixtures")
	}
	if err := second.submit(func(tx *sql.Tx) error {
		if _, err := tx.Exec(pruneObservedOutboundOriginsSQL, ObservedOutboundOriginRetention.Milliseconds(), 250); err != nil {
			return err
		}
		_, err := tx.Exec(pruneUnobservedOutboundOriginsSQL, UnobservedOutboundOriginRetention.Milliseconds(), 250)
		return err
	}); err != nil {
		t.Fatal("prune retention fixtures")
	}

	var retained int
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM bridge_outbound_origins WHERE chat_jid = $1
		AND message_id IN ($2, $3)`, chatJID, observedUnclassifiedID, unobservedRecentID).Scan(&retained); err != nil {
		t.Fatal("count conservatively retained origins")
	}
	var pruned int
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM bridge_outbound_origins WHERE chat_jid = $1
		AND message_id IN ($2, $3)`, chatJID, observedAgedID, unobservedAgedID).Scan(&pruned); err != nil {
		t.Fatal("count pruned origins")
	}
	if retained != 2 || pruned != 0 {
		t.Fatal("observed and unobserved retention predicates changed")
	}
}
