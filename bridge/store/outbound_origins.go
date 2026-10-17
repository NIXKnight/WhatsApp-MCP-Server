package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const (
	// UnobservedOutboundOriginRetention is a local conservative policy for
	// uncertain sends whose WhatsApp echo has not been observed.
	UnobservedOutboundOriginRetention = 90 * 24 * time.Hour
	// ObservedOutboundOriginRetention is the shorter local policy for observed
	// tombstones. They are eligible only after the message row durably records
	// bridge origin, so pruning cannot change later duplicate classification.
	ObservedOutboundOriginRetention = 7 * 24 * time.Hour
)

const registerOutboundOriginSQL = `INSERT INTO bridge_outbound_origins
 (message_id, chat_jid) VALUES ($1, $2)`

const consumeOutboundOriginSQL = `UPDATE bridge_outbound_origins
 SET observed_at = COALESCE(observed_at, clock_timestamp())
 WHERE message_id = $1 AND chat_jid = $2
 RETURNING message_id`

const pruneUnobservedOutboundOriginsSQL = `DELETE FROM bridge_outbound_origins WHERE (message_id, chat_jid) IN (
 SELECT message_id, chat_jid FROM bridge_outbound_origins
 WHERE observed_at IS NULL
   AND created_at <= clock_timestamp() - ($1::bigint * interval '1 millisecond')
 ORDER BY created_at, message_id, chat_jid
 FOR UPDATE SKIP LOCKED LIMIT $2
)`

const pruneObservedOutboundOriginsSQL = `DELETE FROM bridge_outbound_origins o WHERE (o.message_id, o.chat_jid) IN (
 SELECT candidate.message_id, candidate.chat_jid FROM bridge_outbound_origins candidate
 WHERE candidate.observed_at IS NOT NULL
   AND candidate.observed_at <= clock_timestamp() - ($1::bigint * interval '1 millisecond')
   AND EXISTS (
       SELECT 1 FROM messages m
       WHERE m.id = candidate.message_id AND m.chat_jid = candidate.chat_jid
         AND m.is_bridge_origin = TRUE
   )
 ORDER BY candidate.observed_at, candidate.message_id, candidate.chat_jid
 FOR UPDATE OF candidate SKIP LOCKED LIMIT $2
)`

// RegisterOutboundOrigins durably records every exact plausible event chat
// identity for one generated message ID in a single transaction. Duplicate
// input aliases are collapsed, while an identity already present in the store
// remains an error so message IDs cannot be reused as fresh sends.
func (s *Store) RegisterOutboundOrigins(ctx context.Context, messageID string, chatJIDs []string) error {
	if messageID == "" || len(chatJIDs) == 0 {
		return errors.New("outbound_origin_invalid")
	}
	unique := make([]string, 0, len(chatJIDs))
	seen := make(map[string]struct{}, len(chatJIDs))
	for _, chatJID := range chatJIDs {
		if !storedGroupJID.MatchString(chatJID) && !storedPrivateJID.MatchString(chatJID) {
			return errors.New("outbound_origin_invalid")
		}
		if _, ok := seen[chatJID]; ok {
			continue
		}
		seen[chatJID] = struct{}{}
		unique = append(unique, chatJID)
	}
	err := s.submitContext(ctx, func(tx *sql.Tx) error {
		for _, chatJID := range unique {
			if _, err := tx.ExecContext(ctx, registerOutboundOriginSQL, messageID, chatJID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return errors.New("outbound_origin_store_failed")
	}
	return nil
}

// RegisterOutboundOrigin records one exact identity. New send paths should use
// RegisterOutboundOrigins so all aliases commit atomically.
func (s *Store) RegisterOutboundOrigin(ctx context.Context, messageID, chatJID string) error {
	return s.RegisterOutboundOrigins(ctx, messageID, []string{chatJID})
}

// consumeOutboundOrigin marks an exact bridge-send identity as observed. The
// caller must durably classify the matching message in the same transaction.
func consumeOutboundOrigin(ctx context.Context, tx *sql.Tx, messageID, chatJID string) (bool, error) {
	var matched string
	err := tx.QueryRowContext(ctx, consumeOutboundOriginSQL, messageID, chatJID).Scan(&matched)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
