package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"regexp"
	"time"
)

var storedGroupJID = regexp.MustCompile(`^\d+@g\.us$`)
var storedPrivateJID = regexp.MustCompile(`^\d+@(s\.whatsapp\.net|lid)$`)
var storedParticipantJID = regexp.MustCompile(`^\d+@(s\.whatsapp\.net|lid|bot|hosted|hosted\.lid)$`)

// Notification is an immutable event plus mutable delivery bookkeeping. It
// contains routing metadata, but never message content or media material.
type Notification struct {
	EventID          string
	SchemaVersion    int
	EventType        string
	ChatJID          string
	CanonicalChatJID string
	MessageID        string
	EventTimestamp   time.Time
	ReceivedAt       time.Time
	ExpiresAt        time.Time
	ChatSequence     int64
	SenderJID        string
	SenderAltJID     string
	ChatKind         string
	IsFromMe         bool
	Attempts         int
	LeaseToken       string
}

// NotificationOutcome records receiver acceptance, not agent completion.
// Class must be one of the fixed, non-sensitive failure classes below.
type NotificationOutcome struct {
	State         string
	Class         string
	HTTPStatus    int
	NextAttemptAt time.Time
}

const allocateNotificationSequenceSQL = `INSERT INTO notification_chat_sequences
 (canonical_chat_jid, last_sequence) VALUES ($1, 1)
 ON CONFLICT (canonical_chat_jid) DO UPDATE
 SET last_sequence = notification_chat_sequences.last_sequence + 1
 RETURNING last_sequence`

const insertNotificationSQL = `INSERT INTO notification_outbox
 (event_id, schema_version, event_type, chat_jid, canonical_chat_jid, message_id,
  event_timestamp, received_at, expires_at, next_attempt_at, chat_sequence,
  sender_jid, sender_alt_jid, chat_kind, is_from_me)
 VALUES ($1,2,'whatsapp.message.received',$2,$3,$4,$5,$6,$7,$6,$8,$9,$10,$11,$12)`

const claimNotificationSQL = `WITH statement_clock AS MATERIALIZED (
 SELECT clock_timestamp() AS now
), candidate AS (
 SELECT n.event_id FROM notification_outbox n CROSS JOIN statement_clock c
 WHERE n.state = 'pending' AND n.next_attempt_at <= c.now AND n.expires_at > c.now
   AND n.attempts < $1 AND n.chat_jid = ANY($2::text[])
 ORDER BY n.next_attempt_at, n.received_at, n.event_id
 FOR UPDATE OF n SKIP LOCKED LIMIT 1
)
 UPDATE notification_outbox n SET state = 'leased', lease_token = $3,
 lease_until = c.now + ($4::bigint * interval '1 millisecond'), attempts = n.attempts + 1
 FROM candidate candidate, statement_clock c WHERE n.event_id = candidate.event_id
 RETURNING n.event_id, n.schema_version, n.event_type, n.chat_jid,
 n.canonical_chat_jid, n.message_id, n.event_timestamp, n.received_at, n.expires_at,
 n.chat_sequence, n.sender_jid, n.sender_alt_jid, n.chat_kind, n.is_from_me,
 n.attempts, n.lease_token`

const finishNotificationSQL = `UPDATE notification_outbox
 SET state = $3, last_error_class = $4, last_http_status = $5,
 next_attempt_at = $6, terminal_at = CASE WHEN $3 = 'pending' THEN NULL ELSE clock_timestamp() END,
 lease_token = NULL, lease_until = NULL
 WHERE event_id = $1 AND lease_token = $2 AND state = 'leased' AND lease_until > clock_timestamp()`

// Maintenance handles at most limit rows in each statement. Disabled mode
// ignores allowlist/attempt policy but still expires work and prunes metadata.
const maintainNotificationsSQL = `WITH statement_clock AS MATERIALIZED (
 SELECT clock_timestamp() AS now
), candidates AS (
 SELECT n.event_id FROM notification_outbox n CROSS JOIN statement_clock c
 WHERE (n.state = 'pending' OR (n.state = 'leased' AND n.lease_until <= c.now))
 AND (n.expires_at <= c.now OR ($1 AND (n.attempts >= $2 OR NOT (n.chat_jid = ANY($3::text[]))))
      OR n.state = 'leased')
 ORDER BY n.received_at, n.event_id FOR UPDATE OF n SKIP LOCKED LIMIT $4
)
 UPDATE notification_outbox n SET
 state = CASE WHEN n.expires_at <= c.now OR ($1 AND (n.attempts >= $2 OR NOT (n.chat_jid = ANY($3::text[])))) THEN 'dead' ELSE 'pending' END,
 last_error_class = CASE WHEN n.expires_at <= c.now THEN 'expired'
     WHEN $1 AND NOT (n.chat_jid = ANY($3::text[])) THEN 'policy_removed'
     WHEN $1 AND n.attempts >= $2 THEN 'attempts_exhausted' ELSE 'lease_expired' END,
 terminal_at = CASE WHEN n.expires_at <= c.now OR ($1 AND (n.attempts >= $2 OR NOT (n.chat_jid = ANY($3::text[])))) THEN c.now ELSE NULL END,
 lease_token = NULL, lease_until = NULL
 FROM candidates candidate, statement_clock c WHERE n.event_id = candidate.event_id`

const pruneNotificationsSQL = `DELETE FROM notification_outbox WHERE event_id IN (
 SELECT event_id FROM notification_outbox
 WHERE state IN ('accepted','ignored','dead')
   AND terminal_at <= clock_timestamp() - ($1::bigint * interval '1 millisecond')
 ORDER BY terminal_at, event_id FOR UPDATE SKIP LOCKED LIMIT $2
)`

func validV2NotificationRequest(message MessageRow, request NotificationRequest) bool {
	if message.ID == "" || message.ChatJID == "" || message.Timestamp.IsZero() ||
		request.ReceivedAt.IsZero() || request.MaxAge <= 0 || request.MaxAge > 7*24*time.Hour ||
		request.IsFromMe != message.IsFromMe || !storedParticipantJID.MatchString(request.SenderJID) ||
		(request.SenderAltJID != "" && !storedParticipantJID.MatchString(request.SenderAltJID)) {
		return false
	}
	switch request.ChatKind {
	case "group":
		return storedGroupJID.MatchString(message.ChatJID) && request.CanonicalChatJID == message.ChatJID
	case "operator_private":
		return storedPrivateJID.MatchString(message.ChatJID) && storedPrivateJID.MatchString(request.CanonicalChatJID)
	default:
		return false
	}
}

func newNotificationID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", errors.New("notification_entropy_failed")
	}
	return hex.EncodeToString(b[:]), nil
}

func notificationStoreError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("notification_store_failed")
}

func populateClaimedNotification(n *Notification, canonical sql.NullString, sequence sql.NullInt64, sender, senderAlt, kind sql.NullString, fromMe sql.NullBool) error {
	switch n.SchemaVersion {
	case 1:
		if canonical.Valid || sequence.Valid || sender.Valid || senderAlt.Valid || kind.Valid || fromMe.Valid {
			return errors.New("notification_metadata_invalid")
		}
		// Schema v1 has no canonical identity or v2 routing metadata. Keep every
		// v2 field at its zero value; the legacy serializer emits seven fields.
	case 2:
		if !canonical.Valid || canonical.String == "" || !sequence.Valid || sequence.Int64 < 1 ||
			!sender.Valid || sender.String == "" || !kind.Valid || kind.String == "" || !fromMe.Valid {
			return errors.New("notification_metadata_invalid")
		}
		n.CanonicalChatJID = canonical.String
		n.ChatSequence = sequence.Int64
		n.SenderJID = sender.String
		if senderAlt.Valid {
			n.SenderAltJID = senderAlt.String
		}
		n.ChatKind = kind.String
		n.IsFromMe = fromMe.Bool
	default:
		return errors.New("notification_metadata_invalid")
	}
	return nil
}

// ClaimNotification commits one exclusive, bounded lease before returning work.
// PostgreSQL's execution clock determines due/expiry eligibility and lease start.
// A nil result means no due work. Claim attempts count even if a crash prevents
// sending, so repeated crashes cannot bypass the configured attempt bound.
func (s *Store) ClaimNotification(ctx context.Context, _ time.Time, allowed []string, maxAttempts int, lease time.Duration) (*Notification, error) {
	if len(allowed) == 0 {
		return nil, nil
	}
	leaseMilliseconds := lease.Milliseconds()
	if maxAttempts < 1 || leaseMilliseconds < 1 {
		return nil, errors.New("notification_claim_invalid")
	}
	token, err := newNotificationID()
	if err != nil {
		return nil, err
	}
	allowed = append([]string(nil), allowed...)
	var result *Notification
	err = s.submitContext(ctx, func(tx *sql.Tx) error {
		var n Notification
		var canonical, sender, senderAlt, kind sql.NullString
		var sequence sql.NullInt64
		var fromMe sql.NullBool
		err := tx.QueryRowContext(ctx, claimNotificationSQL, maxAttempts, allowed, token, leaseMilliseconds).Scan(
			&n.EventID, &n.SchemaVersion, &n.EventType, &n.ChatJID, &canonical,
			&n.MessageID, &n.EventTimestamp, &n.ReceivedAt, &n.ExpiresAt, &sequence,
			&sender, &senderAlt, &kind, &fromMe, &n.Attempts, &n.LeaseToken)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := populateClaimedNotification(&n, canonical, sequence, sender, senderAlt, kind, fromMe); err != nil {
			return err
		}
		result = &n
		return nil
	})
	if err != nil {
		return nil, notificationStoreError(ctx)
	}
	return result, nil
}

var notificationClasses = map[string]bool{
	"": true, "network": true, "tls": true, "destination_policy": true,
	"http_permanent": true, "http_retry": true, "rate_limited": true,
	"protocol": true, "receiver_ignored": true, "expired": true,
	"attempts_exhausted": true, "policy_removed": true, "cancelled": true, "metadata_invalid": true,
}

// FinishNotification fences updates by the still-current unexpired lease.
// PostgreSQL execution wall clock, not a pre-queue app timestamp, checks expiry
// and records terminal transitions. False means ownership was lost; it must not
// be treated as an acknowledgement.
func (s *Store) FinishNotification(ctx context.Context, n Notification, outcome NotificationOutcome, _ time.Time) (bool, error) {
	if !notificationClasses[outcome.Class] || (outcome.State != "pending" && outcome.State != "accepted" && outcome.State != "ignored" && outcome.State != "dead") || outcome.HTTPStatus < 0 || outcome.HTTPStatus > 599 {
		return false, errors.New("notification_outcome_invalid")
	}
	var changed bool
	err := s.submitContext(ctx, func(tx *sql.Tx) error {
		r, err := tx.ExecContext(ctx, finishNotificationSQL, n.EventID, n.LeaseToken, outcome.State, outcome.Class, outcome.HTTPStatus, outcome.NextAttemptAt.UTC())
		if err != nil {
			return err
		}
		count, err := r.RowsAffected()
		changed = count == 1
		return err
	})
	if err != nil {
		return false, notificationStoreError(ctx)
	}
	return changed, nil
}

// MaintainNotifications performs bounded notification maintenance and outbound
// origin pruning. PostgreSQL's execution clock drives every cutoff and
// transition. It makes no network requests and runs even when delivery is
// disabled, so uncertain-send markers do not depend on controller enablement.
func (s *Store) MaintainNotifications(ctx context.Context, _ time.Time, enabled bool, allowed []string, maxAttempts int, retention time.Duration, limit int) error {
	retentionMilliseconds := retention.Milliseconds()
	if limit < 1 || limit > 1000 || retentionMilliseconds < 1 {
		return errors.New("notification_maintenance_invalid")
	}
	// An empty but non-null SQL array is important for disabled maintenance.
	allowed = append([]string{}, allowed...)
	err := s.submitContext(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, maintainNotificationsSQL, enabled, maxAttempts, allowed, limit); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, pruneNotificationsSQL, retentionMilliseconds, limit); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, pruneObservedOutboundOriginsSQL, ObservedOutboundOriginRetention.Milliseconds(), limit); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, pruneUnobservedOutboundOriginsSQL, UnobservedOutboundOriginRetention.Milliseconds(), limit)
		return err
	})
	if err != nil {
		return notificationStoreError(ctx)
	}
	return nil
}
