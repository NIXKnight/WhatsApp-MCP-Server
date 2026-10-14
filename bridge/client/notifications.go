package client

import (
	"regexp"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

var notificationGroupJID = regexp.MustCompile(`^\d+@g\.us$`)
var notificationOperatorJID = regexp.MustCompile(`^\d+@(s\.whatsapp\.net|lid)$`)
var notificationParticipantJID = regexp.MustCompile(`^\d+@(s\.whatsapp\.net|lid|bot|hosted|hosted\.lid)$`)

type notificationPolicy struct {
	groups            map[string]bool
	operators         map[string]bool
	canonicalOperator string
	maxAge            time.Duration
}

// Option configures the bridge client without granting any action capability.
type Option func(*Client)

// WithIncomingNotifications enables metadata-only notifications for exact
// group and operator chat identities. The first configured operator JID is the
// canonical private stream/session identity. Empty or invalid policy fails
// closed (no notifications).
func WithIncomingNotifications(allowed []string, maxAge time.Duration) Option {
	disable := func(c *Client) { c.notifications = notificationPolicy{} }
	if maxAge <= 0 || maxAge > 7*24*time.Hour {
		return disable
	}
	policy := notificationPolicy{
		groups:    make(map[string]bool),
		operators: make(map[string]bool),
		maxAge:    maxAge,
	}
	for _, jid := range allowed {
		switch {
		case notificationGroupJID.MatchString(jid):
			policy.groups[jid] = true
		case notificationOperatorJID.MatchString(jid):
			policy.operators[jid] = true
			if policy.canonicalOperator == "" {
				policy.canonicalOperator = jid
			}
		default:
			return disable
		}
	}
	if len(policy.groups) == 0 || len(policy.operators) == 0 {
		return disable
	}
	return func(c *Client) { c.notifications = policy }
}

func normalizedNotificationJID(jid types.JID) string {
	if jid.IsEmpty() {
		return ""
	}
	return jid.ToNonAD().String()
}

func (p notificationPolicy) candidate(evt *events.Message, receivedAt time.Time) *store.NotificationRequest {
	if evt == nil || evt.IsEdit || evt.Info.Edit != types.EditAttributeEmpty ||
		evt.SourceWebMsg != nil || evt.UnavailableRequestID != "" || evt.Message == nil ||
		evt.Info.ID == "" || evt.Info.Timestamp.IsZero() || receivedAt.IsZero() ||
		p.maxAge <= 0 || p.maxAge > 7*24*time.Hour {
		return nil
	}

	actualChatJID := evt.Info.Chat.String()
	senderJID := normalizedNotificationJID(evt.Info.Sender)
	senderAltJID := normalizedNotificationJID(evt.Info.SenderAlt)
	if !notificationParticipantJID.MatchString(senderJID) ||
		(senderAltJID != "" && !notificationParticipantJID.MatchString(senderAltJID)) {
		return nil
	}

	request := &store.NotificationRequest{
		ReceivedAt:   receivedAt,
		MaxAge:       p.maxAge,
		SenderJID:    senderJID,
		SenderAltJID: senderAltJID,
		IsFromMe:     evt.Info.IsFromMe,
	}
	if p.groups[actualChatJID] {
		request.CanonicalChatJID = actualChatJID
		request.ChatKind = "group"
		return request
	}
	if !p.operators[actualChatJID] {
		return nil
	}

	if evt.Info.IsFromMe {
		recipientAltJID := normalizedNotificationJID(evt.Info.RecipientAlt)
		if recipientAltJID != "" && !p.operators[recipientAltJID] {
			return nil
		}
	} else if !p.operators[senderJID] || (senderAltJID != "" && !p.operators[senderAltJID]) {
		return nil
	}
	request.CanonicalChatJID = p.canonicalOperator
	request.ChatKind = "operator_private"
	return request
}
