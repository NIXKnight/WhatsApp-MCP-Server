package client

import (
	"regexp"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

var notificationChatJID = regexp.MustCompile(`^\d+@(s\.whatsapp\.net|g\.us|lid)$`)

type notificationPolicy struct {
	allowed map[string]bool
	maxAge  time.Duration
}

// Option configures the bridge client without granting any action capability.
type Option func(*Client)

// WithIncomingNotifications enables metadata-only notifications for exact
// allowlisted chats. Empty or invalid policy fails closed (no notifications).
func WithIncomingNotifications(allowed []string, maxAge time.Duration) Option {
	disable := func(c *Client) { c.notifications = notificationPolicy{} }
	if maxAge <= 0 || maxAge > 7*24*time.Hour {
		return disable
	}
	policy := notificationPolicy{allowed: make(map[string]bool), maxAge: maxAge}
	for _, jid := range allowed {
		if !notificationChatJID.MatchString(jid) {
			return disable
		}
		policy.allowed[jid] = true
	}
	return func(c *Client) { c.notifications = policy }
}

func (p notificationPolicy) candidate(evt *events.Message, receivedAt time.Time) *store.NotificationRequest {
	if evt == nil || evt.Info.IsFromMe || evt.IsEdit || evt.Info.Edit != types.EditAttributeEmpty ||
		evt.SourceWebMsg != nil || evt.UnavailableRequestID != "" || evt.Message == nil ||
		evt.Info.ID == "" || evt.Info.Timestamp.IsZero() || receivedAt.IsZero() ||
		p.maxAge <= 0 || p.maxAge > 7*24*time.Hour || !p.allowed[evt.Info.Chat.String()] {
		return nil
	}
	return &store.NotificationRequest{ReceivedAt: receivedAt, MaxAge: p.maxAge}
}
