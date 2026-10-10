package client

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func TestIncomingNotificationPolicy(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := &Client{}
	WithIncomingNotifications([]string{"100@g.us"}, time.Hour)(c)
	for _, tt := range []struct {
		name     string
		change   func(*events.Message)
		eligible bool
	}{
		{"incoming", func(*events.Message) {}, true},
		{"first sender retry", func(e *events.Message) { e.RetryCount = 2 }, true},
		{"self", func(e *events.Message) { e.Info.IsFromMe = true }, false},
		{"edit wrapper", func(e *events.Message) { e.IsEdit = true }, false},
		{"edit attribute", func(e *events.Message) { e.Info.Edit = types.EditAttributeMessageEdit }, false},
		{"pin", func(e *events.Message) { e.Info.Edit = types.EditAttributePinInChat }, false},
		{"revoke", func(e *events.Message) { e.Info.Edit = types.EditAttributeSenderRevoke }, false},
		{"history origin", func(e *events.Message) { e.SourceWebMsg = &waWeb.WebMessageInfo{} }, false},
		{"recovery origin", func(e *events.Message) { e.UnavailableRequestID = "TEST_RECOVERY" }, false},
		{"no message", func(e *events.Message) { e.Message = nil }, false},
		{"disallowed", func(e *events.Message) { e.Info.Chat = types.NewJID("200", "g.us") }, false},
		{"no identifier", func(e *events.Message) { e.Info.ID = "" }, false},
		{"no event time", func(e *events.Message) { e.Info.Timestamp = time.Time{} }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := &events.Message{Message: &waE2E.Message{}, Info: types.MessageInfo{ID: "TEST_M1", Timestamp: now, MessageSource: types.MessageSource{Chat: types.NewJID("100", "g.us")}}}
			tt.change(e)
			candidate := c.notifications.candidate(e, now)
			if (candidate != nil) != tt.eligible {
				t.Fatal("unexpected eligibility")
			}
			if candidate != nil && (candidate.ReceivedAt != now || candidate.MaxAge != time.Hour) {
				t.Fatal("receipt policy changed")
			}
		})
	}
	if c.notifications.candidate(nil, now) != nil {
		t.Fatal("nil event eligible")
	}
}

func TestNotificationPolicyFailsClosed(t *testing.T) {
	c := &Client{}
	WithIncomingNotifications([]string{"100@g.us"}, time.Hour)(c)
	WithIncomingNotifications([]string{"*"}, time.Hour)(c)
	if len(c.notifications.allowed) != 0 {
		t.Fatal("invalid policy admitted")
	}
	WithIncomingNotifications([]string{"100@g.us"}, time.Hour)(c)
	WithIncomingNotifications([]string{"100@g.us"}, 0)(c)
	if len(c.notifications.allowed) != 0 {
		t.Fatal("invalid age preserved prior policy")
	}
	if (notificationPolicy{}).candidate(&events.Message{}, time.Now()) != nil {
		t.Fatal("disabled policy admitted")
	}
}
