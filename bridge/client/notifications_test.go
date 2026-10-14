package client

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func notificationEvent(now time.Time) *events.Message {
	return &events.Message{
		Message: &waE2E.Message{},
		Info: types.MessageInfo{
			ID:        "TEST_M1",
			Timestamp: now,
			MessageSource: types.MessageSource{
				Chat:   types.NewJID("100", types.GroupServer),
				Sender: types.NewJID("500", types.DefaultUserServer),
			},
		},
	}
}

func TestIncomingNotificationPolicy(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := &Client{}
	WithIncomingNotifications([]string{"100@g.us", "300@lid", "400@s.whatsapp.net"}, time.Hour)(c)
	tests := []struct {
		name      string
		change    func(*events.Message)
		eligible  bool
		kind      string
		canonical string
		sender    string
		alt       string
	}{
		{"group incoming", func(*events.Message) {}, true, "group", "100@g.us", "500@s.whatsapp.net", ""},
		{"group first sender retry", func(e *events.Message) { e.RetryCount = 2 }, true, "group", "100@g.us", "500@s.whatsapp.net", ""},
		{"group own message", func(e *events.Message) { e.Info.IsFromMe = true }, true, "group", "100@g.us", "500@s.whatsapp.net", ""},
		{"group bot participant", func(e *events.Message) { e.Info.Sender = types.NewJID("501", types.BotServer) }, true, "group", "100@g.us", "501@bot", ""},
		{"group hosted participant", func(e *events.Message) { e.Info.Sender = types.JID{User: "502", Device: 7, Server: types.HostedServer} }, true, "group", "100@g.us", "502@hosted", ""},
		{"group device identities normalized", func(e *events.Message) {
			e.Info.Sender = types.JID{User: "500", Device: 7, Server: types.DefaultUserServer}
			e.Info.SenderAlt = types.JID{User: "5000", Device: 8, Server: types.HiddenUserServer}
		}, true, "group", "100@g.us", "500@s.whatsapp.net", "5000@lid"},
		{"operator incoming PN alias", func(e *events.Message) {
			e.Info.Chat = types.NewJID("400", types.DefaultUserServer)
			e.Info.Sender = types.JID{User: "400", Device: 7, Server: types.DefaultUserServer}
			e.Info.SenderAlt = types.JID{User: "300", Device: 8, Server: types.HiddenUserServer}
		}, true, "operator_private", "300@lid", "400@s.whatsapp.net", "300@lid"},
		{"operator incoming LID alias", func(e *events.Message) {
			e.Info.Chat = types.NewJID("300", types.HiddenUserServer)
			e.Info.Sender = types.NewJID("300", types.HiddenUserServer)
		}, true, "operator_private", "300@lid", "300@lid", ""},
		{"operator own message with recipient alias", func(e *events.Message) {
			e.Info.Chat = types.NewJID("400", types.DefaultUserServer)
			e.Info.Sender = types.JID{User: "900", Device: 9, Server: types.DefaultUserServer}
			e.Info.RecipientAlt = types.JID{User: "300", Device: 8, Server: types.HiddenUserServer}
			e.Info.IsFromMe = true
		}, true, "operator_private", "300@lid", "900@s.whatsapp.net", ""},
		{"edit wrapper", func(e *events.Message) { e.IsEdit = true }, false, "", "", "", ""},
		{"edit attribute", func(e *events.Message) { e.Info.Edit = types.EditAttributeMessageEdit }, false, "", "", "", ""},
		{"pin", func(e *events.Message) { e.Info.Edit = types.EditAttributePinInChat }, false, "", "", "", ""},
		{"revoke", func(e *events.Message) { e.Info.Edit = types.EditAttributeSenderRevoke }, false, "", "", "", ""},
		{"history origin", func(e *events.Message) { e.SourceWebMsg = &waWeb.WebMessageInfo{} }, false, "", "", "", ""},
		{"recovery origin", func(e *events.Message) { e.UnavailableRequestID = "TEST_RECOVERY" }, false, "", "", "", ""},
		{"no message", func(e *events.Message) { e.Message = nil }, false, "", "", "", ""},
		{"disallowed group", func(e *events.Message) { e.Info.Chat = types.NewJID("200", types.GroupServer) }, false, "", "", "", ""},
		{"unauthorized direct despite operator alternate", func(e *events.Message) {
			e.Info.Chat = types.NewJID("600", types.DefaultUserServer)
			e.Info.Sender = types.NewJID("600", types.DefaultUserServer)
			e.Info.SenderAlt = types.NewJID("300", types.HiddenUserServer)
		}, false, "", "", "", ""},
		{"operator chat with unauthorized inbound sender", func(e *events.Message) {
			e.Info.Chat = types.NewJID("400", types.DefaultUserServer)
			e.Info.Sender = types.NewJID("600", types.DefaultUserServer)
		}, false, "", "", "", ""},
		{"operator chat with unauthorized sender alternate", func(e *events.Message) {
			e.Info.Chat = types.NewJID("400", types.DefaultUserServer)
			e.Info.Sender = types.NewJID("400", types.DefaultUserServer)
			e.Info.SenderAlt = types.NewJID("600", types.HiddenUserServer)
		}, false, "", "", "", ""},
		{"operator own message with unauthorized recipient alternate", func(e *events.Message) {
			e.Info.Chat = types.NewJID("400", types.DefaultUserServer)
			e.Info.Sender = types.NewJID("900", types.DefaultUserServer)
			e.Info.RecipientAlt = types.NewJID("600", types.HiddenUserServer)
			e.Info.IsFromMe = true
		}, false, "", "", "", ""},
		{"unsupported sender namespace", func(e *events.Message) { e.Info.Sender = types.NewJID("500", types.BroadcastServer) }, false, "", "", "", ""},
		{"no identifier", func(e *events.Message) { e.Info.ID = "" }, false, "", "", "", ""},
		{"no event time", func(e *events.Message) { e.Info.Timestamp = time.Time{} }, false, "", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := notificationEvent(now)
			tt.change(e)
			candidate := c.notifications.candidate(e, now)
			if (candidate != nil) != tt.eligible {
				t.Fatal("unexpected eligibility")
			}
			if candidate != nil && (candidate.ReceivedAt != now || candidate.MaxAge != time.Hour || candidate.ChatKind != tt.kind || candidate.CanonicalChatJID != tt.canonical || candidate.SenderJID != tt.sender || candidate.SenderAltJID != tt.alt || candidate.IsFromMe != e.Info.IsFromMe) {
				t.Fatal("notification metadata changed")
			}
		})
	}
	if c.notifications.candidate(nil, now) != nil {
		t.Fatal("nil event eligible")
	}
}

func TestNotificationPolicyFailsClosed(t *testing.T) {
	c := &Client{}
	valid := []string{"100@g.us", "300@lid", "400@s.whatsapp.net"}
	WithIncomingNotifications(valid, time.Hour)(c)
	for _, allowed := range [][]string{{"*"}, {"100@g.us"}, {"300@lid"}, {"100@g.us", "300@hosted"}} {
		WithIncomingNotifications(allowed, time.Hour)(c)
		if len(c.notifications.groups) != 0 || len(c.notifications.operators) != 0 {
			t.Fatal("invalid policy admitted")
		}
		WithIncomingNotifications(valid, time.Hour)(c)
	}
	WithIncomingNotifications(valid, 0)(c)
	if len(c.notifications.groups) != 0 || len(c.notifications.operators) != 0 {
		t.Fatal("invalid age preserved prior policy")
	}
	if (notificationPolicy{}).candidate(&events.Message{}, time.Now()) != nil {
		t.Fatal("disabled policy admitted")
	}
}
