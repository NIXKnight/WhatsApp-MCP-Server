package client

import (
	"context"
	"errors"
	"slices"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

type fakeOriginStore struct {
	err     error
	origins map[string]bool
	aliases []string
	order   *[]string
}

func (f *fakeOriginStore) RegisterOutboundOrigins(_ context.Context, messageID string, chatJIDs []string) error {
	*f.order = append(*f.order, "register")
	if f.err != nil {
		return f.err
	}
	f.aliases = append([]string(nil), chatJIDs...)
	if f.origins == nil {
		f.origins = make(map[string]bool)
	}
	for _, chatJID := range chatJIDs {
		f.origins[messageID+"\x00"+chatJID] = true
	}
	return nil
}

type fakeOutboundMessenger struct {
	ids      []types.MessageID
	sendErr  error
	sends    int
	sentID   types.MessageID
	sentChat types.JID
	order    *[]string
	onSend   func()
}

func (f *fakeOutboundMessenger) GenerateMessageID() types.MessageID {
	id := f.ids[0]
	f.ids = f.ids[1:]
	return id
}

func (f *fakeOutboundMessenger) SendMessage(_ context.Context, chat types.JID, _ *waE2E.Message, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
	*f.order = append(*f.order, "send")
	f.sends++
	f.sentChat = chat
	if len(extra) == 1 {
		f.sentID = extra[0].ID
	}
	if f.onSend != nil {
		f.onSend()
	}
	return whatsmeow.SendResponse{ID: f.sentID}, f.sendErr
}

func TestSendBridgeMessageRegistersPNAndLIDAliasesBeforeOriginalPNSend(t *testing.T) {
	order := []string{}
	origins := &fakeOriginStore{order: &order}
	messenger := &fakeOutboundMessenger{ids: []types.MessageID{"TEST_GENERATED_ID"}, order: &order}
	resolvedLID := types.NewJID("200", types.HiddenUserServer)
	requestedPN := types.NewJID("100", types.DefaultUserServer)
	messenger.onSend = func() {
		for _, chatJID := range []string{requestedPN.String(), resolvedLID.String()} {
			if !origins.origins["TEST_GENERATED_ID\x00"+chatJID] {
				t.Fatal("echo could race send before all aliases were registered")
			}
		}
	}
	bridge := &Client{
		origins: origins, outbound: messenger,
		resolveOutbound: func(context.Context, types.JID) (types.JID, error) {
			order = append(order, "resolve")
			return resolvedLID, nil
		},
	}

	response, err := bridge.SendBridgeMessage(context.Background(), requestedPN, &waE2E.Message{})
	if err != nil {
		t.Fatal("tracked send failed")
	}
	if !slices.Equal(order, []string{"resolve", "register", "send"}) {
		t.Fatal("external send did not follow atomic alias registration")
	}
	if !slices.Equal(origins.aliases, []string{requestedPN.String(), resolvedLID.String()}) {
		t.Fatal("PN and resolved LID aliases were not registered together")
	}
	if response.ID != "TEST_GENERATED_ID" || messenger.sentID != "TEST_GENERATED_ID" {
		t.Fatal("generated WhatsApp ID was not passed through SendRequestExtra")
	}
	if messenger.sentChat != requestedPN {
		t.Fatal("outbound destination was rewritten instead of preserving the PN")
	}
}

func TestSendBridgeMessageGroupRegistersOnceAndPreservesDestination(t *testing.T) {
	order := []string{}
	origins := &fakeOriginStore{order: &order}
	messenger := &fakeOutboundMessenger{ids: []types.MessageID{"TEST_GENERATED_ID"}, order: &order}
	group := types.NewJID("100", types.GroupServer)
	bridge := &Client{
		origins: origins, outbound: messenger,
		resolveOutbound: func(context.Context, types.JID) (types.JID, error) {
			t.Fatal("non-PN send attempted alias resolution")
			return types.EmptyJID, nil
		},
	}

	if _, err := bridge.SendBridgeMessage(context.Background(), group, &waE2E.Message{}); err != nil {
		t.Fatal("group send failed")
	}
	if !slices.Equal(origins.aliases, []string{group.String()}) || messenger.sentChat != group {
		t.Fatal("group identity changed")
	}
}

func TestSendBridgeMessageResolutionFailurePreventsRegistrationAndSend(t *testing.T) {
	order := []string{}
	origins := &fakeOriginStore{order: &order}
	messenger := &fakeOutboundMessenger{ids: []types.MessageID{"TEST_GENERATED_ID"}, order: &order}
	bridge := &Client{
		origins: origins, outbound: messenger,
		resolveOutbound: func(context.Context, types.JID) (types.JID, error) {
			return types.EmptyJID, errors.New("outbound_chat_resolution_failed")
		},
	}

	if _, err := bridge.SendBridgeMessage(context.Background(), types.NewJID("100", types.DefaultUserServer), &waE2E.Message{}); err == nil {
		t.Fatal("resolution failure did not fail the request")
	}
	if messenger.sends != 0 || len(order) != 0 {
		t.Fatal("registration or send occurred without a required LID alias")
	}
}

func TestSendBridgeMessageRegistrationFailurePreventsSend(t *testing.T) {
	order := []string{}
	origins := &fakeOriginStore{err: errors.New("outbound_origin_store_failed"), order: &order}
	messenger := &fakeOutboundMessenger{ids: []types.MessageID{"TEST_GENERATED_ID"}, order: &order}
	bridge := &Client{
		origins: origins, outbound: messenger,
		resolveOutbound: func(_ context.Context, chat types.JID) (types.JID, error) { return chat, nil },
	}

	if _, err := bridge.SendBridgeMessage(context.Background(), types.NewJID("100", types.GroupServer), &waE2E.Message{}); err == nil {
		t.Fatal("registration failure did not fail the request")
	}
	if messenger.sends != 0 || !slices.Equal(order, []string{"register"}) {
		t.Fatal("external send occurred after registration failure")
	}
}

func TestSendBridgeMessageRetainsMarkersOnUncertainFailureAndUsesFreshRetryID(t *testing.T) {
	order := []string{}
	origins := &fakeOriginStore{order: &order}
	messenger := &fakeOutboundMessenger{
		ids:     []types.MessageID{"TEST_FIRST_ID", "TEST_RETRY_ID"},
		sendErr: errors.New("TEST_UNCERTAIN_SEND_FAILURE"),
		order:   &order,
	}
	bridge := &Client{
		origins: origins, outbound: messenger,
		resolveOutbound: func(_ context.Context, chat types.JID) (types.JID, error) { return chat, nil },
	}
	chat := types.NewJID("100", types.GroupServer)

	if _, err := bridge.SendBridgeMessage(context.Background(), chat, &waE2E.Message{}); err == nil {
		t.Fatal("uncertain send failure was hidden")
	}
	if !origins.origins["TEST_FIRST_ID\x00100@g.us"] {
		t.Fatal("uncertain send removed its suppression marker")
	}
	if _, err := bridge.SendBridgeMessage(context.Background(), chat, &waE2E.Message{}); err == nil {
		t.Fatal("retry send failure was hidden")
	}
	if !origins.origins["TEST_RETRY_ID\x00100@g.us"] || messenger.sentID != "TEST_RETRY_ID" {
		t.Fatal("retry reused the uncertain message ID")
	}
}
