package client

import (
	"context"
	"errors"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type outboundOriginStore interface {
	RegisterOutboundOrigins(context.Context, string, []string) error
}

type outboundMessenger interface {
	GenerateMessageID() types.MessageID
	SendMessage(context.Context, types.JID, *waE2E.Message, ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error)
}

// SendBridgeMessage pre-generates a WhatsApp message ID, atomically registers
// every exact plausible event chat identity, and only then performs the
// external send. The original requested destination is passed to whatsmeow so
// its pinned PN send branch remains authoritative. Send failures intentionally
// retain all markers because the external outcome is uncertain.
func (c *Client) SendBridgeMessage(ctx context.Context, chat types.JID, msg *waE2E.Message) (whatsmeow.SendResponse, error) {
	if c == nil || c.origins == nil || c.outbound == nil || c.resolveOutbound == nil {
		return whatsmeow.SendResponse{}, errors.New("outbound_send_unavailable")
	}
	messageID := c.outbound.GenerateMessageID()
	if messageID == "" {
		return whatsmeow.SendResponse{}, errors.New("outbound_message_id_invalid")
	}
	aliases := []string{chat.String()}
	if chat.Server == types.DefaultUserServer {
		lid, err := c.resolveOutbound(ctx, chat)
		if err != nil {
			return whatsmeow.SendResponse{}, err
		}
		aliases = append(aliases, lid.String())
	}
	if err := c.origins.RegisterOutboundOrigins(ctx, string(messageID), aliases); err != nil {
		return whatsmeow.SendResponse{}, err
	}
	return c.outbound.SendMessage(ctx, chat, msg, whatsmeow.SendRequestExtra{ID: messageID})
}

// resolveOutboundChat resolves the required LID alias for a PN destination.
// It does not choose the outbound destination: SendBridgeMessage preserves the
// original PN and lets pinned whatsmeow execute its normal PN send branch.
func (c *Client) resolveOutboundChat(ctx context.Context, chat types.JID) (types.JID, error) {
	if chat.Server != types.DefaultUserServer {
		return chat, nil
	}
	lid, err := c.WA.Store.LIDs.GetLIDForPN(ctx, chat)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return types.EmptyJID, ctxErr
		}
		return types.EmptyJID, errors.New("outbound_chat_resolution_failed")
	}
	if !lid.IsEmpty() {
		return lid, nil
	}
	info, err := c.WA.GetUserInfo(ctx, []types.JID{chat})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return types.EmptyJID, ctxErr
		}
		return types.EmptyJID, errors.New("outbound_chat_resolution_failed")
	}
	lid = info[chat].LID
	if lid.IsEmpty() {
		return types.EmptyJID, errors.New("outbound_chat_resolution_failed")
	}
	return lid, nil
}

// React sends an emoji reaction to a target message in the given chat. An empty
// emoji removes a previously sent reaction. sender is the author of the target
// message; pass types.EmptyJID when reacting to one's own message. Reaction
// events have no persistable text/media body, so they cannot create message
// notifications and do not need bridge-origin tracking. It returns the
// whatsmeow send response for the reaction message itself.
func (c *Client) React(ctx context.Context, chat, sender types.JID, targetID types.MessageID, emoji string) (whatsmeow.SendResponse, error) {
	msg := c.WA.BuildReaction(chat, sender, targetID, emoji)
	return c.WA.SendMessage(ctx, chat, msg)
}

// Edit edits a previously sent message in the given chat, replacing its body
// with newText. Editing is only valid for messages sent by this account. Edit
// events are explicitly excluded by notification admission, so they do not need
// bridge-origin tracking. It returns the whatsmeow send response for the edit.
func (c *Client) Edit(ctx context.Context, chat types.JID, targetID types.MessageID, newText string) (whatsmeow.SendResponse, error) {
	newContent := &waE2E.Message{
		Conversation: proto.String(newText),
	}
	msg := c.WA.BuildEdit(chat, targetID, newContent)
	return c.WA.SendMessage(ctx, chat, msg)
}

// Revoke revokes (deletes for everyone) a target message in the given chat.
// sender is the author of the target message; pass types.EmptyJID to revoke
// one's own message. Revoke events are explicitly excluded by notification
// admission, so they do not need bridge-origin tracking. Group admins may
// revoke other members' messages by supplying the original sender. It returns
// the whatsmeow send response for the revoke message.
func (c *Client) Revoke(ctx context.Context, chat, sender types.JID, targetID types.MessageID) (whatsmeow.SendResponse, error) {
	msg := c.WA.BuildRevoke(chat, sender, targetID)
	return c.WA.SendMessage(ctx, chat, msg)
}
