package client

import (
	"context"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// React sends an emoji reaction to a target message in the given chat. An empty
// emoji removes a previously sent reaction. sender is the author of the target
// message; pass types.EmptyJID when reacting to one's own message. It returns
// the whatsmeow send response for the reaction message itself.
func (c *Client) React(ctx context.Context, chat, sender types.JID, targetID types.MessageID, emoji string) (whatsmeow.SendResponse, error) {
	msg := c.WA.BuildReaction(chat, sender, targetID, emoji)
	return c.WA.SendMessage(ctx, chat, msg)
}

// Edit edits a previously sent message in the given chat, replacing its body
// with newText. Editing is only valid for messages sent by this account. It
// returns the whatsmeow send response for the edit message.
func (c *Client) Edit(ctx context.Context, chat types.JID, targetID types.MessageID, newText string) (whatsmeow.SendResponse, error) {
	newContent := &waE2E.Message{
		Conversation: proto.String(newText),
	}
	msg := c.WA.BuildEdit(chat, targetID, newContent)
	return c.WA.SendMessage(ctx, chat, msg)
}

// Revoke revokes (deletes for everyone) a target message in the given chat.
// sender is the author of the target message; pass types.EmptyJID to revoke
// one's own message. Group admins may revoke other members' messages by
// supplying the original sender. It returns the whatsmeow send response for the
// revoke message.
func (c *Client) Revoke(ctx context.Context, chat, sender types.JID, targetID types.MessageID) (whatsmeow.SendResponse, error) {
	msg := c.WA.BuildRevoke(chat, sender, targetID)
	return c.WA.SendMessage(ctx, chat, msg)
}
