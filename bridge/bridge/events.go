package bridge

import (
	"fmt"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// handleEvent is the central event dispatcher registered with whatsmeow.
func (c *Client) handleEvent(rawEvt interface{}) {
	switch evt := rawEvt.(type) {

	case *events.Connected:
		c.log.Info("whatsapp: connected")
		c.conn.handleConnected()

	case *events.Disconnected:
		c.log.Warn("whatsapp: disconnected (transient)")
		c.conn.handleDisconnected()

	case *events.LoggedOut:
		c.log.Warn("whatsapp: logged out", "reason", evt.Reason)
		c.conn.handleLoggedOut()

	case *events.StreamError:
		c.log.Warn("whatsapp: stream error", "code", evt.Code)
		c.conn.handleStreamError()

	case *events.ConnectFailure:
		// ConnectFailure carries reason codes covering bans, outdated clients,
		// logged-out sessions, and server-side errors. Log prominently and stop
		// reconnecting — whatsmeow already emits TemporaryBan / LoggedOut for
		// the common fatal sub-cases, but any unhandled ConnectFailure also
		// warrants a permanent stop to avoid hammering a rejected session.
		c.log.Error("whatsapp: connect failure",
			"reason", evt.Reason.String(),
			"message", evt.Message,
		)
		c.conn.handlePermanentDisconnect("connect failure: " + evt.Reason.String())

	case *events.TemporaryBan:
		// Account temporarily banned. The ban reason and remaining duration are
		// logged. Do not auto-reconnect: repeated attempts during a ban window
		// escalate the penalty.
		c.log.Error("whatsapp: temporary ban",
			"code", evt.Code.String(),
			"expire", evt.Expire,
		)
		c.conn.handlePermanentDisconnect("temporary ban: " + evt.Code.String())

	case *events.ClientOutdated:
		// WhatsApp rejected the connection because the client version is too
		// old. Reconnecting will not help until the whatsmeow dependency is
		// updated and the binary is redeployed.
		c.log.Error("whatsapp: client outdated — update whatsmeow dependency and redeploy")
		c.conn.handlePermanentDisconnect("client outdated")

	case *events.StreamReplaced:
		// Another device connected with the same session keys and took over the
		// stream. Auto-reconnecting would create a reconnect fight between the
		// two instances. Require a manual operator restart.
		c.log.Error("whatsapp: session replaced by another device — manual restart required")
		c.conn.handlePermanentDisconnect("stream replaced by another device")

	case *events.KeepAliveTimeout:
		c.keepAliveFailures++
		c.log.Warn("whatsapp: keep-alive timeout", "consecutive", c.keepAliveFailures)
		if c.keepAliveFailures >= 3 {
			c.log.Error("whatsapp: 3 consecutive keep-alive timeouts, forcing reconnect")
			c.keepAliveFailures = 0
			go func() {
				c.WA.Disconnect()
				// The resulting Disconnected event will fire and trigger scheduleReconnect.
			}()
		}

	case *events.KeepAliveRestored:
		c.keepAliveFailures = 0
		c.log.Info("whatsapp: keep-alive restored")

	case *events.Message:
		c.processMessage(evt)

	case *events.HistorySync:
		c.log.Info("whatsapp: history sync received",
			"conversations", len(evt.Data.GetConversations()))
		c.processHistorySync(evt)

	case *events.Contact:
		c.processContact(evt)

	case *events.PushNameSetting:
		c.log.Debug("push name setting received", "name", evt.Action.GetName())

	case *events.PushName:
		c.processPushName(evt)
	}
}

// processMessage stores a single incoming (or outgoing) message.
func (c *Client) processMessage(evt *events.Message) {
	chatJID := evt.Info.Chat.String()
	sender := evt.Info.Sender.String()
	senderName := evt.Info.PushName
	if senderName == "" {
		senderName = evt.Info.Sender.User
	}

	// Ensure the chat row exists.
	isGroup := evt.Info.Chat.Server == "g.us"
	chatName := c.resolveChatName(evt.Info.Chat, isGroup, senderName)
	if err := c.Store.UpsertChat(chatJID, chatName, isGroup, 0, evt.Info.Timestamp, ""); err != nil {
		c.log.Warn("failed to upsert chat", "jid", chatJID, "err", err)
	}

	// Increment unread counter for incoming messages.
	if !evt.Info.IsFromMe {
		if err := c.Store.IncrementUnread(chatJID); err != nil {
			c.log.Warn("failed to increment unread", "jid", chatJID, "err", err)
		}
	}

	content, quotedID, quotedParticipant := extractTextContent(evt.Message)
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(evt.Message)

	preview := content
	if content == "" && mediaType != "" {
		preview = fmt.Sprintf("[%s]", mediaType)
	}

	// Update last message preview.
	if err := c.Store.UpsertChat(chatJID, chatName, isGroup, 0, evt.Info.Timestamp, preview); err != nil {
		c.log.Warn("failed to update chat preview", "err", err)
	}

	msg := &MessageRow{
		ID:                evt.Info.ID,
		ChatJID:           chatJID,
		Sender:            sender,
		SenderName:        senderName,
		Content:           content,
		Timestamp:         evt.Info.Timestamp,
		IsFromMe:          evt.Info.IsFromMe,
		MediaType:         mediaType,
		Filename:          filename,
		URL:               url,
		MediaKey:          mediaKey,
		FileSHA256:        fileSHA256,
		FileEncSHA256:     fileEncSHA256,
		FileLength:        int64(fileLength),
		PushName:          evt.Info.PushName,
		QuotedMessageID:   quotedID,
		QuotedParticipant: quotedParticipant,
	}

	if err := c.Store.UpsertMessage(msg); err != nil {
		c.log.Warn("failed to store message", "id", evt.Info.ID, "err", err)
	} else {
		direction := "←"
		if evt.Info.IsFromMe {
			direction = "→"
		}
		// Log message type and chat at info level; content stays at debug only.
		msgKind := "text"
		if mediaType != "" {
			msgKind = mediaType
		}
		c.log.Info("message stored",
			"direction", direction,
			"chat", chatJID,
			"kind", msgKind,
		)
		c.log.Debug("message content",
			"direction", direction,
			"chat", chatJID,
			"sender", sender,
			"content", truncate(content, 80),
		)
	}
}

// processHistorySync iterates over synced conversations and stores messages.
func (c *Client) processHistorySync(evt *events.HistorySync) {
	stored := 0
	for _, conv := range evt.Data.GetConversations() {
		if conv.GetID() == "" {
			continue
		}
		chatJID := conv.GetID()
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			c.log.Warn("history sync: cannot parse jid", "jid", chatJID, "err", err)
			continue
		}

		isGroup := jid.Server == "g.us"
		chatName := c.resolveHistoryConvName(conv, jid, isGroup)

		// Determine the timestamp of the most-recent message for the chat row.
		var latestTime time.Time
		var latestPreview string

		for _, histMsg := range conv.GetMessages() {
			if histMsg.GetMessage() == nil {
				continue
			}
			wm := histMsg.GetMessage()
			ts := time.Unix(int64(wm.GetMessageTimestamp()), 0)

			sender := ""
			isFromMe := false
			if key := wm.GetKey(); key != nil {
				if key.GetFromMe() {
					isFromMe = true
					if c.WA.Store.ID != nil {
						sender = c.WA.Store.ID.User + "@s.whatsapp.net"
					}
				} else if key.GetParticipant() != "" {
					sender = key.GetParticipant()
				} else {
					sender = jid.String()
				}
			}

			content, quotedID, quotedParticipant := extractTextContent(wm.GetMessage())
			mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(wm.GetMessage())
			pushName := wm.GetPushName()

			msgID := ""
			if wm.GetKey() != nil {
				msgID = wm.GetKey().GetID()
			}

			msg := &MessageRow{
				ID:                msgID,
				ChatJID:           chatJID,
				Sender:            sender,
				SenderName:        pushName,
				Content:           content,
				Timestamp:         ts,
				IsFromMe:          isFromMe,
				MediaType:         mediaType,
				Filename:          filename,
				URL:               url,
				MediaKey:          mediaKey,
				FileSHA256:        fileSHA256,
				FileEncSHA256:     fileEncSHA256,
				FileLength:        int64(fileLength),
				PushName:          pushName,
				QuotedMessageID:   quotedID,
				QuotedParticipant: quotedParticipant,
			}

			if err := c.Store.UpsertMessage(msg); err != nil {
				c.log.Warn("history sync: failed to store message", "err", err)
				continue
			}
			stored++

			if ts.After(latestTime) {
				latestTime = ts
				latestPreview = content
				if content == "" && mediaType != "" {
					latestPreview = fmt.Sprintf("[%s]", mediaType)
				}
			}
		}

		if !latestTime.IsZero() {
			if err := c.Store.UpsertChat(chatJID, chatName, isGroup, 0, latestTime, latestPreview); err != nil {
				c.log.Warn("history sync: failed to upsert chat", "jid", chatJID, "err", err)
			}
		}
	}
	c.log.Info("history sync complete", "stored", stored)
}

// processContact stores a contact from a Contact event.
func (c *Client) processContact(evt *events.Contact) {
	jid := evt.JID.String()
	name := evt.Action.GetFullName()
	notify := evt.Action.GetFirstName()
	phone := evt.JID.User
	if err := c.Store.UpsertContact(jid, name, notify, phone); err != nil {
		c.log.Warn("failed to upsert contact", "jid", jid, "err", err)
	}
}

// processPushName stores/updates a contact entry from a push name event.
func (c *Client) processPushName(evt *events.PushName) {
	jid := evt.JID.String()
	name := evt.NewPushName
	phone := evt.JID.User
	if err := c.Store.UpsertContact(jid, name, name, phone); err != nil {
		c.log.Warn("failed to upsert push name contact", "jid", jid, "err", err)
	}
}

// resolveChatName resolves a display name for a chat, falling back through
// group info, contact store, and finally the bare JID user segment.
func (c *Client) resolveChatName(jid types.JID, isGroup bool, pushName string) string {
	if isGroup {
		gi, err := c.WA.GetGroupInfo(jid)
		if err == nil && gi.Name != "" {
			return gi.Name
		}
		return fmt.Sprintf("Group %s", jid.User)
	}

	contact, err := c.WA.Store.Contacts.GetContact(jid)
	if err == nil && contact.FullName != "" {
		return contact.FullName
	}
	if pushName != "" {
		return pushName
	}
	return jid.User
}

// resolveHistoryConvName extracts the best available name from a history sync
// conversation proto.
func (c *Client) resolveHistoryConvName(conv interface{ GetDisplayName() string }, jid types.JID, isGroup bool) string {
	if dn := conv.GetDisplayName(); dn != "" {
		return dn
	}
	return c.resolveChatName(jid, isGroup, "")
}

// ---- Proto extraction helpers ------------------------------------------

// extractTextContent returns the text body plus quoted message info from a
// waE2E.Message proto.
func extractTextContent(msg *waE2E.Message) (text, quotedID, quotedParticipant string) {
	if msg == nil {
		return
	}

	if t := msg.GetConversation(); t != "" {
		text = t
	} else if ext := msg.GetExtendedTextMessage(); ext != nil {
		text = ext.GetText()
		if ci := ext.GetContextInfo(); ci != nil {
			quotedID = ci.GetStanzaID()
			quotedParticipant = ci.GetParticipant()
		}
	}

	// Also capture quoted message context from non-text messages.
	if quotedID == "" {
		var ci interface{ GetStanzaID() string; GetParticipant() string }
		switch {
		case msg.GetImageMessage() != nil:
			ci = msg.GetImageMessage().GetContextInfo()
		case msg.GetVideoMessage() != nil:
			ci = msg.GetVideoMessage().GetContextInfo()
		case msg.GetAudioMessage() != nil:
			ci = msg.GetAudioMessage().GetContextInfo()
		case msg.GetDocumentMessage() != nil:
			ci = msg.GetDocumentMessage().GetContextInfo()
		}
		if ci != nil {
			quotedID = ci.GetStanzaID()
			quotedParticipant = ci.GetParticipant()
		}
	}

	return
}

// extractMediaInfo returns media metadata fields from a waE2E.Message proto.
func extractMediaInfo(msg *waE2E.Message) (
	mediaType, filename, url string,
	mediaKey, fileSHA256, fileEncSHA256 []byte,
	fileLength uint64,
) {
	if msg == nil {
		return
	}

	if img := msg.GetImageMessage(); img != nil {
		ext := mimeToExt(img.GetMimetype(), ".jpg")
		return "image", "image_" + nowStamp() + ext,
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	if vid := msg.GetVideoMessage(); vid != nil {
		ext := mimeToExt(vid.GetMimetype(), ".mp4")
		return "video", "video_" + nowStamp() + ext,
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + nowStamp() + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	if doc := msg.GetDocumentMessage(); doc != nil {
		fn := doc.GetFileName()
		if fn == "" {
			fn = "document_" + nowStamp()
		}
		return "document", fn,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return
}

func nowStamp() string {
	return time.Now().UTC().Format("20060102_150405")
}

func mimeToExt(mime, def string) string {
	switch {
	case strings.Contains(mime, "jpeg") || strings.Contains(mime, "jpg"):
		return ".jpg"
	case strings.Contains(mime, "png"):
		return ".png"
	case strings.Contains(mime, "gif"):
		return ".gif"
	case strings.Contains(mime, "webp"):
		return ".webp"
	case strings.Contains(mime, "mp4"):
		return ".mp4"
	case strings.Contains(mime, "quicktime"):
		return ".mov"
	default:
		return def
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
