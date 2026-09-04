"""Message-related MCP tools (L4 tool/contract).

Provides ten tools:

* :func:`send_message` — Send a plain-text message to an individual contact.
* :func:`check_new_messages` — Poll for new messages since a given Unix timestamp.
* :func:`get_messages` — Retrieve recent messages from a specific chat.
* :func:`get_unread_chats` — List all chats that have unread messages.
* :func:`get_unread_messages` — Flat list of all unread messages across all chats.
* :func:`get_group_context` — Compact recent-turn window for a group chat.
* :func:`send_reaction` — React to a message with an emoji (or remove a reaction).
* :func:`edit_message` — Edit a previously sent message's text.
* :func:`revoke_message` — Revoke (delete for everyone) a message.
* :func:`check_triggers` — Batch trigger check across multiple chats.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from fastmcp import Context, FastMCP

if TYPE_CHECKING:
    from whatsapp_mcp.bridge_client import BridgeClient

logger = logging.getLogger(__name__)


def register_message_tools(mcp: FastMCP) -> None:
    """Register all message tools onto *mcp*.

    Args:
        mcp: The :class:`~fastmcp.FastMCP` server instance.
    """

    # ------------------------------------------------------------------
    # send_message
    # ------------------------------------------------------------------

    @mcp.tool
    async def send_message(
        ctx: Context,
        to: str,
        text: str,
        quoted_message_id: str | None = None,
        quoted_participant: str | None = None,
        mentions: list[str] | None = None,
    ) -> str:
        """Send a plain-text WhatsApp message to an individual contact.

        The ``to`` field must be a phone number including country code
        (e.g. ``923001234567``) or a full individual JID
        (e.g. ``923001234567@s.whatsapp.net``).

        Optionally quote or reply to an existing message by providing its ID
        in ``quoted_message_id``.  Supply ``quoted_participant`` when the
        quoted message sender is known but the message may not be in the bridge
        store.

        Args:
            ctx: FastMCP context — provides ``ctx.lifespan_context["bridge"]``.
            to: Recipient phone number with country code (e.g. ``923001234567``)
                or individual JID (e.g. ``923001234567@s.whatsapp.net``).
            text: Message body to send.  Must be non-empty.
            quoted_message_id: Optional message ID to reply or quote.
            quoted_participant: Optional JID (phone or LID) of the quoted
                message sender.  Required for quoting when the message is no
                longer in the bridge store.
            mentions: Optional list of JIDs to tag in the message. Use
                phone JIDs (e.g. ``["923224387030@s.whatsapp.net"]``).
                The message text should contain matching ``@phone``
                placeholders (e.g. ``@923224387030``) for WhatsApp to
                render them as display names.

        Returns:
            JSON string with ``{"id": "...", "timestamp": "..."}`` on
            success, or raises :class:`RuntimeError` on bridge error.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]

        payload: dict = {"to": to, "text": text}
        if quoted_message_id:
            payload["quotedMessageId"] = quoted_message_id
        if quoted_participant:
            payload["quotedParticipant"] = quoted_participant
        if mentions:
            payload["mentions"] = mentions

        result = await bridge.post("/api/send", json=payload)
        return json.dumps(result)

    # ------------------------------------------------------------------
    # check_new_messages
    # ------------------------------------------------------------------

    @mcp.tool
    async def check_new_messages(
        ctx: Context,
        since: int,
        limit: int = 100,
        chat_jid: str | None = None,
    ) -> str:
        """Poll for new messages since an explicit Unix millisecond timestamp.

        The caller is responsible for persisting the watermark and advancing it
        on each successful call.  Use the ``timestamp`` field from the response
        as the next ``since`` value, or the current wall-clock time in
        milliseconds when no messages are returned.

        Pass ``since=0`` on the very first call to receive recent history.

        Args:
            ctx: FastMCP context.
            since: Unix timestamp in **milliseconds**.  Only messages stored
                after this time are returned.  Pass ``0`` to retrieve all
                recent stored messages.  Example: ``1700000000000``.
            limit: Maximum number of messages to return (1-500, default 100).
            chat_jid: Optional JID to filter results to a single chat.

        Returns:
            JSON string with ``{"count": N, "messages": [...], "since": "...",
            "timestamp": "..."}`` where each message has ``id``,
            ``chat_jid``, ``sender``, ``sender_name``, ``content``,
            ``timestamp``, ``is_from_me``, ``push_name``.
            When nothing is new: ``"No new messages."``.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]

        result = await bridge.get("/api/check", since=since, limit=limit, chat_jid=chat_jid)

        messages = result.get("messages") if isinstance(result, dict) else []
        if not messages:
            return "No new messages."

        return json.dumps(result)

    # ------------------------------------------------------------------
    # get_messages
    # ------------------------------------------------------------------

    @mcp.tool
    async def get_messages(
        ctx: Context,
        jid: str,
        limit: int = 20,
    ) -> str:
        """Retrieve the most recent messages from a WhatsApp chat.

        Returns messages in reverse-chronological order (newest first).
        Works for both individual chats and group chats.

        Args:
            ctx: FastMCP context.
            jid: Full WhatsApp JID of the chat.  Individual contacts use the
                form ``923001234567@s.whatsapp.net``; groups use
                ``120363039783372408@g.us``.
            limit: Maximum number of messages to return.  Defaults to 20.
                Capped by the bridge at 500.

        Returns:
            JSON string containing a list of message objects, each with fields:
            ``id``, ``chat_jid``, ``sender``, ``sender_name``, ``content``,
            ``timestamp``, ``is_from_me``, ``media_type``, ``push_name``,
            ``quoted_message_id``, ``quoted_participant``.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get("/api/messages", chat_jid=jid, limit=limit)
        return json.dumps(result)

    # ------------------------------------------------------------------
    # get_unread_chats
    # ------------------------------------------------------------------

    @mcp.tool
    async def get_unread_chats(
        ctx: Context,
        message_limit: int = 5,
    ) -> str:
        """Return all chats that have unread messages, with recent message previews.

        For each unread chat the response includes the chat JID, name (when
        available), whether it is a group, the unread message count, and the
        most recent unread messages up to ``message_limit``.

        Use this tool to discover what new conversations need attention across
        all contacts and groups at once.

        Args:
            ctx: FastMCP context.
            message_limit: Maximum number of recent messages to include per
                unread chat.  Defaults to 5.

        Returns:
            JSON string.  When unread chats exist:
            ``{"unreadChats": N, "chats": [...]}`` where each chat entry
            contains ``jid``, ``name``, ``isGroup``, ``unreadCount``, and
            ``messages``.
            When nothing is unread: ``"No unread chats."``.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get("/api/unread", msg_limit=message_limit)

        chats = result.get("chats") if isinstance(result, dict) else []
        if not chats:
            return "No unread chats."

        return json.dumps(result)

    # ------------------------------------------------------------------
    # get_unread_messages
    # ------------------------------------------------------------------

    @mcp.tool
    async def get_unread_messages(ctx: Context) -> str:
        """Return a flat list of all unread messages across all chats.

        Messages are sorted by timestamp, newest first.  Each entry includes
        the chat JID, chat name, whether it is a group, message ID, sender
        JID, sender display name, message content, and timestamp (Unix ms).

        Use this for a quick full-inbox overview when you need to see
        everything unread without drilling into individual chats first.

        Args:
            ctx: FastMCP context.

        Returns:
            JSON string.  When unread messages exist:
            ``{"totalUnread": N, "messages": [...]}`` where each message has
            ``chatJid``, ``chatName``, ``isGroup``, ``messageId``,
            ``participant``, ``senderName``, ``text``, ``timestamp``.
            When nothing is unread: ``"No unread messages."``.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        # flat=true instructs the bridge to return an UnreadFlatResponse
        # directly — avoids manual flattening and ensures consistent field names.
        result = await bridge.get("/api/unread", flat="true")

        messages = result.get("messages") if isinstance(result, dict) else []
        if not messages:
            return "No unread messages."

        return json.dumps(result)

    # ------------------------------------------------------------------
    # get_group_context
    # ------------------------------------------------------------------

    @mcp.tool
    async def get_group_context(
        ctx: Context,
        jid: str,
        limit: int = 20,
    ) -> str:
        """Return a compact recent-turn window for a group chat.

        Strips own messages and empty content. Returns only fields needed
        for context assembly in chronological order (oldest first).

        Args:
            ctx: FastMCP context.
            jid: Group JID ending with @g.us.
            limit: Maximum messages (default 20).

        Returns:
            JSON list: [{"name", "sender", "text", "ts"}] in chronological order.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get("/api/messages", chat_jid=jid, limit=limit)
        messages = result if isinstance(result, list) else []

        chronological = []
        for m in reversed(messages):
            text = (m.get("content") or "").strip()
            if not text:
                continue
            chronological.append({
                "name": m.get("sender_name") or m.get("push_name") or m.get("sender", ""),
                "sender": m.get("sender", ""),
                "text": text,
                "ts": m["timestamp"],
            })

        return json.dumps(chronological)

    # ------------------------------------------------------------------
    # send_reaction
    # ------------------------------------------------------------------

    @mcp.tool
    async def send_reaction(
        ctx: Context,
        chat_jid: str,
        message_id: str,
        emoji: str,
        sender: str | None = None,
    ) -> str:
        """React to a WhatsApp message with an emoji.

        Sends a reaction to an existing message.  Pass an empty ``emoji`` string
        to remove a reaction previously sent to the same message.

        Args:
            ctx: FastMCP context.
            chat_jid: JID of the chat containing the target message, e.g.
                ``120363039783372408@g.us`` or ``923001234567@s.whatsapp.net``.
            message_id: ID of the message being reacted to.  Obtain it from
                :func:`get_messages`, :func:`check_new_messages`, or
                :func:`get_unread_messages`.
            emoji: Reaction emoji (e.g. ``👍``).  An empty string removes a
                prior reaction.
            sender: Optional JID of the target message's author.  Omit when
                reacting to one's own message.

        Returns:
            JSON string ``{"success": true, "message_id": "..."}`` on success.
            Raises :class:`RuntimeError` on bridge error.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.send_reaction(
            chat_jid=chat_jid,
            message_id=message_id,
            emoji=emoji,
            sender=sender,
        )
        return json.dumps(result)

    # ------------------------------------------------------------------
    # edit_message
    # ------------------------------------------------------------------

    @mcp.tool
    async def edit_message(
        ctx: Context,
        chat_jid: str,
        message_id: str,
        new_text: str,
    ) -> str:
        """Edit the text of a message previously sent by this account.

        Only messages sent by this account can be edited.  WhatsApp also limits
        edits to a short window after sending; the bridge surfaces any rejection
        as an error.

        Args:
            ctx: FastMCP context.
            chat_jid: JID of the chat containing the message to edit.
            message_id: ID of the message to edit (must be one this account
                sent).
            new_text: Replacement message body.  Must be non-empty.

        Returns:
            JSON string ``{"success": true, "message_id": "..."}`` on success.
            Raises :class:`RuntimeError` on validation failure or bridge error.
        """
        if not new_text or not new_text.strip():
            raise RuntimeError("new_text must be a non-empty message body.")

        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.edit_message(
            chat_jid=chat_jid,
            message_id=message_id,
            new_text=new_text,
        )
        return json.dumps(result)

    # ------------------------------------------------------------------
    # revoke_message
    # ------------------------------------------------------------------

    @mcp.tool
    async def revoke_message(
        ctx: Context,
        chat_jid: str,
        message_id: str,
        sender: str | None = None,
    ) -> str:
        """Revoke (delete for everyone) a WhatsApp message.

        Omit ``sender`` to revoke one's own message.  Supply the original
        sender's JID when a group admin revokes another member's message.

        Args:
            ctx: FastMCP context.
            chat_jid: JID of the chat containing the message to revoke.
            message_id: ID of the message to revoke.
            sender: Optional JID of the target message's author.  Required only
                when an admin revokes another member's message.

        Returns:
            JSON string ``{"success": true, "message_id": "..."}`` on success.
            Raises :class:`RuntimeError` on bridge error.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.revoke_message(
            chat_jid=chat_jid,
            message_id=message_id,
            sender=sender,
        )
        return json.dumps(result)

    # ------------------------------------------------------------------
    # check_triggers
    # ------------------------------------------------------------------

    @mcp.tool
    async def check_triggers(
        ctx: Context,
        jids: list[str],
        mention_jid: str | None = None,
        sender_jids: list[str] | None = None,
        limit: int = 100,
        dry_run: bool = False,
    ) -> str:
        """Check multiple chats for new messages in a single batched call.

        A batch version of :func:`check_new_messages` that uses per-chat
        server-side watermarks.  For each JID it returns only the inbound
        messages received since that chat's watermark, optionally filtered by
        mention or sender, then advances the watermark (unless ``dry_run``).

        Args:
            ctx: FastMCP context.
            jids: List of chat JIDs to check.  Must be non-empty.
            mention_jid: Optional JID — keep only messages mentioning it.
            sender_jids: Optional list of sender JIDs — keep only messages from
                these senders.
            limit: Maximum messages returned per chat.  Defaults to 100.
            dry_run: When ``True``, report unseen messages without advancing the
                watermarks (a subsequent call returns the same messages).

        Returns:
            JSON string ``{"total": N, "groups": {jid: {"count": N,
            "messages": [...]}}}``.  Returns ``"No new messages."`` when nothing
            is unseen.  Raises :class:`RuntimeError` when ``jids`` is empty or on
            bridge error.
        """
        if not jids:
            raise RuntimeError("jids must be a non-empty list of chat JIDs.")

        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.check_triggers(
            jids=jids,
            mention_jid=mention_jid,
            sender_jids=sender_jids,
            limit=limit,
            dry_run=dry_run,
        )

        total = result.get("total", 0) if isinstance(result, dict) else 0
        if not total:
            return "No new messages."

        return json.dumps(result)
