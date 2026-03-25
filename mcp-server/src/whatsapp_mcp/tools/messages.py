"""Message-related MCP tools.

Provides five tools:

* :func:`send_message` — Send a plain-text message to an individual contact.
* :func:`check_new_messages` — Poll for new messages since a given Unix timestamp.
* :func:`get_messages` — Retrieve recent messages from a specific chat.
* :func:`get_unread_chats` — List all chats that have unread messages.
* :func:`get_unread_messages` — Flat list of all unread messages across all chats.
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

        Returns:
            JSON string with ``{"count": N, "messages": [...], "since": "...",
            "timestamp": "..."}`` where each message has ``id``,
            ``chat_jid``, ``sender``, ``sender_name``, ``content``,
            ``timestamp``, ``is_from_me``, ``push_name``.
            When nothing is new: ``"No new messages."``.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]

        result = await bridge.get("/api/check", since=since, limit=limit)

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
        return json.dumps(result, indent=2)

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

        return json.dumps(result, indent=2)

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

        return json.dumps(result, indent=2)
