"""Message-related MCP tools (L4 tool/contract).

Provides eleven tools:

* :func:`send_message` — Send a plain-text message to an individual contact.
* :func:`check_new_messages` — Poll from a supplied Unix-millisecond watermark.
* :func:`get_message` — Retrieve one exact message from a specific chat.
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

from whatsapp_mcp.models import (
    CheckResponse,
    MessageResponseEnvelope,
    MessagesResponse,
    SendResponse,
    validate_bridge_response,
)

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
            JSON string with ``{"success": true, "message_id": "SENT1"}`` on
            success. Raises :class:`RuntimeError` on a bridge error or invalid
            bridge response contract.
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
        response = validate_bridge_response(SendResponse, result)
        return json.dumps(response.model_dump(mode="json", exclude_unset=True))

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

        The caller owns the watermark. Pass ``since=0`` on the first call.
        The optional chat filter is sent under the bridge's exact ``jid`` query
        key, while the MCP argument remains ``chat_jid`` for compatibility.

        Args:
            ctx: FastMCP context.
            since: Unix timestamp in **milliseconds**.  Only messages stored
                after this time are returned.  Pass ``0`` to retrieve all
                recent stored messages.  Example: ``1700000000000``.
            limit: Maximum number of messages to return (1-500, default 100).
            chat_jid: Optional JID to filter results to a single chat.

        Returns:
            A validated JSON ``CheckResponse`` envelope, or exactly
            ``No new messages.`` when that valid envelope has no messages.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get(
            "/api/check",
            since=since,
            limit=limit,
            jid=chat_jid,
        )
        response = validate_bridge_response(CheckResponse, result)
        if not response.messages:
            return "No new messages."

        return json.dumps(response.model_dump(mode="json", exclude_unset=True))

    # ------------------------------------------------------------------
    # get_message
    # ------------------------------------------------------------------

    @mcp.tool(annotations={"readOnlyHint": True})
    async def get_message(
        ctx: Context,
        message_id: str,
        chat_jid: str,
    ) -> str:
        """Retrieve one exact WhatsApp message from its actual chat.

        Message IDs are scoped by chat, so both values are required. Use this
        tool when an event supplies an exact ``message_id`` and ``chat_jid``.

        Args:
            ctx: FastMCP context.
            message_id: Exact stored WhatsApp message ID.
            chat_jid: Exact JID of the chat containing the message.

        Returns:
            The nested ``MessageResponse`` object as JSON after strict
            ``MessageResponseEnvelope`` validation.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get_message(
            message_id=message_id,
            chat_jid=chat_jid,
        )
        response = validate_bridge_response(MessageResponseEnvelope, result)
        return json.dumps(
            response.message.model_dump(mode="json", exclude_unset=True)
        )

    # ------------------------------------------------------------------
    # get_messages
    # ------------------------------------------------------------------

    @mcp.tool(annotations={"readOnlyHint": True})
    async def get_messages(
        ctx: Context,
        jid: str,
        limit: int = 20,
    ) -> str:
        """Retrieve the most recent messages from a WhatsApp chat.

        Messages are returned newest first. Individual and group JIDs are
        supported.

        Args:
            ctx: FastMCP context.
            jid: Full WhatsApp JID of the chat.  Individual contacts use the
                form ``923001234567@s.whatsapp.net``; groups use
                ``120363039783372408@g.us``.
            limit: Maximum number of messages to return.  Defaults to 20.
                Capped by the bridge at 500.

        Returns:
            A validated JSON ``MessagesResponse`` envelope containing complete
            ``MessageResponse`` objects plus pagination metadata.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get(
            "/api/messages",
            chat_jid=jid,
            limit=limit,
        )
        response = validate_bridge_response(MessagesResponse, result)
        return json.dumps(response.model_dump(mode="json", exclude_unset=True))

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

        Own messages and blank content are removed. The remaining bridge
        newest-first messages are emitted oldest first.

        Args:
            ctx: FastMCP context.
            jid: Group JID ending with @g.us.
            limit: Maximum messages (default 20).

        Returns:
            JSON objects containing only ``name``, ``sender``, ``text``, and
            ``ts`` in chronological order.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get(
            "/api/messages",
            chat_jid=jid,
            limit=limit,
        )
        response = validate_bridge_response(MessagesResponse, result)

        chronological = []
        for message in reversed(response.messages):
            if message.is_from_me:
                continue
            text = message.content.strip()
            if not text:
                continue
            dumped = message.model_dump(mode="json", exclude_unset=True)
            chronological.append(
                {
                    "name": (
                        message.sender_name or message.push_name or message.sender
                    ),
                    "sender": message.sender,
                    "text": text,
                    "ts": dumped["timestamp"],
                }
            )

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
