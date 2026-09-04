"""Group-related MCP tools (L4 tool/contract).

Provides four tools:

* :func:`list_groups` — List all group chats the account belongs to.
* :func:`get_group` — Fetch full metadata for a single group including participants.
* :func:`send_group_message` — Send a plain-text message to a group chat.
* :func:`send_auto_message` — Send a message to any JID, auto-routing to individual or group.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from fastmcp import Context, FastMCP

if TYPE_CHECKING:
    from whatsapp_mcp.bridge_client import BridgeClient

logger = logging.getLogger(__name__)


def register_group_tools(mcp: FastMCP) -> None:
    """Register all group tools onto *mcp*.

    Args:
        mcp: The :class:`~fastmcp.FastMCP` server instance.
    """

    # ------------------------------------------------------------------
    # list_groups
    # ------------------------------------------------------------------

    @mcp.tool
    async def list_groups(ctx: Context) -> str:
        """Return all WhatsApp group chats the account belongs to.

        Groups are identified by JIDs ending with ``@g.us``.  Each entry
        includes at minimum the group ``jid`` and ``name``; participant lists
        are not included — use :func:`get_group` for that.

        Args:
            ctx: FastMCP context.

        Returns:
            JSON string containing a list of group objects, each with
            ``jid``, ``name``, ``participant_count``, and
            ``last_message_time``.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get("/api/groups")
        return json.dumps(result)

    # ------------------------------------------------------------------
    # get_group
    # ------------------------------------------------------------------

    @mcp.tool
    async def get_group(ctx: Context, jid: str) -> str:
        """Fetch full metadata for a WhatsApp group, including participant list.

        The ``jid`` must end with ``@g.us``.  The response includes the group
        name, description, owner, creation time, and the full participant list
        with admin flags.

        Args:
            ctx: FastMCP context.
            jid: Group JID — must end with ``@g.us``, e.g.
                ``120363039783372408@g.us``.

        Returns:
            JSON string with group metadata (``jid``, ``name``,
            ``is_group``, ``unread_count``, ``last_message_time``,
            ``last_message_preview``, ``participants``).  Each participant has
            ``jid``, ``is_admin``, and ``is_super_admin`` fields.
            Raises :class:`RuntimeError` when the JID is invalid, not found,
            or the bridge returns an error.
        """
        if not jid.endswith("@g.us"):
            raise RuntimeError(
                f'Invalid group JID "{jid}": group JIDs must end with @g.us. '
                "Example: 120363039783372408@g.us"
            )

        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get(f"/api/groups/{jid}")
        return json.dumps(result)

    # ------------------------------------------------------------------
    # send_group_message
    # ------------------------------------------------------------------

    @mcp.tool
    async def send_group_message(
        ctx: Context,
        jid: str,
        text: str,
        quoted_message_id: str | None = None,
        quoted_participant: str | None = None,
        mentions: list[str] | None = None,
    ) -> str:
        """Send a plain-text message to a WhatsApp group chat.

        The ``jid`` must be a group JID ending with ``@g.us``.  For sending
        to individual contacts use :func:`send_message` instead.

        Optionally quote or reply to an existing group message by providing
        its ID in ``quoted_message_id``.  Supply ``quoted_participant`` when
        the quoted message sender's JID is known so the bridge can construct
        the reply context even if the original message has expired from the
        store.

        Args:
            ctx: FastMCP context.
            jid: Group JID — must end with ``@g.us``, e.g.
                ``120363039783372408@g.us``.
            text: Message text to send to the group.  Must be non-empty.
            quoted_message_id: Optional message ID to reply or quote.
            quoted_participant: Optional JID (phone JID or LID) of the quoted
                message sender.  Required for quoting when the original message
                may no longer be in the bridge store.  Example:
                ``259570677067973@lid``.
            mentions: Optional list of JIDs to tag in the message. Use
                phone JIDs (e.g. ``["923224387030@s.whatsapp.net"]``).
                The message text should contain matching ``@phone``
                placeholders (e.g. ``@923224387030``) for WhatsApp to
                render them as display names.

        Returns:
            JSON string with ``{"id": "...", "timestamp": "..."}`` on
            success.  Raises :class:`RuntimeError` on validation failure or
            bridge error.
        """
        if not jid.endswith("@g.us"):
            raise RuntimeError(
                f'Invalid group JID "{jid}": group JIDs must end with @g.us. '
                "Example: 120363039783372408@g.us"
            )

        bridge: BridgeClient = ctx.lifespan_context["bridge"]

        payload: dict = {"to": jid, "text": text}
        if quoted_message_id:
            payload["quotedMessageId"] = quoted_message_id
        if quoted_participant:
            payload["quotedParticipant"] = quoted_participant
        if mentions:
            payload["mentions"] = mentions

        result = await bridge.post("/api/send", json=payload)
        return json.dumps(result)

    # ------------------------------------------------------------------
    # send_auto_message
    # ------------------------------------------------------------------

    @mcp.tool
    async def send_auto_message(
        ctx: Context,
        jid: str,
        text: str,
        quoted_message_id: str | None = None,
        quoted_participant: str | None = None,
        mentions: list[str] | None = None,
    ) -> str:
        """Send a message to any WhatsApp JID, auto-routing based on suffix.

        Works for both individual contacts (@s.whatsapp.net) and groups
        (@g.us). Bare phone numbers are treated as individual contacts.

        Args:
            ctx: FastMCP context.
            jid: Any WhatsApp JID — individual, group, or bare phone number.
            text: Message body to send. Must be non-empty.
            quoted_message_id: Optional message ID to reply or quote.
            quoted_participant: Optional JID of the quoted message sender.
            mentions: Optional list of JIDs to tag in the message.

        Returns:
            JSON string with {"id": "...", "timestamp": "..."} on success.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]

        payload: dict = {"to": jid, "text": text}
        if quoted_message_id:
            payload["quotedMessageId"] = quoted_message_id
        if quoted_participant:
            payload["quotedParticipant"] = quoted_participant
        if mentions:
            payload["mentions"] = mentions

        result = await bridge.post("/api/send", json=payload)
        return json.dumps(result)
