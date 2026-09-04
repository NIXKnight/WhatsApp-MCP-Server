"""Contact-related MCP tools (L4 tool/contract).

Provides two tools:

* :func:`list_contacts` — Return all contacts known to the bridge store.
* :func:`get_contact` — Return metadata for a single contact by JID.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from fastmcp import Context, FastMCP

if TYPE_CHECKING:
    from whatsapp_mcp.bridge_client import BridgeClient

logger = logging.getLogger(__name__)


def register_contact_tools(mcp: FastMCP) -> None:
    """Register all contact tools onto *mcp*.

    Args:
        mcp: The :class:`~fastmcp.FastMCP` server instance.
    """

    # ------------------------------------------------------------------
    # list_contacts
    # ------------------------------------------------------------------

    @mcp.tool
    async def list_contacts(ctx: Context) -> str:
        """Return all WhatsApp contacts currently known to the bridge store.

        Each entry contains at minimum a ``jid`` field, plus ``name``,
        ``notify``, and ``phone`` fields when available.

        The bridge store is populated from WhatsApp history sync on first
        connection and updated as new messages arrive.  This list may not
        include every contact if history sync is incomplete.

        Args:
            ctx: FastMCP context.

        Returns:
            JSON string containing a list of contact objects, each with
            ``jid``, ``name``, ``notify``, and ``phone`` fields.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get("/api/contacts")
        return json.dumps(result, indent=2)

    # ------------------------------------------------------------------
    # get_contact
    # ------------------------------------------------------------------

    @mcp.tool
    async def get_contact(ctx: Context, jid: str) -> str:
        """Return metadata for a single WhatsApp contact by their JID.

        Looks up the contact in the bridge SQLite store.  Returns a 404-style
        error message when the JID is not found.

        Args:
            ctx: FastMCP context.
            jid: Full WhatsApp JID of the contact, e.g.
                ``923001234567@s.whatsapp.net``.  LID-based JIDs
                (``64617900437747@lid``) are also accepted when the bridge
                stores them.

        Returns:
            JSON string with the contact object (``jid``, ``name``,
            ``notify``, ``phone``) on success, or raises
            :class:`RuntimeError` when the JID is not found or the bridge
            returns an error.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.get(f"/api/contacts/{jid}")
        return json.dumps(result, indent=2)
