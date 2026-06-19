"""Search-related MCP tools (L4 tool/contract).

Provides one tool:

* :func:`semantic_search` — Hybrid (full-text + vector RRF) message search.

The search is routed through the Go bridge's ``POST /api/search`` endpoint
(which already exposes hybrid Reciprocal-Rank-Fusion search), **not** through a
direct database connection.  This keeps the MCP server free of database drivers
and credentials and lets the bridge own all storage concerns.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from fastmcp import Context, FastMCP

if TYPE_CHECKING:
    from whatsapp_mcp.bridge_client import BridgeClient

logger = logging.getLogger(__name__)


def register_search_tools(mcp: FastMCP) -> None:
    """Register all search tools onto *mcp*.

    Args:
        mcp: The :class:`~fastmcp.FastMCP` server instance.
    """

    # ------------------------------------------------------------------
    # semantic_search
    # ------------------------------------------------------------------

    @mcp.tool
    async def semantic_search(
        ctx: Context,
        query: str,
        chat_jid: str | None = None,
        sender: str | None = None,
        limit: int = 10,
    ) -> str:
        """Search stored WhatsApp messages by relevance to a query.

        Runs the bridge's hybrid search (full-text plus optional vector
        Reciprocal-Rank-Fusion) over the message store and returns the
        best-matching messages ranked by score.  Useful for finding a message
        when only its topic or a few words are remembered.

        Args:
            ctx: FastMCP context — provides ``ctx.lifespan_context["bridge"]``.
            query: Natural-language or keyword search string.  Must be non-empty.
            chat_jid: Optional JID to restrict the search to a single chat,
                e.g. ``120363039783372408@g.us`` or
                ``923001234567@s.whatsapp.net``.
            sender: Optional sender JID filter.  Forwarded to the bridge for
                forward-compatibility.
            limit: Maximum number of results to return.  Defaults to 10; the
                bridge clamps the effective value to the range 1-100.

        Returns:
            JSON string ``{"results": [...], "total": N}`` where each result has
            ``id``, ``chat_jid``, ``content``, ``timestamp``, ``sender_name``,
            ``score``, ``snippet``, and ``match_type``.  Returns
            ``"No matching messages found."`` when nothing matches.
        """
        if not query or not query.strip():
            raise RuntimeError("query must be a non-empty search string.")

        bridge: BridgeClient = ctx.lifespan_context["bridge"]
        result = await bridge.search(
            query=query,
            chat_jid=chat_jid,
            sender=sender,
            limit=limit,
        )

        results = result.get("results") if isinstance(result, dict) else None
        if not results:
            return "No matching messages found."

        return json.dumps(result)
