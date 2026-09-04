"""FastMCP server definition for the WhatsApp MCP server.

Wires together the lifespan (bridge client startup/shutdown), the cross-cutting
telemetry middleware, and all tool modules.  The :data:`mcp` instance is the
single authoritative FastMCP object; tools are registered against it in each
``register_*`` helper.

Lifespan pattern
----------------
The bridge client is constructed once on server startup, health-checked, and
injected into ``ctx.lifespan_context["bridge"]`` for every tool call.  On
server shutdown the client is cleanly closed.

Usage::

    # Importing this module produces the ready-to-run ``mcp`` object.
    from whatsapp_mcp.server import mcp
    mcp.run()
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastmcp import FastMCP

from whatsapp_mcp.bridge_client import BridgeClient
from whatsapp_mcp.middleware import TelemetryMiddleware
from whatsapp_mcp.tools.contacts import register_contact_tools
from whatsapp_mcp.tools.groups import register_group_tools
from whatsapp_mcp.tools.media import register_media_tools
from whatsapp_mcp.tools.messages import register_message_tools
from whatsapp_mcp.tools.search import register_search_tools

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(server: FastMCP) -> AsyncIterator[dict]:  # noqa: ARG001
    """FastMCP lifespan: start the bridge client and verify connectivity.

    Yields a context dict containing the single :class:`BridgeClient` instance
    under the key ``"bridge"``.  Tools retrieve it via
    ``ctx.lifespan_context["bridge"]``.

    The health check polls the bridge up to five times with a two-second gap.
    If the bridge is unreachable after all attempts, the server startup fails
    with a :class:`~whatsapp_mcp.bridge_client.BridgeUnavailableError` so the
    MCP client receives a clean error rather than a confusing tool-level
    exception on the first call.
    """
    logger.info("WhatsApp MCP server starting — verifying Go bridge connectivity.")
    client = BridgeClient()
    await client.verify_health()
    logger.info("Bridge connected. Server ready.")

    try:
        yield {"bridge": client}
    finally:
        logger.info("WhatsApp MCP server shutting down — closing bridge client.")
        await client.close()


# ---------------------------------------------------------------------------
# Create server
# ---------------------------------------------------------------------------

mcp: FastMCP = FastMCP("WhatsApp", lifespan=_lifespan)

# ---------------------------------------------------------------------------
# Cross-cutting middleware
# ---------------------------------------------------------------------------
# TelemetryMiddleware records every tool call's name, duration, and outcome to
# the bridge in one place via ``on_call_tool`` — no per-tool timing boilerplate.
mcp.add_middleware(TelemetryMiddleware())

# ---------------------------------------------------------------------------
# Register all tool modules
# ---------------------------------------------------------------------------

register_message_tools(mcp)
register_contact_tools(mcp)
register_group_tools(mcp)
register_media_tools(mcp)
register_search_tools(mcp)
