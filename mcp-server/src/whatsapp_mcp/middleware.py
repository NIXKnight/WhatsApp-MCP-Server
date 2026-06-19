"""Cross-cutting FastMCP middleware for the WhatsApp MCP server.

Houses the single telemetry hook that records every tool invocation's name,
duration, and outcome to the Go bridge.  Implementing this once as middleware —
rather than threading start/stop timing and a ``record_tool_call`` line through
every individual tool — keeps the tool functions focused on their domain logic
and guarantees uniform, drift-free telemetry coverage across all tools.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from fastmcp.server.middleware import Middleware, MiddlewareContext

if TYPE_CHECKING:
    from whatsapp_mcp.bridge_client import BridgeClient

logger = logging.getLogger(__name__)


class TelemetryMiddleware(Middleware):
    """Record per-tool-call telemetry to the bridge in one cross-cutting place.

    Wraps every ``tools/call`` request via :meth:`on_call_tool`.  Timing starts
    before the tool runs and the outcome (success / error) is captured around
    ``call_next``.  After the tool completes — whether it returned or raised —
    the result is forwarded to
    :meth:`~whatsapp_mcp.bridge_client.BridgeClient.record_tool_call`.

    Guarantees:

    * The tool's return value is never altered — ``call_next`` 's result is
      returned verbatim.
    * Tool exceptions propagate unchanged; only the telemetry outcome flag is
      recorded before re-raising.
    * Telemetry failures never surface.  ``record_tool_call`` swallows its own
      errors, and the bridge lookup / recording is additionally guarded here so
      a missing lifespan context or recording fault can never break a tool call.
    """

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        """Time the tool call, run it, then record telemetry fire-and-forget.

        Args:
            context: Middleware context.  ``context.message.name`` is the tool
                name; ``context.fastmcp_context`` exposes the lifespan context
                holding the shared :class:`BridgeClient`.
            call_next: Continuation that executes the tool (and any inner
                middleware) and returns its :class:`ToolResult`.

        Returns:
            The tool's :class:`ToolResult`, unmodified.
        """
        tool_name = context.message.name
        start = time.monotonic()
        success = True
        error_msg = ""

        try:
            return await call_next(context)
        except Exception as exc:
            success = False
            error_msg = str(exc)
            raise
        finally:
            duration_ms = int((time.monotonic() - start) * 1000)
            await self._record(context, tool_name, duration_ms, success, error_msg)

    @staticmethod
    async def _record(
        context: MiddlewareContext,
        tool_name: str,
        duration_ms: int,
        success: bool,
        error_msg: str,
    ) -> None:
        """Look up the bridge and record the call, swallowing any fault.

        Telemetry is best-effort: a missing lifespan context (e.g. a call that
        never reached a live session) or any recording error is logged at debug
        level and otherwise ignored, so this method never affects the tool path.
        """
        try:
            fastmcp_ctx = context.fastmcp_context
            if fastmcp_ctx is None:
                return
            bridge: BridgeClient = fastmcp_ctx.lifespan_context["bridge"]
            # record_tool_call is itself fire-and-forget (never raises); awaiting
            # it keeps telemetry deterministic without altering the tool result.
            await bridge.record_tool_call(tool_name, duration_ms, success, error_msg)
        except Exception as exc:  # noqa: BLE001 — telemetry must never propagate.
            logger.debug("Telemetry hook for %s failed (ignored): %s", tool_name, exc)
