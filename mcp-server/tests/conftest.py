"""Shared pytest fixtures for the WhatsApp MCP server tests.

Provides:

* :class:`FakeBridge` — an in-memory stand-in for
  :class:`~whatsapp_mcp.bridge_client.BridgeClient` that records every call and
  returns canned responses, so tool routing can be asserted without a live Go
  bridge.
* ``client`` fixture — an in-process :class:`fastmcp.Client` connected to a
  FastMCP server wired *identically* to ``whatsapp_mcp.server`` (same tool
  ``register_*`` helpers, same :class:`TelemetryMiddleware`) but with a lifespan
  that injects a :class:`FakeBridge` instead of opening a real HTTP client.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import pytest
import pytest_asyncio
from fastmcp import Client, FastMCP

from whatsapp_mcp.middleware import TelemetryMiddleware
from whatsapp_mcp.tools.contacts import register_contact_tools
from whatsapp_mcp.tools.groups import register_group_tools
from whatsapp_mcp.tools.media import register_media_tools
from whatsapp_mcp.tools.messages import register_message_tools
from whatsapp_mcp.tools.search import register_search_tools


class FakeBridge:
    """Records calls to the bridge surface and returns canned responses.

    Implements the high-level endpoint methods used by the new tools plus the
    ``get``/``post`` primitives used by the existing tools, so a fully wired
    server can be exercised end-to-end in-process.  Every call is appended to
    :attr:`calls` as ``(method_name, kwargs)`` and telemetry recordings to
    :attr:`telemetry`.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.telemetry: list[dict[str, Any]] = []

    # -- new typed endpoint wrappers --------------------------------------

    async def search(
        self,
        query: str,
        chat_jid: str | None = None,
        sender: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        self.calls.append(
            ("search", {"query": query, "chat_jid": chat_jid, "sender": sender, "limit": limit})
        )
        return {
            "results": [
                {
                    "id": "M1",
                    "chat_jid": chat_jid or "120363000000000000@g.us",
                    "content": "hello world",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "sender_name": "Alice",
                    "score": 0.9,
                    "snippet": "hello",
                    "match_type": "fts",
                }
            ],
            "total": 1,
        }

    async def send_reaction(
        self, chat_jid: str, message_id: str, emoji: str, sender: str | None = None
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "send_reaction",
                {"chat_jid": chat_jid, "message_id": message_id, "emoji": emoji, "sender": sender},
            )
        )
        return {"success": True, "message_id": "R1"}

    async def edit_message(self, chat_jid: str, message_id: str, new_text: str) -> dict[str, Any]:
        self.calls.append(
            ("edit_message", {"chat_jid": chat_jid, "message_id": message_id, "new_text": new_text})
        )
        return {"success": True, "message_id": message_id}

    async def revoke_message(
        self, chat_jid: str, message_id: str, sender: str | None = None
    ) -> dict[str, Any]:
        self.calls.append(
            ("revoke_message", {"chat_jid": chat_jid, "message_id": message_id, "sender": sender})
        )
        return {"success": True, "message_id": message_id}

    async def check_triggers(
        self,
        jids: list[str],
        mention_jid: str | None = None,
        sender_jids: list[str] | None = None,
        limit: int = 100,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "check_triggers",
                {
                    "jids": jids,
                    "mention_jid": mention_jid,
                    "sender_jids": sender_jids,
                    "limit": limit,
                    "dry_run": dry_run,
                },
            )
        )
        return {
            "total": 1,
            "groups": {
                jids[0]: {
                    "count": 1,
                    "messages": [{"id": "T1", "chat_jid": jids[0], "content": "ping"}],
                }
            },
        }

    async def record_tool_call(
        self, tool_name: str, duration_ms: int, success: bool, error_msg: str = ""
    ) -> None:
        # Mirrors the real client's fire-and-forget contract: never raises.
        self.telemetry.append(
            {
                "tool_name": tool_name,
                "duration_ms": duration_ms,
                "success": success,
                "error_msg": error_msg,
            }
        )

    # -- primitives used by the pre-existing tools (not exercised here) ----

    async def get(self, path: str, **params: Any) -> Any:  # pragma: no cover - defensive
        self.calls.append(("get", {"path": path, "params": params}))
        return {}

    async def post(self, path: str, json: dict[str, Any] | None = None) -> Any:  # pragma: no cover
        self.calls.append(("post", {"path": path, "json": json}))
        return {}


def build_test_server(bridge: FakeBridge) -> FastMCP:
    """Build a FastMCP server wired exactly like ``whatsapp_mcp.server``.

    The only difference from production is the lifespan, which yields the
    supplied :class:`FakeBridge` under the ``"bridge"`` key instead of opening a
    real HTTP client and running a health check.  The same ``register_*``
    helpers and the same :class:`TelemetryMiddleware` are used, so tool
    registration and the telemetry hook are tested as shipped.
    """

    @asynccontextmanager
    async def _lifespan(server: FastMCP):  # noqa: ARG001
        yield {"bridge": bridge}

    mcp = FastMCP("WhatsApp-Test", lifespan=_lifespan)
    mcp.add_middleware(TelemetryMiddleware())
    register_message_tools(mcp)
    register_contact_tools(mcp)
    register_group_tools(mcp)
    register_media_tools(mcp)
    register_search_tools(mcp)
    return mcp


@pytest.fixture
def fake_bridge() -> FakeBridge:
    return FakeBridge()


@pytest_asyncio.fixture
async def client(fake_bridge: FakeBridge):
    """In-process FastMCP client connected to the fake-bridge test server."""
    mcp = build_test_server(fake_bridge)
    async with Client(mcp) as c:
        yield c
