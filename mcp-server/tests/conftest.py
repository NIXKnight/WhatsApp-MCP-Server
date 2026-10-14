"""Production-equivalent FastMCP test wiring with a recording fake bridge."""

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


TEST_CHAT_JID = "120363000000000000@g.us"
TEST_TIMESTAMP = "2026-01-01T00:00:00Z"


def message_response(
    *,
    message_id: str = "M1",
    content: str = "hello world",
    is_from_me: bool = False,
    sender: str = "923001234567@s.whatsapp.net",
    sender_name: str = "Alice",
    push_name: str = "Alice Push",
    timestamp: str = TEST_TIMESTAMP,
) -> dict[str, Any]:
    """Return a complete non-sensitive ``MessageResponse`` fixture."""
    return {
        "id": message_id,
        "chat_jid": TEST_CHAT_JID,
        "sender": sender,
        "sender_name": sender_name,
        "content": content,
        "timestamp": timestamp,
        "is_from_me": is_from_me,
        "media_type": "audio",
        "filename": "fixture.ogg",
        "url": "https://example.test/fixture.ogg",
        "file_length": 42,
        "push_name": push_name,
        "quoted_message_id": "Q1",
        "quoted_participant": "923009999999@s.whatsapp.net",
        "snippet": "hello",
        "transcription": "fixture transcript",
        "transcribed_at": "2026-01-01T00:00:01Z",
    }


class FakeBridge:
    """Record bridge calls and expose mutable canned contract responses."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.telemetry: list[dict[str, Any]] = []
        self.message_response: Any = {"message": message_response()}
        self.messages_response: Any = {
            "messages": [message_response()],
            "total": 1,
            "limit": 20,
            "offset": 0,
        }
        self.check_response: Any = {
            "messages": [message_response()],
            "count": 1,
            "since": "2026-01-01T00:00:00Z",
            "timestamp": "2026-01-01T00:00:02Z",
        }
        self.send_response: Any = {"success": True, "message_id": "S1"}

    async def get_message(self, message_id: str, chat_jid: str) -> Any:
        self.calls.append(
            (
                "get_message",
                {"message_id": message_id, "chat_jid": chat_jid},
            )
        )
        return self.message_response

    async def search(
        self,
        query: str,
        chat_jid: str | None = None,
        sender: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "search",
                {
                    "query": query,
                    "chat_jid": chat_jid,
                    "sender": sender,
                    "limit": limit,
                },
            )
        )
        return {
            "results": [
                {
                    "id": "M1",
                    "chat_jid": chat_jid or TEST_CHAT_JID,
                    "content": "hello world",
                    "timestamp": TEST_TIMESTAMP,
                    "sender_name": "Alice",
                    "score": 0.9,
                    "snippet": "hello",
                    "match_type": "fts",
                }
            ],
            "total": 1,
        }

    async def send_reaction(
        self,
        chat_jid: str,
        message_id: str,
        emoji: str,
        sender: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "send_reaction",
                {
                    "chat_jid": chat_jid,
                    "message_id": message_id,
                    "emoji": emoji,
                    "sender": sender,
                },
            )
        )
        return {"success": True, "message_id": "R1"}

    async def edit_message(
        self,
        chat_jid: str,
        message_id: str,
        new_text: str,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "edit_message",
                {
                    "chat_jid": chat_jid,
                    "message_id": message_id,
                    "new_text": new_text,
                },
            )
        )
        return {"success": True, "message_id": message_id}

    async def revoke_message(
        self,
        chat_jid: str,
        message_id: str,
        sender: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "revoke_message",
                {
                    "chat_jid": chat_jid,
                    "message_id": message_id,
                    "sender": sender,
                },
            )
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
                    "messages": [
                        {
                            "id": "T1",
                            "chat_jid": jids[0],
                            "content": "ping",
                        }
                    ],
                }
            },
        }

    async def record_tool_call(
        self,
        tool_name: str,
        duration_ms: int,
        success: bool,
        error_msg: str = "",
    ) -> None:
        self.telemetry.append(
            {
                "tool_name": tool_name,
                "duration_ms": duration_ms,
                "success": success,
                "error_msg": error_msg,
            }
        )

    async def get(self, path: str, **params: Any) -> Any:
        self.calls.append(("get", {"path": path, "params": params}))
        if path == "/api/check":
            return self.check_response
        if path == "/api/messages":
            return self.messages_response
        return {}

    async def post(
        self,
        path: str,
        json: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append(("post", {"path": path, "json": json}))
        if path == "/api/send":
            return self.send_response
        return {}


def build_test_server(bridge: FakeBridge) -> FastMCP:
    """Build production registration and middleware with an injected bridge."""

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
    """Return an in-process client connected to the fake bridge server."""
    mcp = build_test_server(fake_bridge)
    async with Client(mcp) as connected:
        yield connected
