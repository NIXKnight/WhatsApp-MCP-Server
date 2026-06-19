"""Endpoint-level tests for the new BridgeClient methods.

Uses ``httpx.MockTransport`` to intercept requests at the transport layer and
assert that each new client method targets the exact bridge path with the
expected JSON body and HTTP method.  This is the authoritative proof that each
high-level method maps to the correct bridge endpoint.

It also verifies the throttle routing: send mutations (reaction/edit/revoke) go
through the throttled ``post`` path, while read/telemetry POSTs (search,
check_triggers, telemetry) bypass the throttle.
"""

from __future__ import annotations

import json

import httpx
import pytest

from whatsapp_mcp.bridge_client import BridgeClient


def _make_client(handler) -> BridgeClient:
    """Build a BridgeClient whose httpx client uses a MockTransport handler."""
    bc = BridgeClient(base_url="http://bridge.test")
    bc._client = httpx.AsyncClient(
        base_url="http://bridge.test",
        transport=httpx.MockTransport(handler),
    )
    # Neutralise the inter-send throttle so tests do not sleep 2-7s.
    bc._throttle_calls = 0

    async def _no_throttle() -> None:
        bc._throttle_calls += 1

    bc._throttle_send = _no_throttle  # type: ignore[method-assign]
    return bc


async def test_search_hits_api_search():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": [], "total": 0})

    bc = _make_client(handler)
    try:
        await bc.search(query="hi", chat_jid="120363@g.us", limit=7)
    finally:
        await bc._client.aclose()

    assert captured["method"] == "POST"
    assert captured["url"].endswith("/api/search")
    assert captured["body"] == {"query": "hi", "limit": 7, "chat_jid": "120363@g.us"}
    # Search is a read — must NOT have been throttled.
    assert bc._throttle_calls == 0


async def test_send_reaction_hits_api_send_reaction_throttled():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "message_id": "R1"})

    bc = _make_client(handler)
    try:
        await bc.send_reaction(chat_jid="120363@g.us", message_id="M1", emoji="👍")
    finally:
        await bc._client.aclose()

    assert captured["url"].endswith("/api/send/reaction")
    assert captured["body"] == {"chat_jid": "120363@g.us", "message_id": "M1", "emoji": "👍"}
    # Reaction is a send — must have been throttled exactly once.
    assert bc._throttle_calls == 1


async def test_edit_message_hits_api_send_edit_throttled():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "message_id": "M2"})

    bc = _make_client(handler)
    try:
        await bc.edit_message(chat_jid="120363@g.us", message_id="M2", new_text="new")
    finally:
        await bc._client.aclose()

    assert captured["url"].endswith("/api/send/edit")
    assert captured["body"] == {"chat_jid": "120363@g.us", "message_id": "M2", "new_text": "new"}
    assert bc._throttle_calls == 1


async def test_revoke_message_hits_api_send_revoke_throttled():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "message_id": "M3"})

    bc = _make_client(handler)
    try:
        await bc.revoke_message(
            chat_jid="120363@g.us", message_id="M3", sender="9230@s.whatsapp.net"
        )
    finally:
        await bc._client.aclose()

    assert captured["url"].endswith("/api/send/revoke")
    assert captured["body"] == {
        "chat_jid": "120363@g.us",
        "message_id": "M3",
        "sender": "9230@s.whatsapp.net",
    }
    assert bc._throttle_calls == 1


async def test_check_triggers_hits_api_check_triggers_not_throttled():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"total": 0, "groups": {}})

    bc = _make_client(handler)
    try:
        await bc.check_triggers(
            jids=["120363@g.us"], mention_jid="9230@s.whatsapp.net", limit=10, dry_run=True
        )
    finally:
        await bc._client.aclose()

    assert captured["url"].endswith("/api/check/triggers")
    assert captured["body"] == {
        "jids": ["120363@g.us"],
        "limit": 10,
        "filters": {"mention_jid": "9230@s.whatsapp.net"},
        "dry_run": True,
    }
    # Trigger check is a read — must NOT have been throttled.
    assert bc._throttle_calls == 0


async def test_record_tool_call_hits_telemetry_endpoint():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    bc = _make_client(handler)
    try:
        await bc.record_tool_call("send_reaction", 12, True, "")
    finally:
        await bc._client.aclose()

    assert captured["url"].endswith("/api/telemetry/tool")
    assert captured["body"] == {
        "tool_name": "send_reaction",
        "duration_ms": 12,
        "success": True,
        "error_msg": "",
    }
    assert bc._throttle_calls == 0


async def test_record_tool_call_never_raises_on_failure():
    """Telemetry is fire-and-forget: a 500 (or transport error) must not raise."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    bc = _make_client(handler)
    try:
        # Must complete without raising despite the bridge returning 500.
        await bc.record_tool_call("x", 1, False, "err")
    finally:
        await bc._client.aclose()


async def test_search_translates_503_to_not_connected():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "not connected"})

    bc = _make_client(handler)
    try:
        with pytest.raises(RuntimeError, match="WhatsApp not connected"):
            await bc.search(query="hi")
    finally:
        await bc._client.aclose()
