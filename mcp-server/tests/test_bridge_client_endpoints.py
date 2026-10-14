"""Endpoint-level tests for BridgeClient route and retry contracts."""

from __future__ import annotations

import json

import httpx
import pytest

from whatsapp_mcp.bridge_client import BridgeClient


def _make_client(handler) -> BridgeClient:
    """Build a client whose HTTP layer uses a recording mock transport."""
    bridge = BridgeClient(base_url="http://bridge.test")
    bridge._client = httpx.AsyncClient(
        base_url="http://bridge.test",
        transport=httpx.MockTransport(handler),
    )
    bridge._throttle_calls = 0

    async def _no_throttle() -> None:
        bridge._throttle_calls += 1

    bridge._throttle_send = _no_throttle  # type: ignore[method-assign]
    return bridge


async def test_get_message_encodes_one_path_segment_and_exact_query():
    captured: dict = {}
    envelope = {
        "message": {
            "id": "M/1 ?#%",
            "chat_jid": "120363@g.us",
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["raw_path"] = request.url.raw_path.split(b"?", 1)[0]
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=envelope)

    bridge = _make_client(handler)
    try:
        result = await bridge.get_message(
            message_id="M/1 ?#%",
            chat_jid="120363@g.us",
        )
    finally:
        await bridge._client.aclose()

    assert result == envelope
    assert captured == {
        "method": "GET",
        "raw_path": b"/api/messages/M%2F1%20%3F%23%25",
        "params": {"chat_jid": "120363@g.us"},
    }
    assert bridge._throttle_calls == 0


async def test_get_message_preserves_literal_percent_escape():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["raw_path"] = request.url.raw_path.split(b"?", 1)[0]
        return httpx.Response(200, json={"message": {"id": "M%2F1"}})

    bridge = _make_client(handler)
    try:
        await bridge.get_message(
            message_id="M%2F1",
            chat_jid="120363@g.us",
        )
    finally:
        await bridge._client.aclose()

    assert captured["raw_path"] == b"/api/messages/M%252F1"


async def test_check_query_uses_jid_not_chat_jid():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(
            200,
            json={
                "messages": [],
                "count": 0,
                "since": "2026-01-01T00:00:00Z",
                "timestamp": "2026-01-01T00:00:01Z",
            },
        )

    bridge = _make_client(handler)
    try:
        await bridge.get(
            "/api/check",
            since=1000,
            limit=7,
            jid="120363@g.us",
        )
    finally:
        await bridge._client.aclose()

    assert captured == {
        "method": "GET",
        "path": "/api/check",
        "params": {
            "since": "1000",
            "limit": "7",
            "jid": "120363@g.us",
        },
    }


async def test_search_hits_api_search():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": [], "total": 0})

    bridge = _make_client(handler)
    try:
        await bridge.search(query="hi", chat_jid="120363@g.us", limit=7)
    finally:
        await bridge._client.aclose()

    assert captured["method"] == "POST"
    assert captured["url"].endswith("/api/search")
    assert captured["body"] == {
        "query": "hi",
        "limit": 7,
        "chat_jid": "120363@g.us",
    }
    assert bridge._throttle_calls == 0


async def test_send_reaction_hits_api_send_reaction_throttled():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"success": True, "message_id": "R1"},
        )

    bridge = _make_client(handler)
    try:
        await bridge.send_reaction(
            chat_jid="120363@g.us",
            message_id="M1",
            emoji="👍",
        )
    finally:
        await bridge._client.aclose()

    assert captured["url"].endswith("/api/send/reaction")
    assert captured["body"] == {
        "chat_jid": "120363@g.us",
        "message_id": "M1",
        "emoji": "👍",
    }
    assert bridge._throttle_calls == 1


async def test_edit_message_hits_api_send_edit_throttled():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"success": True, "message_id": "M2"},
        )

    bridge = _make_client(handler)
    try:
        await bridge.edit_message(
            chat_jid="120363@g.us",
            message_id="M2",
            new_text="new",
        )
    finally:
        await bridge._client.aclose()

    assert captured["url"].endswith("/api/send/edit")
    assert captured["body"] == {
        "chat_jid": "120363@g.us",
        "message_id": "M2",
        "new_text": "new",
    }
    assert bridge._throttle_calls == 1


async def test_revoke_message_hits_api_send_revoke_throttled():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"success": True, "message_id": "M3"},
        )

    bridge = _make_client(handler)
    try:
        await bridge.revoke_message(
            chat_jid="120363@g.us",
            message_id="M3",
            sender="9230@s.whatsapp.net",
        )
    finally:
        await bridge._client.aclose()

    assert captured["url"].endswith("/api/send/revoke")
    assert captured["body"] == {
        "chat_jid": "120363@g.us",
        "message_id": "M3",
        "sender": "9230@s.whatsapp.net",
    }
    assert bridge._throttle_calls == 1


async def test_post_transport_failure_is_not_retried():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("TEST_CONNECT_FAILURE", request=request)

    bridge = _make_client(handler)
    try:
        with pytest.raises(RuntimeError, match="Bridge unavailable"):
            await bridge.post(
                "/api/send",
                json={"to": "9230", "text": "hello"},
            )
    finally:
        await bridge._client.aclose()

    assert attempts == 1
    assert bridge._throttle_calls == 1


async def test_check_triggers_hits_api_check_triggers_not_throttled():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"total": 0, "groups": {}})

    bridge = _make_client(handler)
    try:
        await bridge.check_triggers(
            jids=["120363@g.us"],
            mention_jid="9230@s.whatsapp.net",
            limit=10,
            dry_run=True,
        )
    finally:
        await bridge._client.aclose()

    assert captured["url"].endswith("/api/check/triggers")
    assert captured["body"] == {
        "jids": ["120363@g.us"],
        "limit": 10,
        "filters": {"mention_jid": "9230@s.whatsapp.net"},
        "dry_run": True,
    }
    assert bridge._throttle_calls == 0


async def test_record_tool_call_hits_telemetry_endpoint():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    bridge = _make_client(handler)
    try:
        await bridge.record_tool_call("get_message", 12, True, "")
    finally:
        await bridge._client.aclose()

    assert captured["url"].endswith("/api/telemetry/tool")
    assert captured["body"] == {
        "tool_name": "get_message",
        "duration_ms": 12,
        "success": True,
        "error_msg": "",
    }
    assert bridge._throttle_calls == 0


async def test_record_tool_call_never_raises_on_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "TEST_FAILURE"})

    bridge = _make_client(handler)
    try:
        await bridge.record_tool_call("x", 1, False, "TEST_ERROR")
    finally:
        await bridge._client.aclose()


async def test_search_translates_coded_503_to_not_connected():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            json={"error": "not connected", "code": "NOT_CONNECTED"},
        )

    bridge = _make_client(handler)
    try:
        with pytest.raises(RuntimeError, match="WhatsApp not connected"):
            await bridge.search(query="hi")
    finally:
        await bridge._client.aclose()
