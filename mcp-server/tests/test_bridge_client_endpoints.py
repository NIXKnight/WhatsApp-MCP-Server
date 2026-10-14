"""Endpoint-level tests for BridgeClient route and retry contracts."""

from __future__ import annotations

import json

import httpx
import pytest

import whatsapp_mcp.bridge_client as bridge_client_module
from whatsapp_mcp.bridge_client import BridgeClient, BridgeUnavailableError


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


@pytest.mark.parametrize(
    ("error_type", "marker", "expected"),
    [
        (httpx.ConnectError, "TEST_CONNECT_MARKER", "Bridge unavailable."),
        (httpx.ReadTimeout, "TEST_TIMEOUT_MARKER", "Bridge request timed out."),
    ],
)
async def test_post_transport_failure_is_sanitized_and_not_retried(
    error_type,
    marker,
    expected,
):
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise error_type(marker, request=request)

    bridge = _make_client(handler)
    try:
        with pytest.raises(RuntimeError) as exc_info:
            await bridge.post(
                "/api/send",
                json={"to": "9230", "text": "hello"},
            )
    finally:
        await bridge._client.aclose()

    error = str(exc_info.value)
    assert error == expected
    assert marker not in error
    assert "bridge.test" not in error
    assert "/api/send" not in error
    assert attempts == 1
    assert bridge._throttle_calls == 1


@pytest.mark.parametrize(
    ("error_type", "marker", "expected"),
    [
        (httpx.ConnectError, "TEST_GET_CONNECT_MARKER", "Bridge unavailable."),
        (httpx.ReadTimeout, "TEST_GET_TIMEOUT_MARKER", "Bridge request timed out."),
    ],
)
async def test_get_transport_failure_is_sanitized_and_retried_three_times(
    error_type,
    marker,
    expected,
):
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise error_type(marker, request=request)

    bridge = _make_client(handler)
    try:
        with pytest.raises(RuntimeError) as exc_info:
            await bridge.get("/api/messages", chat_jid="120363@g.us")
    finally:
        await bridge._client.aclose()

    error = str(exc_info.value)
    assert error == expected
    assert marker not in error
    assert "bridge.test" not in error
    assert "/api/messages" not in error
    assert attempts == 3
    assert bridge._throttle_calls == 0


@pytest.mark.parametrize("method", ["get", "post"])
async def test_http_error_body_and_machine_code_are_sanitized(method):
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(
            500,
            json={
                "error": "TEST_RAW_RESPONSE_MARKER",
                "code": "TEST_MACHINE_CODE_MARKER",
            },
        )

    bridge = _make_client(handler)
    try:
        with pytest.raises(RuntimeError) as exc_info:
            if method == "get":
                await bridge.get("/api/TEST_GET_PATH")
            else:
                await bridge.post(
                    "/api/TEST_POST_PATH",
                    json={"text": "hello"},
                )
    finally:
        await bridge._client.aclose()

    error = str(exc_info.value)
    assert error == "Bridge request failed with an HTTP server error."
    assert "TEST_RAW_RESPONSE_MARKER" not in error
    assert "TEST_MACHINE_CODE_MARKER" not in error
    assert "bridge.test" not in error
    assert "TEST_GET_PATH" not in error
    assert "TEST_POST_PATH" not in error
    assert attempts == 1


async def test_verify_health_sanitizes_warnings_and_final_error(
    monkeypatch,
    caplog,
):
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("TEST_HEALTH_CONNECT_MARKER", request=request)

    monkeypatch.setattr(bridge_client_module, "_HEALTH_RETRY_DELAY_S", 0)
    bridge = _make_client(handler)
    try:
        with caplog.at_level("WARNING"):
            with pytest.raises(BridgeUnavailableError) as exc_info:
                await bridge.verify_health()
    finally:
        await bridge._client.aclose()

    error = str(exc_info.value)
    assert error == "Go WhatsApp bridge is unavailable after 5 attempts."
    assert "TEST_HEALTH_CONNECT_MARKER" not in error
    assert "bridge.test" not in error
    assert "TEST_HEALTH_CONNECT_MARKER" not in caplog.text
    assert "bridge.test" not in caplog.text
    assert attempts == 5


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


@pytest.mark.parametrize(
    ("status", "code", "expected"),
    [
        (503, "NOT_CONNECTED", "WhatsApp not connected."),
        (
            503,
            "DOWNLOAD_RETRY_PENDING",
            "Media download is pending: the bridge could not fetch it this "
            "moment and is retrying in the background. Try again shortly.",
        ),
        (
            410,
            "MEDIA_PERMANENTLY_UNAVAILABLE",
            "Media is permanently unavailable and cannot be re-downloaded.",
        ),
    ],
)
async def test_search_retains_fixed_semantics_for_known_codes(
    status,
    code,
    expected,
):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            json={"error": "TEST_KNOWN_CODE_BODY_MARKER", "code": code},
        )

    bridge = _make_client(handler)
    try:
        with pytest.raises(RuntimeError) as exc_info:
            await bridge.search(query="hi")
    finally:
        await bridge._client.aclose()

    error = str(exc_info.value)
    assert error == expected
    assert "TEST_KNOWN_CODE_BODY_MARKER" not in error
    assert code not in error
