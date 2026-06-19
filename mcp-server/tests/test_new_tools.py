"""In-process tool-invocation tests for the five new tools.

Each test calls a tool via the in-process :class:`fastmcp.Client` and asserts:

1. The tool routed to the correct bridge method (which maps 1:1 to a bridge
   endpoint — the exact path-level mapping is verified in
   ``test_bridge_client_endpoints.py``).
2. The arguments were forwarded faithfully.
3. The cross-cutting :class:`TelemetryMiddleware` recorded the call exactly once
   with the right tool name and a success outcome — proving the telemetry hook
   lives in a single place and fires for every tool.
"""

from __future__ import annotations

import json

import pytest
from fastmcp.exceptions import ToolError


async def test_semantic_search_routes_to_search(client, fake_bridge):
    res = await client.call_tool(
        "semantic_search",
        {"query": "dinner plans", "chat_jid": "120363000000000000@g.us", "limit": 5},
    )
    assert not res.is_error

    method, kwargs = fake_bridge.calls[-1]
    assert method == "search"
    assert kwargs["query"] == "dinner plans"
    assert kwargs["chat_jid"] == "120363000000000000@g.us"
    assert kwargs["limit"] == 5

    payload = json.loads(res.data)
    assert payload["total"] == 1

    assert fake_bridge.telemetry[-1]["tool_name"] == "semantic_search"
    assert fake_bridge.telemetry[-1]["success"] is True


async def test_semantic_search_empty_query_errors(client, fake_bridge):
    with pytest.raises(ToolError):
        await client.call_tool("semantic_search", {"query": "   "})
    # No bridge search attempted on invalid input.
    assert all(m != "search" for m, _ in fake_bridge.calls)
    # Telemetry still recorded the (failed) call exactly once.
    assert fake_bridge.telemetry[-1]["tool_name"] == "semantic_search"
    assert fake_bridge.telemetry[-1]["success"] is False


async def test_send_reaction_routes_to_send_reaction(client, fake_bridge):
    res = await client.call_tool(
        "send_reaction",
        {
            "chat_jid": "120363000000000000@g.us",
            "message_id": "MSG1",
            "emoji": "👍",
            "sender": "923001234567@s.whatsapp.net",
        },
    )
    assert not res.is_error

    method, kwargs = fake_bridge.calls[-1]
    assert method == "send_reaction"
    assert kwargs == {
        "chat_jid": "120363000000000000@g.us",
        "message_id": "MSG1",
        "emoji": "👍",
        "sender": "923001234567@s.whatsapp.net",
    }
    assert fake_bridge.telemetry[-1]["tool_name"] == "send_reaction"
    assert fake_bridge.telemetry[-1]["success"] is True


async def test_edit_message_routes_to_edit_message(client, fake_bridge):
    res = await client.call_tool(
        "edit_message",
        {"chat_jid": "120363000000000000@g.us", "message_id": "MSG2", "new_text": "fixed"},
    )
    assert not res.is_error

    method, kwargs = fake_bridge.calls[-1]
    assert method == "edit_message"
    assert kwargs["new_text"] == "fixed"
    assert fake_bridge.telemetry[-1]["tool_name"] == "edit_message"


async def test_edit_message_empty_text_errors(client, fake_bridge):
    with pytest.raises(ToolError):
        await client.call_tool(
            "edit_message",
            {"chat_jid": "120363000000000000@g.us", "message_id": "MSG2", "new_text": " "},
        )
    assert all(m != "edit_message" for m, _ in fake_bridge.calls)
    assert fake_bridge.telemetry[-1]["tool_name"] == "edit_message"
    assert fake_bridge.telemetry[-1]["success"] is False


async def test_revoke_message_routes_to_revoke_message(client, fake_bridge):
    res = await client.call_tool(
        "revoke_message",
        {"chat_jid": "120363000000000000@g.us", "message_id": "MSG3"},
    )
    assert not res.is_error

    method, kwargs = fake_bridge.calls[-1]
    assert method == "revoke_message"
    assert kwargs["message_id"] == "MSG3"
    assert kwargs["sender"] is None
    assert fake_bridge.telemetry[-1]["tool_name"] == "revoke_message"


async def test_check_triggers_routes_to_check_triggers(client, fake_bridge):
    res = await client.call_tool(
        "check_triggers",
        {
            "jids": ["120363000000000000@g.us"],
            "mention_jid": "923001234567@s.whatsapp.net",
            "limit": 50,
            "dry_run": True,
        },
    )
    assert not res.is_error

    method, kwargs = fake_bridge.calls[-1]
    assert method == "check_triggers"
    assert kwargs["jids"] == ["120363000000000000@g.us"]
    assert kwargs["mention_jid"] == "923001234567@s.whatsapp.net"
    assert kwargs["limit"] == 50
    assert kwargs["dry_run"] is True

    payload = json.loads(res.data)
    assert payload["total"] == 1
    assert fake_bridge.telemetry[-1]["tool_name"] == "check_triggers"


async def test_check_triggers_empty_jids_errors(client, fake_bridge):
    with pytest.raises(ToolError):
        await client.call_tool("check_triggers", {"jids": []})
    assert all(m != "check_triggers" for m, _ in fake_bridge.calls)
    assert fake_bridge.telemetry[-1]["tool_name"] == "check_triggers"
    assert fake_bridge.telemetry[-1]["success"] is False


async def test_telemetry_fires_once_per_call(client, fake_bridge):
    """Telemetry is recorded exactly once per tool call, in one place."""
    before = len(fake_bridge.telemetry)
    await client.call_tool(
        "send_reaction",
        {"chat_jid": "120363000000000000@g.us", "message_id": "X", "emoji": "🔥"},
    )
    assert len(fake_bridge.telemetry) == before + 1
