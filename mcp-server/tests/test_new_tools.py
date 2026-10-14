"""In-process tests for new tools and repaired message contracts."""

from __future__ import annotations

import copy
import json

import pytest
from fastmcp.exceptions import ToolError


TEST_CHAT_JID = "120363000000000000@g.us"


async def test_get_message_routes_validates_envelope_and_records_telemetry(
    client,
    fake_bridge,
):
    timestamp = "2026-01-01T00:00:00.123456789Z"
    transcribed_at = "2026-01-01T00:00:01.987654321+05:30"
    fake_bridge.message_response["message"]["timestamp"] = timestamp
    fake_bridge.message_response["message"]["transcribed_at"] = transcribed_at

    result = await client.call_tool(
        "get_message",
        {"message_id": "M1", "chat_jid": TEST_CHAT_JID},
    )

    assert not result.is_error
    assert fake_bridge.calls[-1] == (
        "get_message",
        {"message_id": "M1", "chat_jid": TEST_CHAT_JID},
    )
    payload = json.loads(result.data)
    assert "message" not in payload
    assert payload["id"] == "M1"
    assert payload["snippet"] == "hello"
    assert payload["transcription"] == "fixture transcript"
    assert payload["timestamp"] == timestamp
    assert payload["transcribed_at"] == transcribed_at
    assert fake_bridge.telemetry[-1]["tool_name"] == "get_message"
    assert fake_bridge.telemetry[-1]["success"] is True


@pytest.mark.parametrize("timezone", ["+23:59", "-23:59", "Z"])
async def test_get_message_preserves_valid_rfc3339_boundary_offsets(
    client,
    fake_bridge,
    timezone,
):
    timestamp = f"2026-01-01T00:00:00.123456789{timezone}"
    fake_bridge.message_response["message"]["timestamp"] = timestamp

    result = await client.call_tool(
        "get_message",
        {"message_id": "M1", "chat_jid": TEST_CHAT_JID},
    )

    assert json.loads(result.data)["timestamp"] == timestamp


@pytest.mark.parametrize("timezone", ["+00:60", "-12:60", "+24:00"])
async def test_get_message_rejects_invalid_rfc3339_offsets_without_echo(
    client,
    fake_bridge,
    timezone,
):
    timestamp = f"2026-01-01T00:00:00.123456789{timezone}"
    fake_bridge.message_response["message"]["timestamp"] = timestamp

    with pytest.raises(ToolError) as exc_info:
        await client.call_tool(
            "get_message",
            {"message_id": "M1", "chat_jid": TEST_CHAT_JID},
        )

    error = str(exc_info.value)
    assert "MessageResponseEnvelope" in error
    assert timestamp not in error
    assert fake_bridge.telemetry[-1]["success"] is False
    assert timestamp not in fake_bridge.telemetry[-1]["error_msg"]


async def test_get_message_preserves_go_omitempty_shape(client, fake_bridge):
    fake_bridge.message_response = {
        "message": {
            "id": "M_MINIMAL",
            "chat_jid": TEST_CHAT_JID,
            "sender": "9230@s.whatsapp.net",
            "sender_name": "Alice",
            "content": "hello",
            "timestamp": "2026-01-01T00:00:00Z",
            "is_from_me": False,
        }
    }

    result = await client.call_tool(
        "get_message",
        {"message_id": "M_MINIMAL", "chat_jid": TEST_CHAT_JID},
    )

    assert json.loads(result.data) == fake_bridge.message_response["message"]


@pytest.mark.parametrize(
    "malformed",
    [
        {
            "message": {
                "id": "M1",
                "chat_jid": TEST_CHAT_JID,
                "sender": "9230@s.whatsapp.net",
                "sender_name": "Alice",
                "content": "hello",
                "timestamp": "2026-01-01T00:00:00Z",
                "is_from_me": "TEST_RAW_RESPONSE_MARKER",
            }
        },
        {
            "message": {
                "id": "M1",
                "chat_jid": TEST_CHAT_JID,
                "sender": "9230@s.whatsapp.net",
                "sender_name": "Alice",
                "content": "hello",
                "timestamp": "2026-01-01T00:00:00Z",
                "is_from_me": False,
            },
            "unexpected": "TEST_RAW_RESPONSE_MARKER",
        },
        {
            "message": {
                "id": "M1",
                "chat_jid": TEST_CHAT_JID,
                "sender": "9230@s.whatsapp.net",
                "sender_name": "Alice",
                "content": "hello",
                "timestamp": "2026-01-01 00:00:00+00:00",
                "is_from_me": False,
            }
        },
        {
            "message": {
                "id": "M1",
                "chat_jid": TEST_CHAT_JID,
                "sender": "9230@s.whatsapp.net",
                "sender_name": "Alice",
                "content": "hello",
                "timestamp": "2026-02-30T00:00:00Z",
                "is_from_me": False,
            }
        },
        {
            "message": {
                "id": "M1",
                "chat_jid": TEST_CHAT_JID,
                "sender": "9230@s.whatsapp.net",
                "sender_name": "Alice",
                "content": "hello",
                "timestamp": "2026-01-01T00:00:00.1234567890Z",
                "is_from_me": False,
            }
        },
        {
            "message": {
                "id": "M1",
                "chat_jid": TEST_CHAT_JID,
                "sender": "9230@s.whatsapp.net",
                "sender_name": "Alice",
                "content": "hello",
                "timestamp": "2026-01-01T00:00:00Z",
                "is_from_me": False,
                "media_type": "",
            }
        },
        {
            "message": {
                "id": "M1",
                "chat_jid": TEST_CHAT_JID,
                "sender": "9230@s.whatsapp.net",
                "sender_name": "Alice",
                "content": "hello",
                "timestamp": "2026-01-01T00:00:00Z",
                "is_from_me": False,
                "file_length": 0,
            }
        },
        {
            "message": {
                "id": "M1",
                "chat_jid": TEST_CHAT_JID,
                "sender": "9230@s.whatsapp.net",
                "sender_name": "Alice",
                "content": "hello",
                "timestamp": "2026-01-01T00:00:00Z",
                "is_from_me": False,
                "transcribed_at": None,
            }
        },
        {
            "id": "TEST_RAW_RESPONSE_MARKER",
        },
    ],
)
async def test_get_message_rejects_malformed_envelope_without_echo(
    client,
    fake_bridge,
    malformed,
):
    fake_bridge.message_response = malformed
    telemetry_before = len(fake_bridge.telemetry)

    with pytest.raises(ToolError) as exc_info:
        await client.call_tool(
            "get_message",
            {"message_id": "M1", "chat_jid": TEST_CHAT_JID},
        )

    error = str(exc_info.value)
    assert "MessageResponseEnvelope" in error
    assert "TEST_RAW_RESPONSE_MARKER" not in error
    assert len(fake_bridge.telemetry) == telemetry_before + 1
    assert fake_bridge.telemetry[-1]["tool_name"] == "get_message"
    assert fake_bridge.telemetry[-1]["success"] is False
    assert "TEST_RAW_RESPONSE_MARKER" not in fake_bridge.telemetry[-1]["error_msg"]


async def test_check_new_messages_uses_jid_and_preserves_empty_text(
    client,
    fake_bridge,
):
    fake_bridge.check_response = {
        "messages": [],
        "count": 0,
        "since": "2026-01-01T00:00:00Z",
        "timestamp": "2026-01-01T00:00:02Z",
    }

    result = await client.call_tool(
        "check_new_messages",
        {"since": 1000, "limit": 7, "chat_jid": TEST_CHAT_JID},
    )

    assert result.data == "No new messages."
    assert fake_bridge.calls[-1] == (
        "get",
        {
            "path": "/api/check",
            "params": {
                "since": 1000,
                "limit": 7,
                "jid": TEST_CHAT_JID,
            },
        },
    )
    assert "chat_jid" not in fake_bridge.calls[-1][1]["params"]
    assert fake_bridge.telemetry[-1]["tool_name"] == "check_new_messages"
    assert fake_bridge.telemetry[-1]["success"] is True


async def test_check_new_messages_preserves_nanosecond_timestamps(
    client,
    fake_bridge,
):
    message = copy.deepcopy(fake_bridge.messages_response["messages"][0])
    message["timestamp"] = "2026-01-01T00:00:00.123456789Z"
    fake_bridge.check_response = {
        "messages": [message],
        "count": 1,
        "since": "2026-01-01T00:00:00.000000001Z",
        "timestamp": "2026-01-01T00:00:02.999999999+00:00",
    }

    result = await client.call_tool("check_new_messages", {"since": 0})

    payload = json.loads(result.data)
    assert payload["messages"][0]["timestamp"] == message["timestamp"]
    assert payload["since"] == fake_bridge.check_response["since"]
    assert payload["timestamp"] == fake_bridge.check_response["timestamp"]


async def test_check_new_messages_rejects_strict_malformed_envelope(
    client,
    fake_bridge,
):
    fake_bridge.check_response = {
        "messages": [],
        "count": "0",
        "since": "2026-01-01T00:00:00Z",
        "timestamp": "2026-01-01T00:00:02Z",
    }

    with pytest.raises(ToolError, match="CheckResponse"):
        await client.call_tool("check_new_messages", {"since": 0})

    assert fake_bridge.telemetry[-1]["success"] is False


async def test_get_messages_validates_and_returns_envelope(client, fake_bridge):
    timestamp = "2026-01-01T00:00:00.123456789-04:30"
    fake_bridge.messages_response["messages"][0]["timestamp"] = timestamp

    result = await client.call_tool(
        "get_messages",
        {"jid": TEST_CHAT_JID, "limit": 9},
    )

    payload = json.loads(result.data)
    assert set(payload) == {"messages", "total", "limit", "offset"}
    assert payload["messages"][0]["transcription"] == "fixture transcript"
    assert payload["messages"][0]["timestamp"] == timestamp
    assert fake_bridge.calls[-1] == (
        "get",
        {
            "path": "/api/messages",
            "params": {"chat_jid": TEST_CHAT_JID, "limit": 9},
        },
    )


async def test_get_messages_rejects_malformed_envelope(client, fake_bridge):
    fake_bridge.messages_response = {
        "messages": [],
        "total": 0,
        "limit": 20,
    }

    with pytest.raises(ToolError, match="MessagesResponse"):
        await client.call_tool("get_messages", {"jid": TEST_CHAT_JID})


async def test_group_context_filters_and_orders_chronologically(
    client,
    fake_bridge,
):
    template = fake_bridge.messages_response["messages"][0]

    newest = copy.deepcopy(template)
    newest.update(
        {
            "id": "M4",
            "content": " newest ",
            "sender": "400@s.whatsapp.net",
            "sender_name": "Newest",
            "timestamp": "2026-01-01T00:00:04Z",
            "is_from_me": False,
        }
    )
    own = copy.deepcopy(template)
    own.update(
        {
            "id": "M3",
            "content": "must be filtered",
            "timestamp": "2026-01-01T00:00:03Z",
            "is_from_me": True,
        }
    )
    blank = copy.deepcopy(template)
    blank.update(
        {
            "id": "M2",
            "content": "   ",
            "timestamp": "2026-01-01T00:00:02Z",
            "is_from_me": False,
        }
    )
    oldest = copy.deepcopy(template)
    oldest.update(
        {
            "id": "M1",
            "content": " oldest ",
            "sender": "300@s.whatsapp.net",
            "sender_name": "",
            "push_name": "Oldest Push",
            "timestamp": "2026-01-01T00:00:01Z",
            "is_from_me": False,
        }
    )
    fake_bridge.messages_response = {
        "messages": [newest, own, blank, oldest],
        "total": 4,
        "limit": 20,
        "offset": 0,
    }

    result = await client.call_tool(
        "get_group_context",
        {"jid": TEST_CHAT_JID, "limit": 20},
    )

    assert json.loads(result.data) == [
        {
            "name": "Oldest Push",
            "sender": "300@s.whatsapp.net",
            "text": "oldest",
            "ts": "2026-01-01T00:00:01Z",
        },
        {
            "name": "Newest",
            "sender": "400@s.whatsapp.net",
            "text": "newest",
            "ts": "2026-01-01T00:00:04Z",
        },
    ]
    assert fake_bridge.calls[-1][1] == {
        "path": "/api/messages",
        "params": {"chat_jid": TEST_CHAT_JID, "limit": 20},
    }


@pytest.mark.parametrize(
    ("tool_name", "arguments"),
    [
        ("send_message", {"to": "923001234567", "text": "hello"}),
        (
            "send_group_message",
            {"jid": TEST_CHAT_JID, "text": "hello"},
        ),
        (
            "send_auto_message",
            {"jid": TEST_CHAT_JID, "text": "hello"},
        ),
    ],
)
async def test_text_send_tools_validate_success_message_id(
    client,
    fake_bridge,
    tool_name,
    arguments,
):
    fake_bridge.send_response = {
        "success": True,
        "message_id": "SENT1",
    }

    result = await client.call_tool(tool_name, arguments)

    assert json.loads(result.data) == {
        "success": True,
        "message_id": "SENT1",
    }
    assert fake_bridge.calls[-1][0] == "post"
    assert fake_bridge.calls[-1][1]["path"] == "/api/send"
    assert fake_bridge.telemetry[-1]["tool_name"] == tool_name
    assert fake_bridge.telemetry[-1]["success"] is True


@pytest.mark.parametrize(
    ("tool_name", "arguments", "malformed"),
    [
        (
            "send_message",
            {"to": "923001234567", "text": "hello"},
            {
                "success": "true",
                "message_id": "TEST_RAW_RESPONSE_MARKER",
            },
        ),
        (
            "send_group_message",
            {"jid": TEST_CHAT_JID, "text": "hello"},
            {"success": False, "message_id": "TEST_RAW_RESPONSE_MARKER"},
        ),
        (
            "send_auto_message",
            {"jid": TEST_CHAT_JID, "text": "hello"},
            {"success": True, "message_id": ""},
        ),
    ],
)
async def test_text_send_tools_reject_strict_malformed_response(
    client,
    fake_bridge,
    tool_name,
    arguments,
    malformed,
):
    fake_bridge.send_response = malformed

    with pytest.raises(ToolError) as exc_info:
        await client.call_tool(tool_name, arguments)

    error = str(exc_info.value)
    assert "SendResponse" in error
    assert "TEST_RAW_RESPONSE_MARKER" not in error
    assert fake_bridge.telemetry[-1]["tool_name"] == tool_name
    assert fake_bridge.telemetry[-1]["success"] is False
    assert "TEST_RAW_RESPONSE_MARKER" not in fake_bridge.telemetry[-1]["error_msg"]


async def test_semantic_search_routes_to_search(client, fake_bridge):
    result = await client.call_tool(
        "semantic_search",
        {
            "query": "dinner plans",
            "chat_jid": TEST_CHAT_JID,
            "limit": 5,
        },
    )

    assert not result.is_error
    assert fake_bridge.calls[-1][0] == "search"
    assert fake_bridge.calls[-1][1]["query"] == "dinner plans"
    assert fake_bridge.calls[-1][1]["chat_jid"] == TEST_CHAT_JID
    assert fake_bridge.calls[-1][1]["limit"] == 5
    assert json.loads(result.data)["total"] == 1
    assert fake_bridge.telemetry[-1]["tool_name"] == "semantic_search"
    assert fake_bridge.telemetry[-1]["success"] is True


async def test_semantic_search_empty_query_errors(client, fake_bridge):
    with pytest.raises(ToolError):
        await client.call_tool("semantic_search", {"query": "   "})

    assert all(method != "search" for method, _ in fake_bridge.calls)
    assert fake_bridge.telemetry[-1]["tool_name"] == "semantic_search"
    assert fake_bridge.telemetry[-1]["success"] is False


async def test_send_reaction_routes_to_send_reaction(client, fake_bridge):
    result = await client.call_tool(
        "send_reaction",
        {
            "chat_jid": TEST_CHAT_JID,
            "message_id": "MSG1",
            "emoji": "👍",
            "sender": "923001234567@s.whatsapp.net",
        },
    )

    assert not result.is_error
    assert fake_bridge.calls[-1] == (
        "send_reaction",
        {
            "chat_jid": TEST_CHAT_JID,
            "message_id": "MSG1",
            "emoji": "👍",
            "sender": "923001234567@s.whatsapp.net",
        },
    )
    assert fake_bridge.telemetry[-1]["tool_name"] == "send_reaction"
    assert fake_bridge.telemetry[-1]["success"] is True


async def test_edit_message_routes_to_edit_message(client, fake_bridge):
    result = await client.call_tool(
        "edit_message",
        {
            "chat_jid": TEST_CHAT_JID,
            "message_id": "MSG2",
            "new_text": "fixed",
        },
    )

    assert not result.is_error
    assert fake_bridge.calls[-1][0] == "edit_message"
    assert fake_bridge.calls[-1][1]["new_text"] == "fixed"
    assert fake_bridge.telemetry[-1]["tool_name"] == "edit_message"


async def test_edit_message_empty_text_errors(client, fake_bridge):
    with pytest.raises(ToolError):
        await client.call_tool(
            "edit_message",
            {
                "chat_jid": TEST_CHAT_JID,
                "message_id": "MSG2",
                "new_text": " ",
            },
        )

    assert all(method != "edit_message" for method, _ in fake_bridge.calls)
    assert fake_bridge.telemetry[-1]["tool_name"] == "edit_message"
    assert fake_bridge.telemetry[-1]["success"] is False


async def test_revoke_message_routes_to_revoke_message(client, fake_bridge):
    result = await client.call_tool(
        "revoke_message",
        {"chat_jid": TEST_CHAT_JID, "message_id": "MSG3"},
    )

    assert not result.is_error
    assert fake_bridge.calls[-1][0] == "revoke_message"
    assert fake_bridge.calls[-1][1]["message_id"] == "MSG3"
    assert fake_bridge.calls[-1][1]["sender"] is None
    assert fake_bridge.telemetry[-1]["tool_name"] == "revoke_message"


async def test_check_triggers_routes_to_check_triggers(client, fake_bridge):
    result = await client.call_tool(
        "check_triggers",
        {
            "jids": [TEST_CHAT_JID],
            "mention_jid": "923001234567@s.whatsapp.net",
            "limit": 50,
            "dry_run": True,
        },
    )

    assert not result.is_error
    assert fake_bridge.calls[-1][0] == "check_triggers"
    assert fake_bridge.calls[-1][1]["jids"] == [TEST_CHAT_JID]
    assert fake_bridge.calls[-1][1]["mention_jid"] == ("923001234567@s.whatsapp.net")
    assert fake_bridge.calls[-1][1]["limit"] == 50
    assert fake_bridge.calls[-1][1]["dry_run"] is True
    assert json.loads(result.data)["total"] == 1
    assert fake_bridge.telemetry[-1]["tool_name"] == "check_triggers"


async def test_check_triggers_empty_jids_errors(client, fake_bridge):
    with pytest.raises(ToolError):
        await client.call_tool("check_triggers", {"jids": []})

    assert all(method != "check_triggers" for method, _ in fake_bridge.calls)
    assert fake_bridge.telemetry[-1]["tool_name"] == "check_triggers"
    assert fake_bridge.telemetry[-1]["success"] is False


async def test_telemetry_fires_once_per_call(client, fake_bridge):
    before = len(fake_bridge.telemetry)
    await client.call_tool(
        "get_message",
        {"message_id": "M1", "chat_jid": TEST_CHAT_JID},
    )
    assert len(fake_bridge.telemetry) == before + 1
