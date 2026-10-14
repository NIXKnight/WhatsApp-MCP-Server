"""Focused tests for advisory FastMCP tool safety annotations."""

from __future__ import annotations

import pytest


@pytest.fixture
async def tools_by_name():
    """Return production tool definitions keyed by server-native name."""
    from whatsapp_mcp.server import mcp

    return {tool.name: tool for tool in await mcp.list_tools()}


@pytest.mark.parametrize(
    "tool_name",
    ["get_message", "get_messages", "get_contact"],
)
async def test_scoped_read_tools_advertise_read_only_hint(
    tools_by_name,
    tool_name,
):
    annotations = tools_by_name[tool_name].annotations

    assert annotations is not None
    assert annotations.readOnlyHint is True


@pytest.mark.parametrize(
    "tool_name",
    [
        "send_message",
        "send_group_message",
        "send_auto_message",
        "send_reaction",
        "edit_message",
        "revoke_message",
    ],
)
async def test_mutation_tools_do_not_advertise_read_only_hint(
    tools_by_name,
    tool_name,
):
    annotations = tools_by_name[tool_name].annotations

    assert getattr(annotations, "readOnlyHint", None) is not True
