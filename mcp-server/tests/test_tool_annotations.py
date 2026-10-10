"""Focused MCP safety-hint tests.

These tests intentionally do not assert the complete tool count. The production
inventory has a separate known 19-vs-20 baseline issue that is unrelated to the
read-only annotations covered here.
"""

from __future__ import annotations

import pytest


@pytest.fixture
async def tools_by_name():
    """Return the production tool definitions keyed by server-native name."""
    from whatsapp_mcp.server import mcp

    return {tool.name: tool for tool in await mcp.list_tools()}


@pytest.mark.parametrize("tool_name", ["get_messages", "get_contact"])
async def test_scoped_read_tools_advertise_read_only_hint(tools_by_name, tool_name):
    """The two approved scoped reads explicitly advertise read-only behavior."""
    annotations = tools_by_name[tool_name].annotations

    assert annotations is not None
    assert annotations.readOnlyHint is True


@pytest.mark.parametrize(
    "tool_name",
    ["send_message", "send_reaction", "edit_message", "revoke_message"],
)
async def test_mutation_tools_do_not_advertise_read_only_hint(tools_by_name, tool_name):
    """Representative mutations must not advertise the advisory read-only hint."""
    annotations = tools_by_name[tool_name].annotations

    # FastMCP may expose no annotations or an MCP default annotation object.
    assert getattr(annotations, "readOnlyHint", None) is not True
