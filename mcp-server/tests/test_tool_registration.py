"""Tests that the full tool set is registered on the production server.

These assert against the real ``whatsapp_mcp.server.mcp`` object (not the test
clone) so that any drift between ``server.py`` wiring and the expected tool
inventory is caught.  ``mcp.list_tools()`` enumerates registered tools
server-side without needing a client session or the lifespan/health-check.
"""

from __future__ import annotations

EXPECTED_TOOLS = {
    # messages (10)
    "send_message",
    "check_new_messages",
    "get_messages",
    "get_unread_chats",
    "get_unread_messages",
    "get_group_context",
    "send_reaction",
    "edit_message",
    "revoke_message",
    "check_triggers",
    # groups (4)
    "list_groups",
    "get_group",
    "send_group_message",
    "send_auto_message",
    # media (2)
    "send_media",
    "download_media",
    # contacts (2)
    "list_contacts",
    "get_contact",
    # search (1)
    "semantic_search",
}


async def test_full_tool_set_registered():
    """The production server registers exactly 19 tools (14 existing + 5 new)."""
    from whatsapp_mcp.server import mcp

    tools = await mcp.list_tools()
    names = {t.name for t in tools}

    assert len(tools) == 19, f"expected 19 tools, got {len(tools)}: {sorted(names)}"
    assert names == EXPECTED_TOOLS


async def test_new_tools_present():
    """The five new tools are individually present."""
    from whatsapp_mcp.server import mcp

    names = {t.name for t in await mcp.list_tools()}
    for new_tool in (
        "semantic_search",
        "send_reaction",
        "edit_message",
        "revoke_message",
        "check_triggers",
    ):
        assert new_tool in names, f"missing new tool: {new_tool}"
