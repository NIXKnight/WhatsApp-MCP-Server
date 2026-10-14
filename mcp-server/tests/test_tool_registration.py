"""Assert the exact production FastMCP tool inventory."""

from __future__ import annotations


EXPECTED_TOOLS = {
    # messages (11)
    "send_message",
    "check_new_messages",
    "get_message",
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
    # media (3)
    "send_media",
    "download_media",
    "analyze_media",
    # contacts (2)
    "list_contacts",
    "get_contact",
    # search (1)
    "semantic_search",
}


async def test_full_tool_set_registered():
    """Production registers exactly the approved 21-tool inventory."""
    from whatsapp_mcp.server import mcp

    tools = await mcp.list_tools()
    names = {tool.name for tool in tools}

    assert len(tools) == 21, f"expected 21 tools, got {len(tools)}: {sorted(names)}"
    assert names == EXPECTED_TOOLS


async def test_slice_two_inventory_additions_present():
    """The exact read and existing media analyzer remain explicit."""
    from whatsapp_mcp.server import mcp

    names = {tool.name for tool in await mcp.list_tools()}

    assert "get_message" in names
    assert "analyze_media" in names
