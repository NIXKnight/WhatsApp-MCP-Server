# WhatsApp MCP Server

Python FastMCP server exposing 12 WhatsApp tools over the Model Context Protocol. Communicates with the Go WhatsApp bridge via REST HTTP, allowing Claude Code and Claude Desktop to send messages, check unread chats, download media, and more.

## Overview

This is a [FastMCP](https://github.com/modelcontextprotocol/python-sdk) implementation that bridges the Python/Claude ecosystem to WhatsApp via the Go bridge. It implements the MCP stdio transport, meaning it runs as a subprocess spawned by Claude Code or Claude Desktop.

**Prerequisites:**
- Python 3.11+
- Go bridge running on `http://localhost:8080` (see `../bridge/`)
- [uv](https://astral.sh/uv) package manager (recommended; `pip` also works)

## Install and Run

### Quick Start with uv (Recommended)

Run the server without installing, using the system Python:

```bash
uv run --directory /path/to/WhatsApp-MCP-Server/mcp-server whatsapp-mcp
```

Claude Code will start this command automatically via `.mcp.json` configuration.

### Install as Console Script

```bash
cd /path/to/WhatsApp-MCP-Server/mcp-server
uv sync  # or: pip install -e .
whatsapp-mcp
```

### Manual invocation (for testing)

```bash
python -m whatsapp_mcp.main
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BRIDGE_URL` | `http://localhost:8080` | Base URL of the Go WhatsApp bridge. Must be running and accessible. |
| `LOG_LEVEL` | `INFO` | Logging verbosity. Accepted values: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Invalid values revert to `INFO`. |

Example:

```bash
BRIDGE_URL=http://192.168.1.100:8080 whatsapp-mcp
```

### Claude Code Integration (.mcp.json)

Add this to your `.mcp.json` file in Claude Code:

```json
{
  "mcpServers": {
    "WhatsApp": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/WhatsApp-MCP-Server/mcp-server",
        "run",
        "whatsapp-mcp"
      ]
    }
  }
}
```

Restart Claude Code after updating `.mcp.json`.

### Claude Desktop Integration

Add this to `~/.claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "WhatsApp": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/WhatsApp-MCP-Server/mcp-server",
        "run",
        "whatsapp-mcp"
      ],
      "env": {
        "BRIDGE_URL": "http://localhost:8080"
      }
    }
  }
}
```

## MCP Tools Reference

All tools communicate with the Go bridge and return JSON strings. Errors raise `RuntimeError` with descriptive messages.

### Message Tools (5 tools)

| Tool | Parameters | Description |
|------|------------|-------------|
| `send_message` | `to`, `text`, `quoted_message_id?`, `quoted_participant?` | Send a plain-text message to an individual contact |
| `send_group_message` | `to`, `text`, `quoted_message_id?`, `quoted_participant?` | Send a plain-text message to a group chat |
| `check_new_messages` | `since` (Unix ms), `limit?` (1-500, default 100) | Poll for new messages since a Unix millisecond timestamp |
| `get_messages` | `jid`, `limit?` (default 20, max 500) | Retrieve recent messages from a specific chat (individual or group) |
| `get_unread_chats` | `message_limit?` (default 5) | List all chats with unread messages and recent message previews |
| `get_unread_messages` | None | Flat list of all unread messages across all chats |

### Contact Tools (2 tools)

| Tool | Parameters | Description |
|------|------------|-------------|
| `list_contacts` | None | Return all known WhatsApp contacts from the bridge store |
| `get_contact` | `jid` | Retrieve metadata for a single contact by JID |

### Group Tools (3 tools)

| Tool | Parameters | Description |
|------|------------|-------------|
| `list_groups` | None | List all group chats the account belongs to |
| `get_group` | `jid` | Fetch full group metadata including participant list and admin flags |
| `send_group_message` | `to`, `text`, `quoted_message_id?`, `quoted_participant?` | Send a message to a group chat |

### Media Tools (2 tools)

| Tool | Parameters | Description |
|------|------------|-------------|
| `send_media` | `to`, `file_path`, `caption?`, `media_type?` (auto-detect), `ptt?` (default False) | Send image, video, audio, or document to a contact or group |
| `download_media` | `message_id`, `chat_jid`, `output_dir?` | Download and decrypt media from a received message |

## Tool Details

### Message Format

All message tools accept recipient JIDs in two formats:

**Individual contacts:**
- Full JID: `923001234567@s.whatsapp.net` (preferred)
- Bare number: `923001234567` (auto-formatted)

**Group chats:**
- Group JID: `120363039783372408@g.us`

### Timestamps

`check_new_messages` uses Unix milliseconds (13-digit timestamps, e.g., `1700000000000`). Pass `since=0` on the first call to retrieve recent history.

### Quoting/Replying

To reply to or quote an existing message, provide:
- `quoted_message_id`: The ID of the message being quoted
- `quoted_participant`: The JID of the message sender (required if message is not in the bridge store)

Example:

```json
{
  "to": "923001234567@s.whatsapp.net",
  "text": "Your reply here",
  "quotedMessageId": "AAAAH5dVD1Hsc...",
  "quotedParticipant": "923009876543@s.whatsapp.net"
}
```

### Media Types

`send_media` auto-detects media type from file extension:

- **Image**: `.jpg`, `.jpeg`, `.png`, `.gif`, `.webp` → `image`
- **Video**: `.mp4`, `.avi`, `.mkv`, `.mov` → `video`
- **Audio**: `.mp3`, `.ogg`, `.wav`, `.m4a`, `.opus` → `audio`
- **Document**: Any other extension → `document`

Override with the `media_type` parameter.

### Voice Notes (PTT)

To send a WhatsApp voice note (push-to-talk):

1. Provide an OGG Opus `.ogg` file (e.g., generated by the bridge's DSP pipeline)
2. Set `ptt=True`
3. Call `send_media`

The bridge marks the audio with the PTT flag so it appears as a voice note in WhatsApp.

## Architecture

### Lifespan Pattern

The server uses FastMCP's lifespan context to manage the bridge client lifecycle:

1. **Startup**: `BridgeClient` is created and health-checked (5 attempts, 2s intervals)
2. **Tool Calls**: Each tool retrieves the client from `ctx.lifespan_context["bridge"]`
3. **Shutdown**: The client connection pool is cleanly closed

If the bridge is not reachable during startup, the server fails immediately with a clear error message rather than confusing tool-level errors.

### HTTP Retries

- **GET requests**: Retried up to 3 times on transient network errors (ConnectError, TimeoutException) using exponential backoff (0.5s–5s)
- **POST requests**: Never retried to prevent duplicate messages or media uploads

### Error Handling

All HTTP errors are translated to `RuntimeError` with descriptive messages:

- **503 Service Unavailable**: "WhatsApp not connected."
- **Network errors**: "Bridge unavailable at {URL}: {error}"
- **HTTP errors**: "Bridge HTTP {status}: {response_text}"
- **Timeouts**: "Bridge request timed out: {error}"

## Logging

All output is sent to stderr to keep stdout clean for MCP framing (JSON-RPC). The server logs:

- Startup and shutdown
- Bridge health check results
- Tool call execution (via FastMCP)

Log format:

```
2025-01-23T10:45:30 INFO 12345 [whatsapp_mcp.main] main.py:74 - WhatsApp MCP server initialising.
2025-01-23T10:45:30 INFO 12345 [whatsapp_mcp.bridge_client] bridge_client.py:88 - Bridge health check passed on attempt 1.
```

The log level is controlled via the `LOG_LEVEL` environment variable (default: `INFO`):

```bash
LOG_LEVEL=DEBUG whatsapp-mcp
```

Noisy third-party libraries (`httpx`, `httpcore`) are suppressed to `WARNING` regardless of the configured level.

## Troubleshooting

### Bridge Connection Error at Startup

```
BridgeUnavailableError: Go WhatsApp bridge unreachable at http://localhost:8080
```

**Solution**: Ensure the Go bridge is running:

```bash
cd ../bridge
./whatsapp-bridge
```

### "WhatsApp not connected" Error on Tool Call

The bridge is running but the WhatsApp connection has dropped or was never established.

**Solution**: Check the bridge logs and restart it if necessary.

### Tool Returns Empty JSON

The bridge returned a successful HTTP 200, but with unexpected data. This usually indicates a data consistency issue in the bridge store.

**Solution**: Ensure the bridge has been running long enough to sync message history. If the issue persists, check the bridge data directory and logs.

## Project Structure

```
mcp-server/
├── README.md                               This file
├── pyproject.toml                          Project metadata and dependencies
└── src/whatsapp_mcp/
    ├── __init__.py                         Package marker
    ├── main.py                             Entry point (logging, print patching, server startup)
    ├── server.py                           FastMCP server definition and lifespan
    ├── bridge_client.py                    HTTP client for the Go bridge (retries, timeouts)
    ├── models.py                           Pydantic models for requests/responses (if used)
    └── tools/
        ├── __init__.py
        ├── messages.py                     Message tools (send, check, get, unread)
        ├── contacts.py                     Contact tools (list, get)
        ├── groups.py                       Group tools (list, get, send)
        └── media.py                        Media tools (send, download)
```

## Development

### Run Tests

(Tests would be implemented in `tests/` directory)

```bash
uv run pytest
```

### Code Style

The codebase uses standard Python conventions:
- Type hints (Python 3.11+)
- Docstrings (Google style)
- isort, black, ruff (if configured)

### Build and Publish

```bash
uv build  # Creates wheel in dist/
```

## License

MIT License. See [LICENSE](LICENSE) for details.
