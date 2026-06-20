# WhatsApp MCP Server

Python FastMCP server exposing 20 WhatsApp tools over the Model Context Protocol. Communicates with the Go WhatsApp bridge via REST HTTP, allowing Claude Code and Claude Desktop to send messages, check unread chats, react to / edit / revoke messages, search history, download and analyze media, and more.

## Overview

This is a [FastMCP](https://github.com/jlowin/fastmcp) implementation (the `fastmcp>=2.0` package) that bridges the Python/Claude ecosystem to WhatsApp via the Go bridge. It supports three MCP transports: `stdio` (default, for Claude Code / Desktop), `sse`, and `http` (streamable-HTTP) for containerized deployments.

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

### Docker

When running via Docker Compose, the server uses SSE transport on port 3000:

```bash
docker compose up -d
```

Configure your MCP client:

```json
{
  "mcpServers": {
    "WhatsApp": {
      "type": "sse",
      "url": "http://localhost:3000/sse"
    }
  }
}
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BRIDGE_URL` | `http://localhost:8080` | Base URL of the Go WhatsApp bridge. Must be running and accessible. |
| `LOG_LEVEL` | `INFO` | Logging verbosity. Accepted values: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Invalid values revert to `INFO`. |
| `MCP_TRANSPORT` | `stdio` | Transport protocol: `stdio`, `sse`, or `http` (streamable-HTTP). |
| `MCP_HOST` | `0.0.0.0` | Bind address for the `sse` / `http` network transports. |
| `MCP_PORT` | `3000` | Port for the `sse` / `http` network transports. |
| `WORKSPACE_DIR` | `/workspace` | Container workspace directory for media files. `download_media` saves into this directory. |
| `WORKSPACE_HOST_PATH` | (none) | Host path prefix for automatic `send_media` path translation. |

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

20 tools across five modules (`src/whatsapp_mcp/tools/`). All tools communicate with the Go bridge and return JSON strings. Errors raise `RuntimeError` with descriptive messages.

### Message Tools (10 tools)

| Tool | Parameters | Description |
|------|------------|-------------|
| `send_message` | `to`, `text`, `quoted_message_id?`, `quoted_participant?`, `mentions?` | Send a plain-text message to an individual contact; supports optional `mentions` list for @-mentions |
| `check_new_messages` | `since` (Unix ms), `limit?` (1-500, default 100), `chat_jid?` | Poll for new messages since a Unix millisecond timestamp; optionally restrict to a single chat |
| `get_messages` | `jid`, `limit?` (default 20, max 500) | Retrieve recent messages from a specific chat (individual or group), newest first |
| `get_unread_chats` | `message_limit?` (default 5) | List all chats with unread messages and recent message previews |
| `get_unread_messages` | None | Flat list of all unread messages across all chats |
| `get_group_context` | `jid`, `limit?` (default 20) | Compact recent-turn window for a group chat in chronological order; strips own messages and empty content |
| `send_reaction` | `chat_jid`, `message_id`, `emoji`, `sender?` | React to a message with an emoji; pass an empty `emoji` string to remove a prior reaction |
| `edit_message` | `chat_jid`, `message_id`, `new_text` | Edit the text of a message previously sent by this account |
| `revoke_message` | `chat_jid`, `message_id`, `sender?` | Revoke (delete for everyone) a message; supply `sender` only when an admin revokes another member's message |
| `check_triggers` | `jids`, `mention_jid?`, `sender_jids?`, `limit?` (default 100), `dry_run?` (default False) | Batched multi-chat check using per-chat server-side watermarks; advances each watermark unless `dry_run` |

### Contact Tools (2 tools)

| Tool | Parameters | Description |
|------|------------|-------------|
| `list_contacts` | None | Return all known WhatsApp contacts from the bridge store |
| `get_contact` | `jid` | Retrieve metadata for a single contact by JID |

### Group Tools (4 tools)

| Tool | Parameters | Description |
|------|------------|-------------|
| `list_groups` | None | List all group chats the account belongs to |
| `get_group` | `jid` | Fetch full group metadata including participant list and admin flags |
| `send_group_message` | `jid`, `text`, `quoted_message_id?`, `quoted_participant?`, `mentions?` | Send a plain-text message to a group chat (`jid` must end with `@g.us`); supports optional `mentions` list |
| `send_auto_message` | `jid`, `text`, `quoted_message_id?`, `quoted_participant?`, `mentions?` | Send to any JID, auto-routing to individual or group by suffix (bare numbers are treated as individuals) |

### Media Tools (3 tools)

| Tool | Parameters | Description |
|------|------------|-------------|
| `send_media` | `to`, `file_path`, `caption?`, `media_type?` (auto-detect), `ptt?` (default False) | Send image, video, audio, or document to a contact or group |
| `download_media` | `message_id`, `chat_jid` | Download and decrypt media from a received message into `WORKSPACE_DIR` |
| `analyze_media` | `chat_jid`, `message_id` | Sample video frames + transcribe audio on the bridge; returns `frame_paths` / `transcription` / `duration` / `frame_count` |

### Search Tools (1 tool)

| Tool | Parameters | Description |
|------|------------|-------------|
| `semantic_search` | `query`, `chat_jid?`, `sender?`, `limit?` (default 10, bridge clamps 1-100) | Hybrid full-text + vector RRF search via `POST /api/search`; the query is embedded at query time by the bridge |

## Tool Details

### Message Format

All message tools accept recipient JIDs in two formats:

**Individual contacts:**
- Full JID: `923001234567@s.whatsapp.net` (preferred)
- Bare number: `923001234567` (auto-formatted)

**Group chats:**
- Group JID: `120363039783372408@g.us`

`send_message` and `send_auto_message` accept either form. `send_group_message` requires a `@g.us` JID and rejects anything else. `send_auto_message` inspects the JID suffix and routes individual versus group automatically.

### Timestamps

`check_new_messages` uses Unix milliseconds (13-digit timestamps, e.g., `1700000000000`). Pass `since=0` on the first call to retrieve recent history. `check_triggers` does not take a timestamp — it tracks per-chat watermarks server-side, so each call returns only messages unseen since the previous call (unless `dry_run=True`).

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

### Mentions

`send_message`, `send_group_message`, and `send_auto_message` accept an optional `mentions` list of JIDs (use phone JIDs, e.g. `["923224387030@s.whatsapp.net"]`). The message text should contain matching `@phone` placeholders (e.g. `@923224387030`) so WhatsApp renders them as display names.

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

### Downloading vs Analyzing Media

`download_media` only fetches and decrypts the raw attachment bytes to a file under `WORKSPACE_DIR`; the output directory is fixed internally and is not a tool parameter.

`analyze_media` is a higher-level operation: the bridge samples video frames to disk and transcribes the audio track, returning:

```json
{
  "frame_paths": ["/abs/path/frame_000.jpg", "..."],
  "transcription": "...",
  "duration": 42.0,
  "frame_count": 8
}
```

The tool returns this result verbatim — it does **not** read the frames. To inspect visual content, the caller must **Read** each absolute path in `frame_paths` (they are image files) and combine what they show with `transcription`. Analysis runs on the bridge and can take a few minutes for longer clips; the call blocks until it finishes (a longer per-call read timeout is applied for this route).

## Architecture

### Lifespan Pattern

The server uses FastMCP's lifespan context to manage the bridge client lifecycle:

1. **Startup**: `BridgeClient` is created and health-checked (5 attempts, 2s intervals)
2. **Tool Calls**: Each tool retrieves the client from `ctx.lifespan_context["bridge"]`
3. **Shutdown**: The client connection pool is cleanly closed

If the bridge is not reachable during startup, the server fails immediately with a clear error message rather than confusing tool-level errors.

### Telemetry Middleware

A single `TelemetryMiddleware` (registered on the server in `server.py`) wraps every `tools/call`. For each invocation it records the tool name, wall-clock duration in milliseconds, success flag, and error message to the bridge via `POST /api/telemetry/tool`. This is fire-and-forget: timing and recording are handled in one cross-cutting place (no per-tool boilerplate), the tool's return value is never altered, tool exceptions propagate unchanged, and any telemetry failure is swallowed so it can never break a tool call.

### HTTP Retries

- **GET requests**: Retried up to 3 times on transient network errors (ConnectError, TimeoutException) using exponential backoff (0.5s–5s)
- **POST requests**: Never retried to prevent duplicate messages, media uploads, reactions, edits, or revokes

### Send Throttling

Outbound send operations (`send_message`, `send_group_message`, `send_auto_message`, `send_media`, `send_reaction`, `edit_message`, `revoke_message`) are throttled with a randomized 2-7 second delay between consecutive sends to avoid WhatsApp anti-spam detection. Read-only and telemetry POSTs (`semantic_search`, `check_triggers`, `analyze_media`, telemetry) bypass the throttle.

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
    ├── main.py                             Entry point (logging, print patching, transport selection, server startup)
    ├── server.py                           FastMCP server definition, lifespan, telemetry middleware, tool registration
    ├── middleware.py                       TelemetryMiddleware (per-tool-call telemetry to the bridge)
    ├── bridge_client.py                    HTTP client for the Go bridge (retries, timeouts, send throttle)
    └── tools/
        ├── __init__.py
        ├── messages.py                     Message tools (send, check, get, unread, group context, reaction, edit, revoke, triggers)
        ├── contacts.py                     Contact tools (list, get)
        ├── groups.py                       Group tools (list, get, send, auto-send)
        ├── media.py                        Media tools (send, download, analyze)
        └── search.py                       Search tools (semantic_search)
```

## Development

### Run Tests

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
