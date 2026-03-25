# WhatsApp MCP Server

A Model Context Protocol (MCP) server for WhatsApp, built with a Go bridge and Python FastMCP.

## Architecture

```
+-------------------------+    REST / HTTP    +----------------------------+
|    Python FastMCP       |<----------------->|    Go WhatsApp Bridge      |
|    (MCP Server)         |  localhost:8080   |    (whatsmeow)             |
|                         |                   |                            |
|  - 12 MCP tools         |                   |  - WhatsApp Web protocol   |
|  - stdio or SSE         |                   |  - SQLite message store    |
|    transport            |                   |  - QR code auth            |
|  - async httpx          |                   |  - Reconnection + backoff  |
|                         |                   |  - Media upload/download   |
+-------------------------+                   +----------------------------+
    Stdio: Claude Code                     Started independently
    (.mcp.json) or Claude                 (manual / systemd / Docker)
    Desktop. SSE: Docker                  (needs terminal for QR)
```

The Go bridge connects to WhatsApp via the [whatsmeow](https://github.com/tulir/whatsmeow) library and exposes a REST API on `127.0.0.1:8080`. The Python MCP server communicates with the bridge via `httpx` and exposes 12 tools over the MCP stdio transport or SSE transport (for Docker).

## Prerequisites

- Go 1.25+
- Python 3.11+
- [uv](https://astral.sh/uv) (Python package manager)
- Docker (optional, for containerized deployment)
- A WhatsApp account with multi-device support

## Setup

### 1. Build and run the Go bridge

```bash
cd bridge
go build -o whatsapp-bridge .
./whatsapp-bridge
```

On first run, scan the QR code with your WhatsApp mobile app (Settings → Linked Devices → Link a Device).

The bridge stores session data in `bridge/data/`. Subsequent runs reconnect automatically without QR.

Environment variables:
- `BRIDGE_ADDR` (default: `127.0.0.1:8080`)
- `BRIDGE_DATA_DIR` (default: `./data`)
- `BRIDGE_LOG_LEVEL` (default: `info`)

### 2. Configure the MCP server

Add to your `.mcp.json` (Claude Code) or `claude_desktop_config.json` (Claude Desktop):

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

Environment variables:
- `BRIDGE_URL` (default: `http://localhost:8080`)

### Docker (Recommended)

The easiest way to run both services:

```bash
docker compose up -d
```

On first run, check bridge logs for the QR code:

```bash
docker compose logs bridge
```

Scan the QR code with WhatsApp (Settings → Linked Devices → Link a Device).

Configure your MCP client to connect via SSE:

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

Environment variables can be set in `.env` next to `docker-compose.yml`:

```bash
WORKSPACE_PATH=/path/to/shared/directory
```

## MCP Tools

| Tool | Description |
|------|-------------|
| `send_message` | Send a text message to an individual contact; supports optional `mentions` for @-mentions |
| `send_group_message` | Send a text message to a group chat; supports optional `mentions` for @-mentions |
| `check_new_messages` | Poll for new messages since a timestamp |
| `get_messages` | Retrieve recent messages from a chat |
| `get_unread_chats` | Get all chats with unread messages |
| `get_unread_messages` | Get a flat list of all unread messages |
| `list_contacts` | List all known WhatsApp contacts |
| `get_contact` | Get metadata for a single contact |
| `list_groups` | List all group chats |
| `get_group` | Get group metadata with participants |
| `send_media` | Send image, video, audio, or document |
| `download_media` | Download media from a received message |

## Connection Handling

The Go bridge implements connection state machine:
- Exponential backoff reconnection (1s to 60s)
- Handles `TemporaryBan`, `ClientOutdated`, `ConnectFailure`, and `StreamReplaced` events
- Cooldown marker prevents supervisor restart loops after bans
- Keepalive monitoring with forced reconnect after 3 consecutive timeouts

## License

MIT License. See [LICENSE](LICENSE) for details.
