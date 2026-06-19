# WhatsApp MCP Server

A self-hosted WhatsApp **data platform** with a Model Context Protocol (MCP)
interface. A Go bridge captures every WhatsApp event into PostgreSQL, Python
enrichment workers add embeddings and transcriptions, and a Python FastMCP
server exposes the data and send actions to any MCP/LLM host.

## Architecture

The system is organized into an explicit **6-layer (L1–L6)** model. Full,
evidence-grounded detail is in [`docs/architecture-layers.md`](docs/architecture-layers.md);
a concise overview is in [`ARCHITECTURE.md`](ARCHITECTURE.md); the contributor
layer convention is in [`CLAUDE.md`](CLAUDE.md).

```
+----------------------+   REST / HTTP   +-----------------------------+
|  Python FastMCP (L4) |<--------------->|   Go WhatsApp Bridge (L2)   |
|  - 19 MCP tools      |  :8080          |   - whatsmeow / QR auth     |
|  - stdio / SSE / HTTP|                 |   - 24-route HTTP API       |
|  - async httpx       |                 |   - reconnection + backoff  |
+----------------------+                 |   - single-writer store     |
          ▲                              +--------------+--------------+
          │ MCP                                         │ SQL
   MCP/LLM host (L5)                        +-----------▼-------------------+
   Claude Code / Desktop                    |  PostgreSQL 17 + pgvector (L3)|
   or any MCP client                        +-----------▲-------------------+
                                                        │ poll / enrich
                              +-------------------------+------------------------+
                              │  embedder (L3)   transcriber (L3)   dashboard(L6)│
                              │  vector(384)     Whisper STT        :9090 read-only
                              +--------------------------------------------------+
```

The Go bridge connects to WhatsApp via the [whatsmeow](https://github.com/tulir/whatsmeow)
library and exposes a 24-route REST API (`127.0.0.1:8080` standalone). The Python
MCP server talks to the bridge over `httpx` and exposes **19 tools** over the MCP
**stdio**, **SSE**, or **streamable-HTTP** transport (selected via
`MCP_TRANSPORT`). Messages, contacts, links, telemetry, and embeddings are stored
in **PostgreSQL 17 + pgvector** — the single source of truth.

### Components

| Component | Layer | Role |
|-----------|-------|------|
| `bridge/` | L2 / L3 | Go bridge: capture, HTTP API, single-writer PostgreSQL store, hybrid (FTS + pgvector RRF) search |
| `mcp-server/` | L4 | Python FastMCP server: 19 tools, stdio / SSE / HTTP, per-tool telemetry |
| `embedder/` | L3 | Worker: embeds messages into `vector(384)` (sentence-transformers MiniLM) |
| `transcriber/` | L3 | Worker: transcribes voice notes via Whisper, re-downloads missing media |
| `dashboard/` | L6 | Read-only FastAPI + HTMX ops view on `:9090` |
| `.github/workflows/nightly-deps.yml` | L6 | Nightly dependency-refresh CI |

## Prerequisites

- Go 1.25+
- Python 3.11+
- [uv](https://astral.sh/uv) (Python package manager)
- PostgreSQL 17 with the `pgvector` extension (the bridge runs golang-migrate at startup)
- Docker + Docker Compose (recommended — provisions PostgreSQL, both services, the workers, and the dashboard)
- A WhatsApp account with multi-device support
- A Whisper-compatible STT endpoint (optional; only needed for voice-note transcription)

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

19 tools across five modules (`mcp-server/src/whatsapp_mcp/tools/`).

| Tool | Description |
|------|-------------|
| `send_message` | Send a text message to an individual contact; supports optional `mentions` |
| `send_group_message` | Send a text message to a group chat; supports optional `mentions` |
| `send_auto_message` | Send to any JID, auto-routing to individual or group by suffix |
| `check_new_messages` | Poll for new messages since a timestamp |
| `check_triggers` | Batched per-chat trigger check using server-side watermarks |
| `get_messages` | Retrieve recent messages from a chat |
| `get_group_context` | Compact recent-turn window for a group chat |
| `get_unread_chats` | Get all chats with unread messages |
| `get_unread_messages` | Get a flat list of all unread messages |
| `send_reaction` | React to a message with an emoji (empty string removes it) |
| `edit_message` | Edit a previously sent message |
| `revoke_message` | Revoke (delete for everyone) a message |
| `list_contacts` | List all known WhatsApp contacts |
| `get_contact` | Get metadata for a single contact |
| `list_groups` | List all group chats |
| `get_group` | Get group metadata with participants |
| `send_media` | Send image, video, audio, or document |
| `download_media` | Download media from a received message |
| `semantic_search` | Hybrid (full-text + vector RRF) message search, routed through the bridge |

## Enrichment & Storage

- **PostgreSQL 17 + pgvector** is the single source of truth. Schema is managed
  by golang-migrate `.sql` migrations (`bridge/migrations/000001`–`000010`),
  applied automatically at bridge startup.
- **Embedder** (`embedder/`) polls `messages.embedded_at IS NULL`, encodes with
  sentence-transformers (`vector(384)`), and writes `message_embeddings`.
- **Transcriber** (`transcriber/`) polls `messages_media` audio, transcribes via
  a Whisper endpoint, and re-downloads missing media through the bridge.
- Search is hybrid: PostgreSQL full-text + pgvector, fused with Reciprocal Rank
  Fusion in the bridge store.

Workers integrate with the bridge **only through the database** (poll-and-update
on NULL/watermark columns) — there is no message bus.

## Observability

- **Dashboard** (`dashboard/`) — a read-only FastAPI + HTMX view on
  <http://localhost:9090>, showing bridge health, recent messages, links, and
  per-tool telemetry. Provisioned by Docker Compose.
- **Telemetry** — daily counters (`telemetry_daily`) and per-tool latency/outcome
  (`telemetry_tool_calls`), recorded by the MCP server's telemetry middleware.
- **CI** — `.github/workflows/nightly-deps.yml` refreshes dependencies nightly.

## Connection Handling

The Go bridge implements a connection state machine:
- Exponential backoff reconnection (1s to 60s)
- Handles `TemporaryBan`, `ClientOutdated`, `ConnectFailure`, and `StreamReplaced` events
- Cooldown marker (10 min, exit code 2) prevents supervisor restart loops after bans
- Keepalive monitoring with forced reconnect after 3 consecutive timeouts
- Outbound send routes are rate-limited (token bucket) and never retried

## License

MIT License. See [LICENSE](LICENSE) for details.
