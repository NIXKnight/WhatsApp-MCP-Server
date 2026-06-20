# WhatsApp MCP Server

A self-hosted WhatsApp **data platform** with a Model Context Protocol (MCP)
interface. A Go bridge captures every WhatsApp event into PostgreSQL, Python
enrichment workers add embeddings and transcriptions, and a Python FastMCP
server exposes the data and send actions to any MCP/LLM host.

---

## Architecture

### 6-Layer Model

```
L1  External Edge
    go.mau.fi/whatsmeow  (WhatsApp protocol; external dependency)

L2  Capture & Gateway
    bridge/client/        event handler, QR auth, send
    bridge/connection/    state machine, keepalive, backoff
    bridge/api/           25-route HTTP API (chi router)
    bridge/media/         upload, download, OGG/Opus analysis
    bridge/indexer/       URL/link extraction
    bridge/config/        env-based configuration
    bridge/embed/         outbound client -> L3 embedder
    bridge/analyzer/      outbound client -> L3 transcriber

L3  Persistence & Enrichment
    bridge/store/         PostgreSQL read/write via pgx pool
    bridge/migrations/    9 golang-migrate pairs (001, 003-010)
    embedder/             poll worker + on-demand /embed server
    transcriber/          poll worker + on-demand /analyze server

L4  Tool / Contract
    mcp-server/           Python FastMCP: 20 tools, stdio/SSE/HTTP

L5  Intelligence & Control
    Claude Code / Desktop / any MCP client  (external)

L6  Operations & Observability
    docker-compose.yml    5-service stack
    dashboard/            FastAPI + HTMX read-only view
    .github/workflows/    nightly dependency CI
    Makefile / scripts/   build, systemd, deploy
```

### Data-Flow Diagrams

**Inbound capture path (L1 -> L3)**

```
WhatsApp network
      |
      | (encrypted protocol)
      v
  whatsmeow (L1)
      |  events.Message / events.HistorySync / ...
      v
  bridge/client/events.go  (L2: event handler)
      |
      |-- INSERT messages, chats, contacts
      |-- INSERT messages_media  (media rows, local_path NULL)
      v
  PostgreSQL (L3)
      |
      |-- embedder polls (embedded_at IS NULL, content <> '')
      |       model: paraphrase-multilingual-MiniLM-L12-v2
      |       writes message_embeddings.embedding vector(384)
      |
      |-- transcriber polls (messages_media, audio, transcribed_at IS NULL)
      |       calls whisper.cpp /inference
      |       writes messages_media.transcription
      v
  Enriched persistent store
```

**Outbound tool path (L5 -> L1)**

```
Claude / MCP client  (L5)
      |
      | MCP (stdio / SSE / streamable-HTTP)
      v
  FastMCP server: mcp-server/  (L4)
      |
      |  httpx -> bridge REST
      v
  bridge/api/  (L2, :8080)
      |
      |-- send / reaction / edit / revoke -> whatsmeow -> WhatsApp (L1)
      |
      |-- semantic_search flow:
      |     POST /api/search {query: "..."}
      |       bridge calls POST http://embedder:8000/embed  (L3 embedder)
      |           <- {embedding: [384 floats]}
      |       bridge runs hybrid RRF SQL (FTS + pgvector) in PostgreSQL
      |       <- ranked SearchResult list
      |
      |-- analyze_media flow:
      |     MCP: analyze_media(chat_jid, message_id)
      |       POST /api/media/analyze  (L2, thin proxy, 190s timeout)
      |         POST http://transcriber:8500/analyze  (L3 transcriber)
      |           bridge /api/download -> WhatsApp CDN -> disk
      |           ffmpeg: keyframes (1 fps, max 8) + 16kHz mono WAV
      |           whisper.cpp /inference -> transcript
      |         <- {frame_paths, transcription, duration, frame_count}
      v
  Response returned to MCP client
```

---

## Components

| Component | Layer | Role |
|---|---|---|
| `bridge/` | L2 / L3 | Go bridge: QR auth, event capture, 25-route HTTP API, single-writer PostgreSQL store, hybrid FTS + pgvector RRF search |
| `mcp-server/` | L4 | Python FastMCP server: 20 tools, stdio / SSE / streamable-HTTP, per-tool telemetry middleware |
| `embedder/` | L3 | Poll worker + on-demand HTTP server: embeds messages into `vector(384)` using `paraphrase-multilingual-MiniLM-L12-v2` |
| `transcriber/` | L3 | Poll worker + on-demand HTTP server: transcribes voice notes via whisper.cpp, on-demand video frame extraction via ffmpeg |
| `dashboard/` | L6 | Read-only FastAPI + HTMX ops view, Tailwind v3.4 / Tremor design language, dark/light toggle, self-hosted FontAwesome |
| `.github/workflows/nightly-deps.yml` | L6 | Nightly dependency-refresh CI for Go + all 4 Python projects |

---

## Prerequisites

- Go 1.25+
- Python 3.11+
- [uv](https://astral.sh/uv) (Python package manager)
- PostgreSQL 17 with the `pgvector` extension (the bridge runs migrations at startup)
- Docker + Docker Compose (recommended — provisions the full stack)
- A WhatsApp account with multi-device support
- A running whisper.cpp `whisper-server` (optional; required only for voice-note transcription and `analyze_media`)

---

## Setup

### Option A: Docker Compose (recommended)

All six services start with a single command. PostgreSQL, the bridge, the MCP
server, the embedder, the transcriber, and the dashboard are all provisioned
automatically.

```bash
# Build and start everything
docker compose up -d

# On first run: watch bridge logs for the QR code
docker compose logs -f bridge
# Scan the QR code in WhatsApp: Settings -> Linked Devices -> Link a Device

# Check all services are healthy
docker compose ps
```

Connect your MCP client via SSE (MCP server listens on port 3000):

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

Optional environment overrides in an `.env` file next to `docker-compose.yml`:

```
# Shared directory visible to both the bridge and MCP server
WORKSPACE_PATH=/path/to/your/workspace

# Embedder tuning
EMBEDDING_MODEL=paraphrase-multilingual-MiniLM-L12-v2
EMBED_BATCH_SIZE=100
EMBED_POLL_INTERVAL=30

# Transcriber tuning
WHISPER_URL=http://host.docker.internal:8443
WHISPER_MODEL=large-v3
WHISPER_LANGUAGE=ur
TRANSCRIBE_BATCH_SIZE=20
TRANSCRIBE_POLL_INTERVAL=30
```

### Option B: Bare-metal with systemd

The `Makefile` and `scripts/` handle build, venv setup, and systemd unit
creation. Each component gets its own user-level systemd service.

```bash
# Build and install the Go bridge
make bridge

# Sync all Python venvs (mcp-server, embedder, transcriber, dashboard)
make mcp-server workers

# Create and enable systemd units for all components
make bridge-unit mcp-unit embedder-unit transcriber-unit dashboard-unit

# Optional: whisper.cpp server unit
make whisper-unit

# Start the full stack
make start

# Check status of all services
make status

# Tail all logs
make logs
```

Systemd units are placed in `~/.config/systemd/user/` and named:
- `whatsapp-bridge.service`
- `whatsapp-mcp-server.service`
- `whatsapp-embedder.service`
- `whatsapp-transcriber.service`
- `whatsapp-dashboard.service`
- `whatsapp-whisper.service`

Each unit reads an optional env file at `~/.config/whatsapp-bridge/env`; place
`DATABASE_URL` and other secrets there.

### Option C: stdio transport (Claude Code / Desktop)

Add to your `.mcp.json` (Claude Code) or `claude_desktop_config.json` (Claude
Desktop):

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

The bridge must be running separately before the MCP server starts.

---

## Bridge Configuration

All bridge settings are read from environment variables at startup.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL DSN. Example: `postgres://user:pass@localhost:5432/whatsapp?sslmode=disable` |
| `BRIDGE_ADDR` | `127.0.0.1:8080` | HTTP bind address. Warning logged if not loopback — the API has no auth. |
| `BRIDGE_DATA_DIR` | `./data` | Directory for downloaded media files. |
| `BRIDGE_LOG_LEVEL` | `info` | Log verbosity: `debug`, `info`, `warn`, `error`. |
| `EMBEDDER_URL` | `http://embedder:8000` | Base URL of the L3 embedder (query-time embedding). |
| `ANALYZER_URL` | `http://transcriber:8500` | Base URL of the L3 transcriber (on-demand media analysis). |

---

## MCP Server Configuration

| Variable | Default | Description |
|---|---|---|
| `BRIDGE_URL` | `http://localhost:8080` | Bridge REST base URL. |
| `MCP_TRANSPORT` | `stdio` | Transport: `stdio`, `sse`, or `http` (streamable-HTTP). |
| `MCP_HOST` | `0.0.0.0` | Bind host (SSE / HTTP transports only). |
| `MCP_PORT` | `3000` | Bind port (SSE / HTTP transports only). |
| `WORKSPACE_DIR` | `/workspace` | Container path of the shared workspace mount. |
| `WORKSPACE_HOST_PATH` | (unset) | Host path corresponding to `WORKSPACE_DIR`; used to translate file paths for `send_media`. |
| `LOG_LEVEL` | `INFO` | Python log verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR`. |

---

## MCP Tools

20 tools across five modules (`mcp-server/src/whatsapp_mcp/tools/`).
Source: `@mcp.tool` decorator count — messages.py:10, groups.py:4, media.py:3,
contacts.py:2, search.py:1.

| Tool | Module | Description |
|---|---|---|
| `send_message` | messages | Send plain-text to an individual; supports quote, mentions |
| `check_new_messages` | messages | Poll for messages since a Unix ms timestamp |
| `get_messages` | messages | Retrieve recent messages from a chat |
| `get_unread_chats` | messages | List all chats with unread messages and previews |
| `get_unread_messages` | messages | Flat list of all unread messages across all chats |
| `get_group_context` | messages | Compact recent-turn window for a group (strips own messages) |
| `send_reaction` | messages | React to a message with an emoji; empty string removes it |
| `edit_message` | messages | Edit the text of a previously sent message |
| `revoke_message` | messages | Revoke (delete for everyone) a message |
| `check_triggers` | messages | Batch trigger check across multiple chats using server-side watermarks |
| `list_groups` | groups | List all group chats the account belongs to |
| `get_group` | groups | Full group metadata including participant list |
| `send_group_message` | groups | Send plain-text to a group; supports quote, mentions |
| `send_auto_message` | groups | Send to any JID — auto-routes to individual or group by suffix |
| `send_media` | media | Send image, video, audio, or document; auto-detects type from extension |
| `download_media` | media | Download media from a received message to the workspace directory |
| `analyze_media` | media | Sample video frames (ffmpeg) + transcribe audio (Whisper); returns frame paths + transcript |
| `list_contacts` | contacts | Return all contacts from the bridge store |
| `get_contact` | contacts | Metadata for a single contact by JID |
| `semantic_search` | search | Hybrid (full-text + vector RRF) message search; query is embedded at query time by the bridge |

---

## Bridge HTTP API

25 routes registered in `bridge/api/server.go`. The API binds to
`127.0.0.1:8080` by default. It has **no authentication** and must not be
exposed off-host.

```
GET    /api/status
GET    /api/messages
GET    /api/messages/{id}/context
GET    /api/messages/{id}/similar
PUT    /api/messages/{id}/embedding
POST   /api/search
GET    /api/chats
POST   /api/chats/search
GET    /api/chats/{jid}
GET    /api/contacts
GET    /api/contacts/similar
GET    /api/contacts/{jid}
GET    /api/groups
GET    /api/groups/{jid}
GET    /api/unread
GET    /api/check
POST   /api/check/triggers
POST   /api/download
POST   /api/telemetry/tool
POST   /api/media/analyze    (190s route timeout; proxies L3 transcriber)
POST   /api/send             (rate-limited: ~2/s sustained, burst 5)
POST   /api/send/media       (rate-limited)
POST   /api/send/reaction    (rate-limited)
POST   /api/send/edit        (rate-limited)
POST   /api/send/revoke      (rate-limited)
```

---

## Enrichment & Storage

### PostgreSQL + pgvector

PostgreSQL 17 with the `pgvector` extension is the single source of truth.
The bridge runs [golang-migrate](https://github.com/golang-migrate/migrate)
migrations at startup automatically.

Migrations: `bridge/migrations/000001`, `000003`–`000010` (9 pairs; no `000002`).

Key tables:

| Table | Purpose |
|---|---|
| `chats` | Chat metadata: JID, name, group flag, unread count |
| `contacts` | Contact name, notify, phone |
| `messages` | All messages with FTS column (`content_fts tsvector`) and `embedded_at` watermark |
| `messages_media` | Media metadata: URL, local path, transcription, download tracking |
| `message_embeddings` | `embedding vector(384)` per message |
| `links` | URL/link index extracted from messages |
| `telemetry_daily` | Daily counters: sent, received, media, links |
| `telemetry_tool_calls` | Per-tool name, duration_ms, success, error |
| `watermarks` | Per-chat `last_seen` for `check_triggers` |

### Embedder (L3)

Located in `embedder/`. Runs two concurrent functions:

1. **Poll loop** — reads `messages` where `embedded_at IS NULL AND content <> ''`,
   encodes in batches, writes `message_embeddings.embedding vector(384)`, stamps
   `messages.embedded_at`.
2. **On-demand HTTP server** — listens on `EMBED_HTTP_ADDR` (default
   `127.0.0.1:8000`):
   - `POST /embed`   `{"text":"..."}` -> `{"embedding":[384 floats]}`
   - `GET  /health`  -> `{"status":"ok","model":"...","dim":384}`

Model: `paraphrase-multilingual-MiniLM-L12-v2` (sentence-transformers).
Dimension: 384 — matches the `vector(384)` schema column.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL DSN |
| `EMBEDDING_MODEL` | `paraphrase-multilingual-MiniLM-L12-v2` | sentence-transformers model name |
| `EMBED_BATCH_SIZE` | `100` | Messages per embedding batch |
| `EMBED_POLL_INTERVAL` | `30` | Seconds between idle polls |
| `EMBED_HTTP_ADDR` | `127.0.0.1:8000` | Bind address for on-demand /embed + /health |

### Transcriber (L3)

Located in `transcriber/`. Runs two concurrent functions:

1. **Poll loop** — reads `messages_media` where `media_type = 'audio' AND
   transcribed_at IS NULL`, re-downloads missing files via the bridge if needed,
   posts raw audio to whisper.cpp `/inference`, writes `messages_media.transcription`.
2. **On-demand HTTP server** — listens on `ANALYZER_HTTP_ADDR` (default
   `127.0.0.1:8500`):
   - `POST /analyze` `{"chat_jid":"...","message_id":"..."}` ->
     `{"frame_paths":[...],"transcription":"...","duration":<float>,"frame_count":<int>}`
   - `GET  /health`  -> `{"status":"ok"}`

The `/analyze` handler downloads the video via the bridge, extracts up to 8 keyframes
at 1 fps with ffmpeg, demuxes audio to 16 kHz mono WAV, and transcribes with
Whisper. Videos longer than 600 seconds are rejected.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL DSN |
| `BRIDGE_URL` | `http://bridge:8080` | Bridge base URL (for media re-download) |
| `WHISPER_URL` | `http://127.0.0.1:8443` | whisper.cpp server base URL; audio posted to `{WHISPER_URL}/inference` |
| `WHISPER_MODEL` | `large-v3` | Informational only; the model is loaded by whisper-server |
| `WHISPER_LANGUAGE` | `ur` | Language hint (compose default) |
| `TRANSCRIBE_BATCH_SIZE` | `20` | Rows per cycle |
| `TRANSCRIBE_POLL_INTERVAL` | `30` | Seconds between idle polls |
| `ANALYZER_HTTP_ADDR` | `127.0.0.1:8500` | Bind address for on-demand /analyze + /health |

### Hybrid Search

`POST /api/search` accepts `{"query":"...","chat_jid":"...","limit":20}`.

When the caller supplies only `query` text, the bridge calls
`POST http://embedder:8000/embed` to obtain a query vector at query time,
then runs hybrid Reciprocal Rank Fusion over:
- PostgreSQL full-text search (`plainto_tsquery('simple', ...)` on `content_fts`)
- pgvector cosine distance (`<=>`) on `message_embeddings`

If the embedder is unreachable (2-second timeout), search degrades to FTS-only
without erroring the request. Results carry a `match_type` field: `"fts"`,
`"semantic"`, or `"both"`.

---

## Media Analysis (`analyze_media`)

Full flow for the `analyze_media` MCP tool:

```
MCP client calls analyze_media(chat_jid, message_id)
  |
  v
mcp-server POST /api/media/analyze  (L4 -> L2)
  |
  v
bridge/api/handlers_analyze.go  (thin proxy, 190s chi timeout)
  |
  v
bridge/analyzer/client.go  POST http://transcriber:8500/analyze
  |
  v
transcriber/worker.py  analyze_video()
  |-- bridge POST /api/download -> WhatsApp CDN -> disk
  |-- ffprobe duration gate (max 600s)
  |-- ffmpeg: up to 8 keyframe JPEGs at 1 fps
  |-- ffmpeg: 16kHz mono WAV
  |-- POST {WHISPER_URL}/inference -> transcript
  |
  v
{"frame_paths":["/abs/path/frame_001.jpg",...],
 "transcription":"...",
 "duration":42.0,
 "frame_count":8}
```

The MCP tool returns the result verbatim. The LLM host must **Read** each path
in `frame_paths` (they are absolute image files) to inspect visual content.

### Voice Note Duration Fix

`bridge/media/ogg.go` parses the OGG Opus container in-process (no external
commands) to derive the real audio duration and a 64-sample waveform from the
granule position. Previously sent voice notes reported a hardcoded 30-second
duration; they now report the real duration.

---

## Observability

### Dashboard

`dashboard/` is a FastAPI + HTMX application running on port 9090 (default).
It is read-only: all SQL is SELECT-only via psycopg2.

**Recent restyle:** Tailwind v3.4 built with the standalone Tailwind CLI (no
Node required), Tremor design language color tokens, dark/light mode toggle with
no-flash script (respects OS preference and `localStorage.theme`), self-hosted
FontAwesome icons served from `/static/fontawesome/`.

Views and HTMX partials (auto-refresh every 30 seconds):

| Path | Content |
|---|---|
| `GET /` | Full page: bridge status, groups, recent messages, telemetry, links, tool call history |
| `GET /partials/status` | Bridge connection state |
| `GET /partials/messages` | Recent messages table |
| `GET /partials/telemetry` | Daily counters |

Dashboard env vars:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL DSN |
| `BRIDGE_URL` | `http://127.0.0.1:8080` | Bridge base URL for status polling |
| `DASHBOARD_HOST` | `127.0.0.1` | Bind host |
| `DASHBOARD_PORT` | `9090` | Bind port |

### Telemetry Tables

The MCP server's `TelemetryMiddleware` records every tool call to
`POST /api/telemetry/tool` on the bridge, which writes to PostgreSQL:

- `telemetry_daily` — daily rolling counters: `messages_sent`,
  `messages_received`, `media_downloaded`, `media_sent`, `links_indexed`.
- `telemetry_tool_calls` — per-call rows: `tool_name`, `duration_ms`,
  `success`, `error_msg`, `called_at`.

### CI

`.github/workflows/nightly-deps.yml` runs at 03:00 UTC daily. It updates Go
dependencies (`go get -u ./... && go mod tidy`), builds and vets the bridge,
and runs `uv lock --upgrade` for each Python project (mcp-server, embedder,
transcriber, dashboard). Passing updates are opened as pull requests.

---

## Connection Handling

The Go bridge implements a supervised connection state machine in
`bridge/connection/`:

- Exponential backoff reconnection: 1s, 2s, 4s, 8s, 16s, 32s, 60s (repeating).
- Handles `TemporaryBan`, `ClientOutdated`, `ConnectFailure`, and
  `StreamReplaced` events from whatsmeow.
- Cooldown marker (10-minute file on disk, exit code 2) prevents supervisor
  restart loops after bans.
- Keepalive monitor (`bridge/connection/keepalive.go`) forces reconnect after 3
  consecutive timeouts.
- Outbound send routes are rate-limited (token-bucket: ~2 sends/s sustained,
  burst 5, up to 300 ms human-timing jitter) and are never retried.

---

## Build Verification

```bash
# Go (L2/L3)
cd bridge && go build ./... && go vet ./...

# Python syntax check (L3/L4/L6)
python -m py_compile <path/to/changed/file.py>
```

---

## Contributor Guide

See [`CLAUDE.md`](CLAUDE.md) for the 6-layer model, the directory-to-layer map,
where to place new code, and the hard rules (PostgreSQL-only, schema
bridge-owned, no DB driver in L4, send routes never retried).

---

## Acknowledgements

This work is inspired by the open-source
[`asimzeeshan/WhatsApp-bridge`](https://github.com/asimzeeshan/WhatsApp-bridge),
reimplemented and extended for this stack.

## License

MIT License. See [LICENSE](LICENSE) for details.
