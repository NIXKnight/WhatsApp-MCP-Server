# WhatsApp Bridge

Go HTTP server (layers L2/L3) that connects to WhatsApp via [whatsmeow](https://go.mau.fi/whatsmeow), captures every event into PostgreSQL, and exposes a REST API on `127.0.0.1:8080`. The Python FastMCP server consumes this API to deliver 20 MCP tools to any MCP/LLM host; Python enrichment workers (embedder, transcriber) read and write the same database. This bridge owns the schema and is the single writer to PostgreSQL.

## Overview

The bridge wraps the [whatsmeow](https://go.mau.fi/whatsmeow) library (WhatsApp Web multi-device protocol) and persists messages, chats, contacts, media metadata, links, telemetry, and watermarks into **PostgreSQL with the `pgvector` extension**. It implements a supervised connection state machine with exponential backoff, QR-code pairing, keepalive monitoring, and a post-ban cooldown. On top of capture it provides hybrid full-text + vector search, a background media-retry worker, a per-tool telemetry sink, and a thin media-analysis proxy to the L3 transcriber.

Security: the API has **no authentication** and must only be reachable on the loopback interface (`127.0.0.1` / `localhost`) or a compose-internal network. The bridge logs a warning at startup if `BRIDGE_ADDR` is not loopback.

## Prerequisites

- Go 1.25 or later.
- PostgreSQL 17 with the `pgvector` extension (the bridge runs its migrations automatically at startup).
- A WhatsApp account with multi-device support enabled.

No CGO, no SQLite, and no system libraries are required: the binary builds pure-Go with `CGO_ENABLED=0`. (`mattn/go-sqlite3` appears only as an indirect, unused transitive dependency and is never compiled in.)

## Build and Run

### Build

```bash
cd bridge
CGO_ENABLED=0 go build -o whatsapp-bridge .
```

### Run

```bash
DATABASE_URL='postgres://user:pass@localhost:5432/whatsapp?sslmode=disable' ./whatsapp-bridge
```

`DATABASE_URL` is required; the process exits immediately if it is unset. On first run the bridge applies all pending migrations, then enters QR pairing mode. Scan the displayed QR code with your WhatsApp mobile app (Settings -> Linked Devices -> Link a Device).

After the QR scan succeeds the bridge stays connected. Subsequent runs reconnect automatically without re-pairing because the whatsmeow device session is persisted in PostgreSQL (not in a local file). Only downloaded media bytes live on disk, under `BRIDGE_DATA_DIR`.

### Docker Build

```bash
docker compose build bridge
```

The Dockerfile is a multi-stage build: a `golang:1.25-alpine` builder compiles the binary with `CGO_ENABLED=0`, and the runtime stage is a minimal `alpine:3.21` image carrying only the static binary plus `ca-certificates` and `tzdata`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (required) | PostgreSQL DSN. Example: `postgres://user:pass@host:5432/whatsapp?sslmode=disable`. The process exits if unset. |
| `BRIDGE_ADDR` | `127.0.0.1:8080` | TCP bind address for the HTTP server. A warning is logged if this is not a loopback address — the API has no auth. |
| `BRIDGE_DATA_DIR` | `./data` | Directory for **downloaded media files** (and the cooldown marker). Created if missing; must be writable. Session/device data lives in PostgreSQL, not here. |
| `BRIDGE_LOG_LEVEL` | `info` | Log verbosity: `debug`, `info`, `warn`, or `error`. Logs are JSON to stderr. |
| `EMBEDDER_URL` | `http://embedder:8000` | Base URL of the L3 embedder, called for query-time embeddings during hybrid search. |
| `ANALYZER_URL` | `http://transcriber:8500` | Base URL of the L3 transcriber, called by the `/api/media/analyze` proxy. |
| `MEDIA_RETRY_ENABLED` | `true` | Gates the background media-retry worker. When `false`, the worker goroutine never starts. |
| `MEDIA_RETRY_INTERVAL` | `3m` | Poll cadence of the media-retry worker. Doubles as the cross-cycle backoff between attempts for a single row. Go duration syntax (e.g. `90s`, `5m`). |
| `MEDIA_MAX_DOWNLOAD_ATTEMPTS` | `3` | Max re-download attempts for one media row before it is flagged `download_permanently_failed`. |
| `MEDIA_RETRY_BATCH_SIZE` | `10` | Maximum retry-eligible rows processed per cycle. |

The embedder timeout (2s) and analyzer timeout (180s) are compiled-in constants, not environment variables.

Example:

```bash
BRIDGE_LOG_LEVEL=debug BRIDGE_DATA_DIR=/tmp/whatsapp-media \
DATABASE_URL='postgres://user:pass@localhost:5432/whatsapp?sslmode=disable' \
./whatsapp-bridge
```

## REST API Endpoints

All endpoints return JSON. Errors return `{"error":"...","code":"..."}` with an appropriate HTTP status. Query parameters and request bodies use snake_case (e.g. `chat_jid`, `message_id`, `quoted_message_id`). The router is [chi](https://github.com/go-chi/chi); a global 60s request timeout applies to every route except `POST /api/media/analyze` (190s).

25 routes are registered in `bridge/api/server.go`:

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
POST   /api/media/analyze    (190s route timeout; thin proxy to the L3 transcriber)
POST   /api/send             (rate-limited: ~2/s sustained, burst 5, up to 300ms jitter)
POST   /api/send/media       (rate-limited)
POST   /api/send/reaction    (rate-limited)
POST   /api/send/edit        (rate-limited)
POST   /api/send/revoke      (rate-limited)
```

The five `POST /api/send*` routes share one token-bucket limiter (`NewRateLimiter(2.0, 5, 300)`): ~2 sends/second sustained, burst of 5, with up to 300ms of human-timing jitter. Send routes are **rate-limited and never retried** — WhatsApp penalises bursts of automated sends.

## QR Code Pairing Flow

1. **First Run**: the bridge runs migrations, then tries to load a whatsmeow device from PostgreSQL.
2. **No Device Found**: the bridge enters `StateQRWaiting`. A QR code is printed to the terminal (half-block Unicode, sized for small windows). `GET /api/status` returns `200` with `state=QR_WAITING` throughout, so health checks do not see "connection refused".
3. **Scan the Code**: open WhatsApp mobile, Settings -> Linked Devices -> Link a Device, and scan.
4. **Success**: the bridge logs the successful scan, transitions to `StateConnected`, and persists the session in PostgreSQL.
5. **Automatic Reconnect**: on restart the saved device is loaded from PostgreSQL and the bridge reconnects without a QR (unless the session was invalidated by a logout or ban).

## Connection State Machine

The bridge implements a supervised connection state machine in `bridge/connection/`:

```
DISCONNECTED -----> CONNECTING -----> CONNECTED
    ^                    |                 ^
    |                    v                 |
    +------------ QR_WAITING               |
                         |                 |
                         v                 |
                    (QR scanned)           |
                         |                 v
                         +---- CONNECTED <-+
                                  |
                                  v
                    PERMANENTLY_DISCONNECTED
                    (ban, logout, outdated client)
```

### Key Features

- **Exponential Backoff**: failed connections retry with delays of 1s, 2s, 4s, 8s, 16s, 32s, 60s (the schedule caps at 60s and repeats).
- **Cooldown Marker**: after a permanent disconnect (ban, logout, client outdated), a `cooldown` marker file is written under `BRIDGE_DATA_DIR` recording a 10-minute hold-off, and the process exits with **code 2**. On the next start, if the marker is still in the future, the bridge refuses to start and exits 2 again — signalling systemd/Docker restart policies to wait rather than hammer WhatsApp. A successful connection clears the marker.
- **Keepalive Monitoring**: a forced reconnect is triggered after **3 consecutive** `KeepAliveTimeout` events. whatsmeow's own auto-reconnect is disabled so the bridge runs a single, deterministic reconnect loop.
- **Event Handlers**: handle `Connected`, `Disconnected`, `LoggedOut`, `TemporaryBan`, `ClientOutdated`, `ConnectFailure`, and `StreamReplaced`.

## Data Storage

### PostgreSQL + pgvector

PostgreSQL is the single source of truth. The bridge opens one `pgx` pool (`stdlib.OpenDB`, max 10 open / 5 idle connections) and serialises every write through a single writer goroutine. The whatsmeow device store uses the same database via `sqlstore.New(ctx, "pgx", databaseURL, ...)`, so credentials and key material live in PostgreSQL — there are no `.db` files and no SQLite anywhere.

Schema is **bridge-owned**: only `bridge/migrations/` creates or alters tables, applied at startup with [golang-migrate](https://github.com/golang-migrate/migrate) (embedded via `iofs`). There are **9 migration pairs**: `000001` and `000003`–`000010` (no `000002`). Never edit a committed migration; add the next numbered pair.

Key tables and the materialized view:

| Object | Purpose |
|--------|---------|
| `chats` | Chat metadata: JID, name, group flag, unread count, optional `topic_embedding vector(384)`. |
| `contacts` | Contact name / notify / phone, optional `name_embedding vector(384)`. |
| `messages` | All messages, with a generated `content_fts tsvector` column (GIN-indexed) and an `embedded_at` watermark for the embedder. |
| `messages_media` | Media metadata: URL, keys, local path, `download_attempts`, `download_permanently_failed`, and `transcription` / `transcription_lang`. |
| `message_embeddings` | One `embedding vector(384)` row per message (dimension fixed by the `EmbeddingDim = 384` constant and migration `000006`). |
| `links` | URL/link index extracted from message content by `bridge/indexer/`. |
| `telemetry_daily` | Daily rolling counters (sent, received, media, links). |
| `telemetry_tool_calls` | Per-tool-call rows: `tool_name`, `duration_ms`, `success`, `error`. |
| `watermarks` | Per-chat `last_seen` cursor backing the `check_triggers` batch endpoint. |
| `unread_summary` | Materialized view (migration `000004`) summarising unread chats; refreshed by the store. |

There is **no `group_participants` table**; group participants are fetched live from whatsmeow when `GET /api/groups/{jid}` is called.

## Enrichment, Search, and Background Work

The database is the only integration substrate — there is no message bus. Cross-component hand-off is poll-and-update on NULL/watermark columns plus partial indexes.

### Hybrid search (`POST /api/search`)

`store/search.go` + `api/handlers_search.go` implement Reciprocal Rank Fusion (RRF) over two ranked lists: PostgreSQL full-text search (`plainto_tsquery('simple', ...)` over `content_fts`, with `ts_headline` snippets) and pgvector cosine distance (`<=>`) over `message_embeddings`. When the caller sends only `query` text, the bridge fetches a query vector from the embedder (`EMBEDDER_URL`, 2s timeout) and runs full RRF; if the embedder is unreachable it degrades to FTS-only instead of failing the request. Each hit carries `match_type` ∈ {`fts`, `semantic`, `both`}. Embeddings supplied directly on the request must be exactly 384 dimensions.

### Outbound enrichment clients

`bridge/embed/` is a thin client to the L3 embedder (used by hybrid search above). `bridge/analyzer/` is a thin client to the L3 transcriber, invoked by the `POST /api/media/analyze` proxy: that handler forwards `{chat_jid, message_id}` to the transcriber's `/analyze`, which downloads the video through the bridge, samples up to 8 keyframes with ffmpeg, transcribes audio with whisper.cpp, and returns frame paths + transcript. The route carries a 190s chi timeout (the rest of the server runs a 60s timeout) so analysis is bounded by the analyzer client, not the router.

### Media-retry worker (`bridge/mediaretry/`)

A bounded background goroutine (gated by `MEDIA_RETRY_ENABLED`) re-downloads **requested-but-failed non-audio media**. Scope is requested-only: a row is eligible only after an on-demand download already failed (`download_attempts > 0` and not permanently failed); captured-but-never-requested media is left untouched. Re-downloading needs the live whatsmeow session, so the worker starts only after the connection is established. The poll interval (`MEDIA_RETRY_INTERVAL`) is itself the cross-cycle backoff — a transient 403/410 is usually a stale signed URL that a later download refreshes — so each eligible row is retried at most once per cycle (no tight in-loop retry). After `MEDIA_MAX_DOWNLOAD_ATTEMPTS` the row is flagged `download_permanently_failed`. It integrates purely through the database and issues no DDL.

### Telemetry

`POST /api/telemetry/tool` accepts per-call records from the MCP server's telemetry middleware and writes them to `telemetry_tool_calls`, while capture and send paths bump the daily counters in `telemetry_daily`.

## Project Structure

```
bridge/
├── README.md                   This file
├── go.mod / go.sum             Module definition and checksums
├── main.go                     Entry point: config, cooldown marker, lifecycle, graceful shutdown
├── config/
│   └── config.go               Environment-variable parsing and validation
├── client/
│   ├── client.go               whatsmeow client init (pgx device store) and event dispatch
│   ├── events.go               Event handlers + capture (messages, chats, contacts, media)
│   └── send.go                 Outbound send / reaction / edit / revoke
├── connection/
│   ├── manager.go              Connect/reconnect loop, exponential backoff
│   ├── state.go                Connection state enum and accessors
│   └── keepalive.go            Keepalive timeout tracking (3-strike forced reconnect)
├── api/
│   ├── server.go               chi router, middleware, route table, rate limiter
│   ├── handlers.go             Handler struct + shared helpers
│   ├── handlers_messages.go    Messages, context, similar, embedding upsert
│   ├── handlers_chats.go       Chats list/get/topic-search
│   ├── handlers_contacts.go    Contacts list/get/similar
│   ├── handlers_groups.go      Group list/metadata (participants fetched live)
│   ├── handlers_search.go      POST /api/search (hybrid RRF)
│   ├── handlers_send.go        Send / reaction / edit / revoke
│   ├── handlers_download.go    Media download
│   ├── handlers_analyze.go     POST /api/media/analyze proxy
│   ├── handlers_telemetry.go   POST /api/telemetry/tool
│   ├── handlers_health.go      Status / health
│   ├── types.go                Request/response JSON structs
│   ├── jid.go                  JID validation
│   └── ratelimit.go            Token-bucket limiter for send routes
├── store/
│   ├── db.go                   pgx pool, golang-migrate runner, single-writer goroutine
│   ├── messages.go             Message queries
│   ├── chats.go                Chat queries
│   ├── contacts.go             Contact queries
│   ├── media.go                Media-row queries (incl. retry bookkeeping)
│   ├── links.go                Link index writes
│   ├── search.go               Hybrid FTS + pgvector RRF SQL
│   └── telemetry.go            Telemetry counters and tool-call rows
├── media/
│   ├── download.go             Media download and decryption
│   ├── upload.go               Media encryption and upload
│   ├── ogg.go                  OGG/Opus PTT parsing (in-process duration + waveform)
│   ├── classify.go             Media-type classification
│   └── helpers.go              Utilities
├── mediaretry/
│   └── worker.go               Bounded background re-download of requested-but-failed media
├── embed/
│   └── client.go               Outbound client to the L3 embedder (query-time vectors)
├── analyzer/
│   └── client.go               Outbound client to the L3 transcriber (/api/media/analyze)
├── indexer/
│   └── links.go                URL/link extraction from message content
├── migrations/
│   ├── embed.go                go:embed of the .sql migration files
│   └── 000001 + 000003–000010  golang-migrate up/down pairs (PostgreSQL + pgvector schema)
└── data/                       Runtime directory for downloaded media + cooldown marker
    └── .gitkeep
```

## Graceful Shutdown

The bridge handles `SIGINT` and `SIGTERM` with an ordered shutdown:

1. Stop accepting new HTTP requests and drain in-flight ones (15s deadline).
2. Stop the media-retry worker (cancel its context and wait for it) so no in-flight download races the disconnect or store close.
3. Disconnect from WhatsApp.
4. Close the PostgreSQL store: the writer goroutine drains queued writes, then the `pgx` pool is closed.

If a shutdown signal arrives before the initial WhatsApp connection is established, the bridge tears down and exits immediately without writing a cooldown marker.

## Logging

Logs are JSON (`log/slog`) on stderr. Each line carries a timestamp, level, the `service` field, a per-area `component` (e.g. `store`, `client`, `http`, `api`, `mediaretry`, `wastore`), and message/key-value context.

Example:

```json
{"time":"2025-01-23T10:45:30.123456Z","level":"INFO","msg":"WhatsApp connection established","service":"whatsapp-bridge-go"}
```

Control verbosity via `BRIDGE_LOG_LEVEL`:

```bash
BRIDGE_LOG_LEVEL=debug ./whatsapp-bridge  # debug: everything
BRIDGE_LOG_LEVEL=info  ./whatsapp-bridge  # info and above (default)
```

## Troubleshooting

### Bridge exits immediately with "DATABASE_URL ... is required"

`DATABASE_URL` is mandatory. Export a valid PostgreSQL DSN (with `pgvector` available in that database) before starting.

### Migration errors at startup

The bridge runs golang-migrate `Up()` on every start. Ensure the role in `DATABASE_URL` can create extensions/tables and that the `pgvector` extension is installable. Never hand-edit an already-applied migration; add the next numbered pair instead.

### QR Code Not Appearing

- Ensure the terminal is wide enough (at least 80 columns).
- Set `BRIDGE_LOG_LEVEL=debug` and check for connection-setup errors.

### Connection Drops Frequently

- Check network stability.
- WhatsApp may rate-limit repeated reconnections.
- Confirm the whatsmeow pin is current (see `go.mod`).

### "TemporaryBan" or "ClientOutdated" Events

- These are permanent disconnects: the bridge writes a 10-minute cooldown marker and exits with code 2.
- Wait out the cooldown (10 minutes) before restarting.
- If `ClientOutdated` persists, update whatsmeow: `go get -u go.mau.fi/whatsmeow@latest`.

### Port Already in Use

```bash
lsof -i :8080
# or
netstat -tulpn | grep 8080
```

Change `BRIDGE_ADDR` to an available port if needed.

## License

MIT License. See [LICENSE](LICENSE) for details.
