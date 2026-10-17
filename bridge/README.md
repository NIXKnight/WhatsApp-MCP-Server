# WhatsApp Bridge

Go HTTP server (layers L2/L3) that connects to WhatsApp via [whatsmeow](https://go.mau.fi/whatsmeow), captures every event into PostgreSQL, and exposes a REST API on `127.0.0.1:8080`. The Python FastMCP server consumes this API to deliver 21 MCP tools to any MCP/LLM host; Python enrichment workers (embedder, transcriber) read and write the same database. This bridge owns the schema and is the single writer to PostgreSQL.

## Overview

The bridge wraps the [whatsmeow](https://go.mau.fi/whatsmeow) library (WhatsApp Web multi-device protocol) and persists messages, chats, contacts, media metadata, links, telemetry, and watermarks into **PostgreSQL with the `pgvector` extension**. It implements a supervised connection state machine with exponential backoff, QR-code pairing, keepalive monitoring, and a post-ban `BridgeHoldExit=78` cooldown; panic-like or ordinary status 2 remains restartable. On top of capture it provides hybrid full-text + vector search, a background media-retry worker, a per-tool telemetry sink, and a thin media-analysis proxy to the L3 transcriber.

Security: the API has **no authentication** and must only be reachable on the loopback interface (`127.0.0.1` / `localhost`) or a compose-internal network. The bridge logs a warning at startup if `BRIDGE_ADDR` is not loopback.

## Prerequisites

- Go 1.26 or later.
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

The Dockerfile is a multi-stage build: a `golang:1.26-alpine` builder compiles the binary with `CGO_ENABLED=0`, and the runtime stage is a minimal `alpine:3.21` image carrying only the static binary plus `ca-certificates` and `tzdata`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (required) | PostgreSQL DSN. Example: `postgres://user:pass@host:5432/whatsapp?sslmode=disable`. The process exits if unset. |
| `BRIDGE_ADDR` | `127.0.0.1:8080` | TCP bind address for the HTTP server. A warning is logged if this is not a loopback address — the API has no auth. |
| `BRIDGE_DATA_DIR` | `./data` | Directory for **downloaded media files** (and the `BridgeHoldExit=78` cooldown marker). Created if missing; must be writable. Session/device data lives in PostgreSQL, not here. |
| `BRIDGE_LOG_LEVEL` | `info` | Log verbosity: `debug`, `info`, `warn`, or `error`. Logs are JSON to stderr. |
| `BRIDGE_LOG_STATUS_REQUESTS` | `true` | Exact lowercase `true`/`false`. When `false`, suppress successful `GET /api/status` access records only; errors, non-GET requests, and every other endpoint remain logged. |
| `EMBEDDER_URL` | `http://embedder:8000` | Base URL of the L3 embedder, called for query-time embeddings during hybrid search. |
| `ANALYZER_URL` | `http://transcriber:8500` | Base URL of the L3 transcriber, called by the `/api/media/analyze` proxy. |
| `MEDIA_RETRY_ENABLED` | `true` | Gates the background media-retry worker. When `false`, the worker goroutine never starts. |
| `MEDIA_RETRY_INTERVAL` | `3m` | Poll cadence of the media-retry worker. Doubles as the cross-cycle backoff between attempts for a single row. Go duration syntax (e.g. `90s`, `5m`). |
| `MEDIA_MAX_DOWNLOAD_ATTEMPTS` | `3` | Max re-download attempts for one media row before it is flagged `download_permanently_failed`. |
| `MEDIA_RETRY_BATCH_SIZE` | `10` | Maximum retry-eligible rows processed per cycle. |
| `BRIDGE_MANAGED` | `false` | Exact lowercase `true`/`false`. Managed mode never emits a QR and returns reserved hold status 78 when pairing or a permanent reconnect action is required. |
| `BRIDGE_NOTIFY_ENABLED` | `false` | Opt-in incoming metadata relay; no automatic WhatsApp actions. |
| `BRIDGE_NOTIFY_URL` | (required when enabled) | Private HTTPS or literal-loopback HTTP `/webhooks/<route>` endpoint. |
| `BRIDGE_NOTIFY_SECRET` | (required when enabled) | At least 32 bytes of operator-provided HMAC key material; byte-counted, never generated or stored in the outbox. |
| `BRIDGE_NOTIFY_GROUP_ALLOWLIST` | (required when enabled) | Exact comma-separated canonical numeric `@g.us` JIDs. |
| `BRIDGE_NOTIFY_OPERATOR_JIDS` | (required when enabled) | Exact comma-separated operator PN/LID aliases sharing one private session. |
| `BRIDGE_NOTIFY_TIMEOUT` | `5s` | Request deadline. |
| `BRIDGE_NOTIFY_POLL_INTERVAL` | `1s` | Enabled dispatcher poll cadence. |
| `BRIDGE_NOTIFY_MIN_INTERVAL` | `2s` | Minimum global attempt spacing. |
| `BRIDGE_NOTIFY_BATCH_SIZE` | `20` | Sequential claims per cycle. |
| `BRIDGE_NOTIFY_MAX_ATTEMPTS` | `10` | Committed claims before dead-lettering. |
| `BRIDGE_NOTIFY_MAX_AGE` | `24h` | Expiry from original receipt, frozen per event. |
| `BRIDGE_NOTIFY_RETENTION` | `7d` | Retention after terminal transition. |

Managed systemd bridge runs read transitional shared compatibility `~/.config/whatsapp-bridge/env` (the exact 30-name current-consumer union: `DATABASE_URL`, `PG_DSN`, `BRIDGE_ADDR`, `BRIDGE_DATA_DIR`, `BRIDGE_LOG_LEVEL`, `BRIDGE_LOG_STATUS_REQUESTS`, `EMBEDDER_URL`, `ANALYZER_URL`, `MEDIA_RETRY_ENABLED`, `MEDIA_RETRY_INTERVAL`, `MEDIA_MAX_DOWNLOAD_ATTEMPTS`, `MEDIA_RETRY_BATCH_SIZE`, `BRIDGE_URL`, `LOG_LEVEL`, `MCP_TRANSPORT`, `MCP_HOST`, `MCP_PORT`, `WORKSPACE_DIR`, `WORKSPACE_HOST_PATH`, `EMBEDDING_MODEL`, `EMBED_BATCH_SIZE`, `EMBED_POLL_INTERVAL`, `EMBED_HTTP_ADDR`, `WHISPER_URL`, `TRANSCRIBE_BATCH_SIZE`, `TRANSCRIBE_POLL_INTERVAL`, `ANALYZER_HTTP_ADDR`, `DASHBOARD_HOST`, `DASHBOARD_PORT`, and `CONFIG_PATH`), bridge-only `~/.config/whatsapp-bridge/relay.env`, then shared authentication/admission `~/.config/whatsapp-notifications/env`. The shared compatibility file remains transitional until a later dedicated-file migration. The bridge unit retains all three files but launches with exact `ExecStart=/usr/bin/env BRIDGE_ADDR=127.0.0.1:8080 BRIDGE_LOG_LEVEL=info %h/.local/bin/whatsapp-bridge`, so the command-local assignments make loopback/info authoritative despite `EnvironmentFile=` taking precedence over `Environment=` regardless of directive order; the controller reads only the shared notification file plus its controller-only file, applies `UnsetEnvironment=DATABASE_URL`, and never receives the database DSN. `relay.env` exclusively owns `BRIDGE_NOTIFY_ENABLED`, `BRIDGE_NOTIFY_URL`, and optional documented delivery tuning; the shared file exclusively owns secret/group/operator values. Installer-managed directories/files are `0700`/`0600`, reject symlink/non-directory and symlink/non-regular paths, preserve content, and never print values. Initial/repair pairing is private foreground only with `BRIDGE_MANAGED=false BRIDGE_LOG_LEVEL=info`, never `debug`; Compose first starts PostgreSQL, Redis, and the controller namespace with delivery disabled, then uses attached `docker compose run --rm --no-deps -e BRIDGE_MANAGED=false -e BRIDGE_NOTIFY_ENABLED=false -e BRIDGE_LOG_LEVEL=info bridge`, requires that command-local notification disablement to override conflicting `.env`, retains all volumes, and starts the managed bridge afterward. QR output is temporary authentication material and must never enter shared logs/captures. Managed mode never generates a QR and selects discard-only SQL-store/migration, WhatsMeow, and `waLogAdapter` dependency logging before construction; details are neither buffered nor replayed. The separate application event path remains authoritative: managed mode calls the terminal handler first and emits exactly one fixed `permanent_disconnect` only for its winner, with reason/message/code/expire values absent; foreground emits its current event-specific detailed permanent records in their exact order before calling the handler. Accepted Connected/QR success restores retry count before channel publication/logging, and foreground retains exact `QR code scanned successfully` only after accepted QR handling; managed mode never enters QR. Managed unpaired startup emits only fixed `bridge_managed_unpaired`; it, an active cooldown, wrapped QR timeout, wrapped startup permanent disconnect, and one-shot post-connect permanent disconnect return exported `BridgeHoldExit=78` with application fixed classes only. Foreground dependency logs and QR remain unchanged; ordinary cancellation/failures return 1 and actual signals return 0. Status 78 is reserved for application holds and does not collide with Go runtime's panic-like status 2. Compose uses the signal-forwarding bridge entrypoint to map child `BridgeHoldExit=78` to container 0 only when `BRIDGE_MANAGED` is exact `true`; exact `false` or unset preserves child 78 so the documented foreground `docker compose run` reports timeout/hold failure. Child 2 and every other status remain preserved after signal-interrupted waits settle.

See [Hermes notifications](../docs/hermes-notifications.md) for exact bounds,
privacy limitations, disabled cleanup, required receiver permission verification,
and separately approved backlog/migration operations. Deployment stays **PAUSED**.

The embedder timeout (2s) and analyzer timeout (180s) are compiled-in constants, not environment variables.

Example:

```bash
BRIDGE_LOG_LEVEL=debug BRIDGE_DATA_DIR=/tmp/whatsapp-media \
DATABASE_URL='postgres://user:pass@localhost:5432/whatsapp?sslmode=disable' \
./whatsapp-bridge
```

## REST API Endpoints

All endpoints return JSON. Errors return `{"error":"...","code":"..."}` with an appropriate HTTP status. Query parameters and request bodies use snake_case (e.g. `chat_jid`, `message_id`, `quoted_message_id`). The router is [chi](https://github.com/go-chi/chi); a global 60s request timeout applies to every route except `POST /api/media/analyze` (190s).

26 routes are registered in `bridge/api/server.go`:

```
GET    /api/status
GET    /api/messages
GET    /api/messages/{id}
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
- **Cooldown Marker**: after a permanent disconnect (ban, logout, client outdated), a `cooldown` marker file is written under `BRIDGE_DATA_DIR` recording a 10-minute hold-off, and the process returns exported **`BridgeHoldExit=78`**. An active marker returns the same sole supervisor hold status; panic-like or ordinary status 2 remains restartable. A successful connection clears the marker.
- **Keepalive Monitoring**: a forced reconnect is triggered after **3 consecutive** `KeepAliveTimeout` events. whatsmeow's own auto-reconnect is disabled so the bridge runs a single, deterministic reconnect loop.
- **Event Handlers**: handle `Connected`, `Disconnected`, `LoggedOut`, `TemporaryBan`, `ClientOutdated`, `ConnectFailure`, and `StreamReplaced`.

## Data Storage

### PostgreSQL + pgvector

PostgreSQL is the single source of truth. The bridge opens one `pgx` pool (`stdlib.OpenDB`, max 10 open / 5 idle connections) and serialises every write through a single writer goroutine. The whatsmeow device store uses the same database via `sqlstore.New(ctx, "pgx", databaseURL, ...)`, so credentials and key material live in PostgreSQL — there are no `.db` files and no SQLite anywhere.

Schema is **bridge-owned**: only `bridge/migrations/` creates or alters tables, applied at startup with [golang-migrate](https://github.com/golang-migrate/migrate) (embedded via `iofs`). There are **12 migration pairs** through `000013`: `000001` and `000003`–`000013` (no `000002`). Never edit a committed migration; add the next numbered pair.

Key tables and the materialized view:

| Object | Purpose |
|--------|---------|
| `chats` | Chat metadata: JID, name, group flag, unread count, optional `topic_embedding vector(384)`. |
| `contacts` | Contact name / notify / phone, optional `name_embedding vector(384)`. |
| `messages` | All messages, with generated `content_fts`, an embedder watermark, and durable exact-match `is_bridge_origin` classification that updates never clear. |
| `messages_media` | Media metadata: URL, keys, local path, `download_attempts`, `download_permanently_failed`, and `transcription` / `transcription_lang`. |
| `message_embeddings` | One `embedding vector(384)` row per message (dimension fixed by the `EmbeddingDim = 384` constant and migration `000006`). |
| `links` | URL/link index extracted from message content by `bridge/indexer/`. |
| `telemetry_daily` | Daily rolling counters (sent, received, media, links). |
| `telemetry_tool_calls` | Per-tool-call rows: `tool_name`, `duration_ms`, `success`, `error`. |
| `watermarks` | Per-chat `last_seen` cursor backing the `check_triggers` batch endpoint. |
| `notification_outbox` | Optional metadata-only event/retry state; committed atomically with its eligible live event, including when history inserted the message row first. No content, endpoint or authentication material. |
| `bridge_outbound_origins` | Exact pre-send message/chat identities used to suppress bridge echoes; PN sends register both the requested PN and resolved LID before sending. |
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

### Optional Hermes incoming relay (`bridge/notify/`)

An independent single dispatcher leases committed outbox events, signs generic
Hermes HMAC V2 POSTs, and records bounded retry/terminal state. It never sends
from a whatsmeow callback or store transaction. The first eligible live remote
or own event in an exact configured group/operator chat notifies even when a
non-notifying history import inserted the message row first. History imports,
bridge-origin echoes, edits, and already-notified duplicates do not. Exact
bridge-origin evidence is classified durably on the message row before observed
markers can be pruned. The controller classifies own events as manual, confirmed
Pi-originated context-only, or unresolved/uncertain rather than excluding them
universally. Receiver acceptance
is not agent completion or exactly-once execution. Disabled mode creates no new
events or HTTP sender; local expiry/pruning runs in bounded minute batches. This
is the database-handoff pattern plus an optional outbound webhook, not a new
message bus. Origin-marker retention is local conservative policy, not a claim
about a WhatsApp delivery guarantee: unobserved uncertain sends are retained for
90 days; observed tombstones are eligible after 7 days only when the message row
already has durable bridge-origin classification. Both classes prune in bounded,
index-supported, lock-skipping batches. Configuration and operational gates are
in the linked guide.

### Telemetry

`POST /api/telemetry/tool` accepts per-call records from the MCP server's telemetry middleware and writes them to `telemetry_tool_calls`, while capture and send paths bump the daily counters in `telemetry_daily`.

## Project Structure

```
bridge/
├── README.md                   This file
├── go.mod / go.sum             Module definition and checksums
├── main.go                     Entry point: config, BridgeHoldExit=78 cooldown marker, lifecycle, graceful shutdown
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
│   └── 000001 + 000003–000013  12 up/down pairs; no 000002 (PostgreSQL + pgvector schema)
└── data/                       Runtime directory for downloaded media + BridgeHoldExit=78 cooldown marker
    └── .gitkeep
```

## Graceful Shutdown

The bridge handles `SIGINT` and `SIGTERM` with an ordered shutdown:

1. Stop accepting new HTTP requests and drain in-flight ones (15s deadline).
2. Cancel and wait for notification and media-retry workers before any store close.
3. Disconnect from WhatsApp and remove/drain callback ingress.
4. Close the PostgreSQL store: admission closes, the writer drains queued writes, then the `pgx` pool is closed.

A single cleanup path also covers startup errors and pre-connection signals.
Migrations 012 and 013 are additive. Migration 013's new message column has a
default and all older message SQL uses explicit column lists, so the column's
presence alone does not break an old binary. Routine rollback disables
notifications on a migration-aware binary; destructive down requires separate
authorization and loses bridge-echo suppression evidence. For a 013 down
migration, stop the current bridge, restore the old binary artifact while keeping
it stopped, apply the down migration, and only then start the old binary. Never
run the new binary after the column has been removed. The current inventory is
12 pairs through `000013` (`000001` and `000003`–`000013`; no `000002`).

If a shutdown signal arrives before the initial WhatsApp connection is established, the bridge tears down and exits immediately without writing a `BridgeHoldExit=78` cooldown marker.

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
BRIDGE_LOG_STATUS_REQUESTS=false ./whatsapp-bridge  # omit only successful GET /api/status access records
```

## Troubleshooting

### Bridge exits immediately with "DATABASE_URL ... is required"

`DATABASE_URL` is mandatory. Export a valid PostgreSQL DSN (with `pgvector` available in that database) before starting.

### Migration errors at startup

The bridge runs golang-migrate `Up()` on every start. Ensure the role in `DATABASE_URL` can create extensions/tables and that the `pgvector` extension is installable. Never hand-edit an already-applied migration; add the next numbered pair instead.

### QR Code Not Appearing

Pair only in an approved operator-private foreground terminal with exact:

```bash
BRIDGE_MANAGED=false BRIDGE_LOG_LEVEL=info ./whatsapp-bridge
```

Keep the terminal at least 80 columns wide. Never use debug logging for pairing, and never redirect, record, share, or persist the QR output.

### Connection Drops Frequently

- Check network stability.
- WhatsApp may rate-limit repeated reconnections.
- Confirm the whatsmeow pin is current (see `go.mod`).

### "TemporaryBan" or "ClientOutdated" Events

- These are permanent disconnects: the bridge writes a 10-minute cooldown marker and returns exported `BridgeHoldExit=78`, the only non-restartable supervisor hold.
- Wait out the `BridgeHoldExit=78` cooldown (10 minutes) before restarting; panic-like or ordinary status 2 remains restartable.
- If `ClientOutdated` persists, update only to a reviewed whatsmeow pin (currently `go.mau.fi/whatsmeow@v0.0.0-20261007111105-c386243a72ba`); do not use `@latest`.

### Port Already in Use

```bash
lsof -i :8080
# or
netstat -tulpn | grep 8080
```

Change `BRIDGE_ADDR` to an available port if needed.

## License

MIT License. See [LICENSE](LICENSE) for details.

## Local release artifact

Build a host-native release from the repository root with:

```bash
scripts/build-bridge-release.sh
```

A normal release requires a clean worktree and is written beneath
`bridge/dist/releases/<revision>/` with a SHA-256 file and timestamp-free
provenance. During development, `--allow-dirty` builds the current bridge
worktree twice, verifies that both binaries have the same SHA-256 digest, and
writes a revision-plus-source-digest directory marked `NONDEPLOYABLE`. This is
a same-source, same-toolchain check; it is not a claim of reproducibility
across different Go toolchains or dependency caches.

The host systemd unit must treat exported `BridgeHoldExit=78` as the sole deliberate cooldown
(`RestartPreventExitStatus=78`); panic-like or ordinary status 2 remains restartable. The pairing QR is emitted on stdout; production
supervision should discard stdout and pairing must be performed separately in
an approved private foreground terminal with exact `BRIDGE_MANAGED=false BRIDGE_LOG_LEVEL=info`, never debug and never captured in the journal.
