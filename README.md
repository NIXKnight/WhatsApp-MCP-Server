# WhatsApp MCP Server

A self-hosted WhatsApp **data platform** with a Model Context Protocol (MCP) interface. A Go bridge captures every WhatsApp event into PostgreSQL, Python enrichment workers add embeddings and transcriptions, and a Python FastMCP server exposes the data and send actions to any MCP/LLM host.

---

## Architecture

### 6-Layer Model

```
L1  External Edge
    go.mau.fi/whatsmeow  (WhatsApp protocol; external dependency)

L2  Capture & Gateway
    bridge/client/        event handler, QR auth, send
    bridge/connection/    state machine, keepalive, backoff
    bridge/api/           26-route HTTP API (chi router)
    bridge/media/         upload, download, OGG/Opus analysis
    bridge/indexer/       URL/link extraction
    bridge/config/        env-based configuration
    bridge/embed/         outbound client -> L3 embedder
    bridge/analyzer/      outbound client -> L3 transcriber

L3  Persistence & Enrichment
    bridge/store/         PostgreSQL read/write via pgx pool
    bridge/migrations/    11 golang-migrate pairs through 000012 (000001, 000003-000012; no 000002)
    bridge/mediaretry/    media re-download (retry) worker
    embedder/             poll worker + on-demand /embed server
    transcriber/          poll worker + on-demand /analyze server

L4  Tool / Contract
    mcp-server/           Python FastMCP: 21 tools, stdio/SSE/HTTP

L5  Intelligence & Control
    Claude Code / Desktop / any MCP client  (external)

L6  Operations & Observability
    docker-compose.yml    8-service stack (including Redis + session controller)
    dashboard/            FastAPI + HTMX read-only view
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
| `bridge/` | L2 / L3 | Go bridge: QR auth, event capture, 26-route HTTP API, 11 migration pairs (`001`, `003`–`012`; no `002`), single-writer PostgreSQL store, hybrid FTS + pgvector RRF search |
| `mcp-server/` | L4 | Python FastMCP server: 21 tools, stdio / SSE / streamable-HTTP, per-tool telemetry middleware |
| `embedder/` | L3 | Poll worker + on-demand HTTP server: embeds messages into `vector(384)` using `paraphrase-multilingual-MiniLM-L12-v2` |
| `transcriber/` | L3 | Poll worker + on-demand HTTP server: transcribes voice notes via whisper.cpp, on-demand video frame extraction via ffmpeg |
| `dashboard/` | L6 | Read-only FastAPI + HTMX ops view, Tailwind v3.4 / Tremor design language, dark/light toggle, self-hosted FontAwesome |
| `session-controller/` | L5 / L6 | Mandatory Redis-backed schema-v2 worker with persistent, lazy Pi sessions and controlled compaction |

---

## Prerequisites

- Go 1.26+
- Python 3.11+
- [uv](https://astral.sh/uv) (Python package manager)
- PostgreSQL 17 with the `pgvector` extension (the bridge runs migrations at startup)
- Docker + Docker Compose (recommended — provisions the full stack)
- A WhatsApp account with multi-device support
- A running whisper.cpp `whisper-server` (optional; required only for voice-note transcription and `analyze_media`)

---

## Setup

### Option A: Docker Compose (recommended)

Create an operator-owned `.env` with `BRIDGE_NOTIFY_SECRET`, exact `BRIDGE_NOTIFY_GROUP_ALLOWLIST`, exact `BRIDGE_NOTIFY_OPERATOR_JIDS`, `SESSION_CONTROLLER_PI_PROVIDER`, `SESSION_CONTROLLER_PI_MODEL`, `SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS`, `PI_AGENT_DIR`, optional `WORKSPACE_PATH`, and optional raw `POSTGRES_PASSWORD` plus percent-encoded `POSTGRES_PASSWORD_URI` overrides. Do not commit it. Start from these intentionally value-free password entries; when overridden, both represent the same password:

```dotenv
POSTGRES_PASSWORD=
POSTGRES_PASSWORD_URI=
```

The raw variable is used only for PostgreSQL initialization. Every client DSN uses the URI variable, whose reserved characters must be percent-encoded. Both compatibility defaults remain `whatsapp`. Build and validate without activation:

```bash
docker compose build
docker compose config --quiet
```

After separately authorized schema-v1 backlog review/retirement and receiver-capability checks, set `BRIDGE_NOTIFY_ENABLED=false` in the operator-owned Compose environment and first start only PostgreSQL, Redis, and the controller. The controller intentionally remains unready while waiting for a bridge, but its running service namespace is required by `network_mode: service:session-controller`:

```bash
BRIDGE_NOTIFY_ENABLED=false docker compose up -d postgres redis session-controller
docker compose ps
```

For initial or repaired pairing only, run the bridge attached in that existing controller namespace with exactly:

```bash
docker compose run --rm --no-deps -e BRIDGE_MANAGED=false -e BRIDGE_NOTIFY_ENABLED=false -e BRIDGE_LOG_LEVEL=info bridge
```

The command-local `BRIDGE_NOTIFY_ENABLED=false` is mandatory and overrides any conflicting Compose `.env` value, so private pairing cannot deliver controller notifications. Because this command also sets exact `BRIDGE_MANAGED=false`, the entrypoint preserves child `BridgeHoldExit=78`; `docker compose run` therefore reports a pairing timeout or hold as exit 78 instead of converting it to success. Scan the QR in that operator-only foreground terminal, wait for successful pairing, then stop the foreground run normally. Never use debug logging, detach the pairing run, redirect or capture its output, paste the QR, or expose it in a shared terminal, CI log, screen recording, or support transcript. Treat the QR as temporary authentication material and clear private terminal scrollback according to operator policy. Do not run `docker compose down`, `down -v`, `rm`, or remove named volumes: retain PostgreSQL, bridge authentication, Redis, controller sessions, and every volume. Then start the managed bridge while delivery is still disabled, followed by the remaining services:

```bash
BRIDGE_NOTIFY_ENABLED=false docker compose up -d bridge
BRIDGE_NOTIFY_ENABLED=false docker compose up -d embedder transcriber mcp-server dashboard
docker compose ps
curl --fail --silent http://127.0.0.1:8644/ready
```

If retained bridge state is already paired, skip only the foreground QR step; still establish the controller namespace before managed bridge startup. Before any authorization to set `BRIDGE_NOTIFY_ENABLED=true`, an operator must invoke the exact non-prompting inventory command while delivery remains disabled:

```bash
BRIDGE_NOTIFY_ENABLED=false docker compose run --rm --no-deps session-controller --check-pi-inventory
```

The controller image ENTRYPOINT already invokes `node /app/dist/src/main.js`; do not append another `node dist/src/main.js`. Bare-metal and systemd execution still invoke the resolved Node binary, the installed `dist/src/main.js`, and `--check-pi-inventory` directly.

Require exact stdout `{"status":"pi_inventory_ready"}` and exit 0. Exact stderr `{"status":"pi_inventory_failed"}` with exit 1 blocks cutover; investigate privately without exposing child output or credentials. This command is narrowly authorized to read configured Pi auth/model files, apply configured provider/model selection, create/remove one owned temporary tree, and start/bind/dispose the existing MCP child for inventory using validated command/args/cwd. Unlike normal full mode, preflight uses owned temporary cwd, agentDir, and HOME/.agents resource/package roots, project-untrusted in-memory settings, and an owned public `ResourceLoader` with only Pi's MCP built-in plus the owned `whatsapp_session_controller` registration. It restores the exact prior HOME state after disposal on success or failure; configured project/agent/global roots are never scanned, their extensions, skills, templates, themes, context, system files, hooks/startup handlers, and unrelated MCP servers are neither loaded nor run, and child/startup stdout/stderr is not relayed. It does not prompt a model, execute a tool, access Redis/database/backlog, consume a bridge event, start scheduling/webhook intake, or perform a WhatsApp action, and it does not itself authorize service activation or schema-v2 delivery. Only after this check and all other gates pass may a separately authorized cutover set `BRIDGE_NOTIFY_ENABLED=true` and recreate the managed bridge.

The controller is mandatory for the schema-v2 deployment path. Normal service startup first listens with readiness false, then polls the literal-loopback bridge status until the API reports both `state=CONNECTED` and live `is_connected=true`; only then does it start scheduling, acquire the application lease, and report ready. The bridge status `state` remains the state-machine value, while `is_connected` is the underlying WhatsMeow `IsConnected() && IsLoggedIn()` truth. Pi/model auth/MCP remain lazy until first claimed work during normal startup; `/ready` intentionally excludes them. While operational it periodically enforces that same exact bridge status contract; one timeout, transport failure, non-OK response, malformed body, or non-exact status drops readiness, synchronously fails the coordinator's application fence, aborts scheduling and heartbeat renewal, makes already-claimed workers observe `appCurrent=false` before another prompt, requests exit 1, and cleans up. Ordinary SIGINT/SIGTERM instead preserves graceful `stopScheduling` plus unbounded drain. Compose makes the bridge depend on Redis health and controller `service_started` (not controller health), while the controller depends only on Redis health, so loopback polling introduces no readiness cycle.

PostgreSQL initialization uses the raw `${POSTGRES_PASSWORD:-whatsapp}` value; every client DSN uses `${POSTGRES_PASSWORD_URI:-whatsapp}`. If overridden, `POSTGRES_PASSWORD_URI` must be the percent-encoded URI representation of the same raw password used to initialize the role. An existing `postgres-data` volume keeps its initialized password; changing either `.env` variable does not rotate that role. Rotation remains a separately authorized and verified database operation: rotate the database credential first, then change both matching representations and verify every client.

Compose has no portable infinite stop duration. Never use `docker compose down` while a turn may be active. Drain the controller first:

```bash
controller_id="$(docker compose ps -q session-controller)"
docker stop --time=-1 "$controller_id"
docker compose stop bridge dashboard transcriber embedder mcp-server
docker compose stop redis postgres
```

`docker stop --time=-1` is an explicit manual stop, so it suppresses `restart: unless-stopped`, sends the controller its normal stop signal, and waits indefinitely while a Pi turn settles. Investigate rather than force-killing.

### Option B: Bare-metal with systemd

```bash
make install
make setup
make whisper-unit                    # optional
```

These commands install artifacts, preserve existing templates/environment/state, and statically verify temporary copied units only; they do not run `daemon-reload` and never enable, start, restart, or stop services. Controller installation copies only `package.json`, `package-lock.json`, `tsconfig.json`, `src/**/*.ts`, and the three named operator-neutral templates into a fresh same-filesystem staging directory; tests and environment/auth/model/runtime/session/local files cannot enter the release. It runs locked install/typecheck/build/prune there, makes the complete staged tree read-only, atomically publishes the immutable versioned release under `~/.local/share/whatsapp-session-controller/releases/`, and switches `current` only after success; the active release is never mutated. Old releases are retained. Removing them is a manual, separately authorized operation performed only after confirming they are inactive. Both installers run with `umask 077`, reject symlink/non-directory managed directories and symlink/non-regular managed files, preserve content, enforce `0700` configuration/state directories and `0600` files, and never print values. The controller installer validates the Redis state directory with `secure_dir` before writing `redis.conf` or state. Review transitional shared compatibility `~/.config/whatsapp-bridge/env`, bridge-only `~/.config/whatsapp-bridge/relay.env`, shared notification-auth/admission `~/.config/whatsapp-notifications/env`, controller-only `~/.config/whatsapp-session-controller/env`, and all three templates. The current bridge, MCP server, embedder, transcriber, and dashboard units still share `~/.config/whatsapp-bridge/env`; it is explicitly transitional compatibility configuration until a later migration, not a final ownership split. Its exact accepted union is `DATABASE_URL`, `PG_DSN`, `BRIDGE_ADDR`, `BRIDGE_DATA_DIR`, `BRIDGE_LOG_LEVEL`, `EMBEDDER_URL`, `ANALYZER_URL`, `MEDIA_RETRY_ENABLED`, `MEDIA_RETRY_INTERVAL`, `MEDIA_MAX_DOWNLOAD_ATTEMPTS`, `MEDIA_RETRY_BATCH_SIZE`, `BRIDGE_URL`, `LOG_LEVEL`, `MCP_TRANSPORT`, `MCP_HOST`, `MCP_PORT`, `WORKSPACE_DIR`, `WORKSPACE_HOST_PATH`, `EMBEDDING_MODEL`, `EMBED_BATCH_SIZE`, `EMBED_POLL_INTERVAL`, `EMBED_HTTP_ADDR`, `WHISPER_URL`, `TRANSCRIBE_BATCH_SIZE`, `TRANSCRIBE_POLL_INTERVAL`, `ANALYZER_HTTP_ADDR`, `DASHBOARD_HOST`, `DASHBOARD_PORT`, and `CONFIG_PATH`. `BRIDGE_NOTIFY_ENABLED`, `BRIDGE_NOTIFY_URL`, and optional documented relay tuning belong only in `relay.env`; `BRIDGE_NOTIFY_SECRET`, `BRIDGE_NOTIFY_GROUP_ALLOWLIST`, and `BRIDGE_NOTIFY_OPERATOR_JIDS` belong only in the shared notification file. The controller unit reads notification+controller only, never reads either bridge file, and explicitly applies `UnsetEnvironment=DATABASE_URL`. After explicit activation approval:

```bash
make reload-units
make start
make status
make logs
```

The default `start`, `stop`, `restart`, `status`, and `logs` targets deliberately exclude optional `whatsapp-whisper.service`, so the normal stack does not fail when that unit is absent. If Whisper is installed and separately authorized, manage it explicitly with `make start-whisper`, `make stop-whisper`, `make status-whisper`, and `make logs-whisper`; `make whisper-unit` remains optional and performs setup/static verification only.

The managed bridge unit sets `BRIDGE_MANAGED=true`, `RestartPreventExitStatus=78`, `StandardOutput=null`, and `StandardError=journal`, and keeps the transitional compatibility, bridge-only relay, and shared notification files. Its exact `ExecStart=/usr/bin/env BRIDGE_ADDR=127.0.0.1:8080 BRIDGE_LOG_LEVEL=info %h/.local/bin/whatsapp-bridge` makes managed loopback/info authoritative even when an environment file conflicts; the documentation explicitly states that `EnvironmentFile=` values override `Environment=` assignments regardless of directive order. Managed mode selects discard-only dependency loggers before SQL device-store migration and WhatsMeow construction; the SQL store, WhatsMeow, and `waLogAdapter` cannot emit or later replay variable dependency details. The separate application logger remains authoritative for fixed classes. Managed permanent handling calls the terminal handler first and emits exactly one events-layer `permanent_disconnect` only for its winner, with no reason/message/code/expire values; foreground preserves each current event-specific detailed permanent record byte-for-byte and in order before calling that handler. Atomic `markConnected` holds the transition lock across permanent rejection, Connected state/log, retry reset, and nested connected-channel publication; foreground retains exact `QR code scanned successfully` and success return only after that whole operation succeeds. Managed mode never requests, enters, or prints a QR. Managed unpaired startup emits only `bridge_managed_unpaired` and returns exported `BridgeHoldExit=78`; an active cooldown, wrapped `ErrQRTimeout`, wrapped startup permanent disconnect, and the manager's one-shot post-connect permanent-disconnect event likewise end at 78 without exposing dependency details or restarting. Status 78 is reserved for these application holds so Go runtime's panic-like status 2 remains an ordinary restartable failure. Ordinary/context-cancellation failures end at 1 unless an actual signal owns shutdown, and signals end at 0. Initial or repaired bare-metal pairing requires a separately approved private foreground bridge run in an operator-only terminal with exact `BRIDGE_MANAGED=false BRIDGE_LOG_LEVEL=info` (never `debug`), followed by the separately approved managed-service start. Foreground retains its existing dependency logs and QR behavior. Compose uses the exact attached `docker compose run --rm --no-deps -e BRIDGE_MANAGED=false -e BRIDGE_NOTIFY_ENABLED=false -e BRIDGE_LOG_LEVEL=info bridge` procedure above only after PostgreSQL, Redis, and the controller namespace exist with delivery disabled; the command-local notification disablement overrides conflicting `.env`. Do not redirect, record, share, or persist that QR. Bridge stderr remains journaled for fixed application operational/error classes; stdout suppression does not suppress those diagnostics.

`make stop` stops controller intake and drains active Pi/MCP work indefinitely before stopping bridge and Redis. Added units are `whatsapp-session-redis.service` and `whatsapp-session-controller.service`; controller shutdown uses `TimeoutStopSec=infinity` and `KillMode=mixed`. The Redis unit starts the preserved `redis.conf` and then appends exact `--bind 127.0.0.1 ::1 --protected-mode yes`; these final command-line values prevent retained configuration from exposing bare-metal Redis. The controller unit executes exact `${NODE_BIN} %h/.local/share/whatsapp-session-controller/current/dist/src/main.js` and uses `WorkingDirectory` through the atomic `current` symlink; `--preserve-symlinks-main` is deliberately absent. Normal unit startup supplies no CLI flag and remains lazy; operators run the documented inventory flag as an explicit one-shot command, never by adding it to the service unit. The fixed-safe direct-entry guard realpaths both `fileURLToPath(import.meta.url)` and `process.argv[1]`, returns false without variable diagnostics if either lookup fails, enters once through `current`, and remains side-effect free on ordinary import. Node's default main-module resolution pins `import.meta.url` and both package and relative dynamic imports to the concrete immutable release selected at process start even if a later install atomically retargets `current`.

### Option C: stdio transport (Claude Code / Desktop)

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


## Persistent WhatsApp sessions

The mandatory schema-v2 controller accepts authenticated schema-v2 metadata and gives each configured group one persistent Pi session; configured PN/LID aliases share one operator-private session. Redis 7.2+ stores durable ordered work with AOF, `appendfsync always`, and `noeviction`; Pi JSONL files are separate durable session state. Compose Redis publishes no host port and uses its isolated project network; `protected-mode no` is limited to that container network so the controller can connect without inventing credentials. Bare-metal Redis is different: its unit appends `--bind 127.0.0.1 ::1 --protected-mode yes` after the preserved configuration path, making those safety values authoritative even if retained config bytes disagree. This supports controlled local restarts, not universal zero loss under host/storage failure.

During normal service execution, Pi and its existing 21-tool WhatsApp MCP stdio child are initialized exactly once, lazily on the first claimed turn; `/ready` intentionally does not claim Pi/model/MCP readiness. Startup still validates controller configuration and all operator templates. Normal `loadNativeSdk` full mode retains standard project/agent discovery and configured capabilities. Before cutover, the explicit operator-only `--check-pi-inventory` mode instead performs a separate temporary, non-prompting connect/bind/dispose cycle through an owned public-interface loader containing only the required MCP built-in and owned WhatsApp registration, with empty configured MCP input and every skill/template/theme/context surface empty. Hostile configured extensions/startup hooks and unrelated MCP servers cannot load, run, start, or emit; no prompt or tool executes and no startup/child output is forwarded. Both paths fail closed unless the complete `mcp__whatsapp_session_controller` namespace is exactly the 21 production-derived qualified names with no missing, duplicate, or extra 22nd tool and every member has source path `<inline:mcp>` plus source `inline`; same-name wrong-namespace shadows and wrong provenance are rejected.

In Compose, Redis is attached only to the dedicated `controller-internal` network, the controller is attached to both that internal network and `default`, and the bridge inherits both through `network_mode: service:session-controller`; MCP, enrichment, and dashboard services remain only on `default`. Compose sets exact `BRIDGE_MANAGED=true` and runs the bridge through its signal-forwarding entrypoint adapter: only then is bridge child `BridgeHoldExit=78` normalized to container exit 0 to stop `restart: on-failure`; foreground pairing with exact `BRIDGE_MANAGED=false`, or an unset managed variable, preserves child 78 so `docker compose run` reports the timeout/hold failure. Child 0 remains 0, panic-like child status 2 remains 2 and therefore restarts, and every other nonzero status is preserved. The bridge webhook, startup status probe, and controller MCP child use literal loopback (`127.0.0.1:8644` and `127.0.0.1:8080`); the unauthenticated bridge API is never published by the controller. The controller image is built with `APP_UID`/`APP_GID` from the same `${UID:-1000}`/`${GID:-1000}` values used by runtime `user`, so the named `/sessions` volume receives correctly owned image seed directories and the process never runs as root. This does not alter bind-mount ownership: operators must ensure `${WORKSPACE_PATH}` and `${PI_AGENT_DIR}` are readable/writable as required by that numeric UID/GID before activation. Under systemd the controller is `Type=simple`, starts its listener before the bridge, and reports readiness only after WhatsApp connection and application-lease acquisition. Its unit runs from `~/.local/share/whatsapp-session-controller/current`, the atomically switched symlink to one immutable release, and sets `WORKSPACE_DIR=%h/workspace`, so the inherited stdio MCP media path is the same bare-metal workspace configured as `SESSION_CONTROLLER_PI_CWD=${HOME}/workspace` and writable by the bridge for media downloads. The bridge has `Wants=` on the controller, `Requires=` on Redis, and `After=` on both, so explicitly stopping the controller does not stop the bridge before drain.

### Clean cutover and rollback

Do not replay schema-v1 work into the controller. Keep bridge delivery disabled and prior relay workers stopped while a separately authorized database-administration channel reviews v1 pending/leased aggregates and opaque IDs. Obtain separate write authorization for the selected retain/expire/retire action and recheck that no unintended v1 work remains. Repository automation embeds no destructive SQL or retirement command.

Before enabling schema v2, verify exact allowlists, shared HMAC authentication, private/loopback routing, effective receiver capabilities, and normal controller readiness, then run exact `BRIDGE_NOTIFY_ENABLED=false docker compose run --rm --no-deps session-controller --check-pi-inventory` and require `{"status":"pi_inventory_ready"}` with exit 0. The operator's invocation authorizes configured auth/model reads and provider/model selection plus that isolated owned-resource MCP inventory lifecycle with validated command/args/cwd; resource/package discovery is confined to owned temporary cwd, agentDir, and HOME/.agents, exact prior HOME is restored afterward, and configured project/agent/global roots are never scanned. It grants configured extensions/hooks/servers no execution and grants no prompt, tool call, WhatsApp action, backlog/database/Redis access, service activation, or delivery enablement. Routine rollback disables delivery first, drains the controller while the bridge remains available, and retains additive PostgreSQL, Redis, and session state. Down migrations, queue/key deletion, session deletion, credential rotation, and forced termination need separate authorization. No executable destructive SQL is included. Section 14 of the architecture remains binding: reuse the existing bridge and existing WhatsApp MCP; neither the preflight nor this plan creates a replacement gateway, Reactor path, or new action authority.

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
| `MEDIA_RETRY_ENABLED` | `true` | Enable the background media-retry worker. |
| `MEDIA_RETRY_INTERVAL` | `3m` | Poll cadence / per-row backoff. |
| `MEDIA_MAX_DOWNLOAD_ATTEMPTS` | `3` | Attempts before a row is marked permanently failed. |
| `MEDIA_RETRY_BATCH_SIZE` | `10` | Rows retried per cycle. |
| `BRIDGE_MANAGED` | `false` | Exact lowercase `true`/`false`; managed mode never emits pairing QR output. |
| `BRIDGE_NOTIFY_ENABLED` | `false` | Opt-in metadata-only Hermes relay; no auto-reply or action grant. |
| `BRIDGE_NOTIFY_URL` | (required when enabled) | Private HTTPS or literal-loopback HTTP `/webhooks/<route>` endpoint. |
| `BRIDGE_NOTIFY_SECRET` | (required when enabled) | At least 32 bytes of operator-managed HMAC key material; byte-counted, never generated, printed, or persisted in events. |
| `BRIDGE_NOTIFY_GROUP_ALLOWLIST` | (required when enabled) | Exact comma-separated canonical group JIDs. |
| `BRIDGE_NOTIFY_OPERATOR_JIDS` | (required when enabled) | Exact configured PN/LID aliases for the operator-private session. |
| `BRIDGE_NOTIFY_TIMEOUT` | `5s` | Per-request timeout. |
| `BRIDGE_NOTIFY_POLL_INTERVAL` | `1s` | Enabled dispatcher poll cadence. |
| `BRIDGE_NOTIFY_MIN_INTERVAL` | `2s` | Global minimum spacing between attempts. |
| `BRIDGE_NOTIFY_BATCH_SIZE` | `20` | Maximum sequential claims per cycle. |
| `BRIDGE_NOTIFY_MAX_ATTEMPTS` | `10` | Maximum committed claims per event. |
| `BRIDGE_NOTIFY_MAX_AGE` | `24h` | Notification expiry from original receipt. |
| `BRIDGE_NOTIFY_RETENTION` | `7d` | Terminal-state metadata retention. |

[Hermes incoming notifications](docs/hermes-notifications.md) documents the
metadata-only outbox, validation bounds, identifier privacy, **mandatory actual
read-only receiver capability gate**, backlog retirement and rollback safety.
Deployment remains **PAUSED**; enabling, migration execution and configuration
changes require separate operator approval. Disabled mode loads no relay secret
and performs only bounded minute-wise local cleanup with maintenance defaults.

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

21 tools across five modules (`mcp-server/src/whatsapp_mcp/tools/`) after the schema-v2 context lookup is implemented: messages.py:11, groups.py:4, media.py:3, contacts.py:2, search.py:1.

| Tool | Module | Description |
|---|---|---|
| `get_message` | messages | Retrieve one exact message by actual source chat JID and message ID |
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

26 routes registered in `bridge/api/server.go`, including exact-message lookup. The API binds to `127.0.0.1:8080` by default. It has **no authentication** and must not be exposed off-host.

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
POST   /api/download         (200 on hit; 410 MEDIA_PERMANENTLY_UNAVAILABLE terminal / 503 DOWNLOAD_RETRY_PENDING transient; records a download attempt)
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

PostgreSQL 17 with the `pgvector` extension is the single source of truth. The bridge runs [golang-migrate](https://github.com/golang-migrate/migrate) migrations at startup automatically.

Migrations: 11 pairs — `bridge/migrations/000001`, `000003`–`000012` (`001`, `003`–`012`; no `002`).

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

1. **Poll loop** — reads `messages` where `embedded_at IS NULL AND content <> ''`, encodes in batches, writes `message_embeddings.embedding vector(384)`, stamps `messages.embedded_at`.
2. **On-demand HTTP server** — listens on `EMBED_HTTP_ADDR` (default `127.0.0.1:8000`):
   - `POST /embed`   `{"text":"..."}` -> `{"embedding":[384 floats]}`
   - `GET  /health`  -> `{"status":"ok","model":"...","dim":384}`

Model: `paraphrase-multilingual-MiniLM-L12-v2` (sentence-transformers). Dimension: 384 — matches the `vector(384)` schema column.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL DSN |
| `EMBEDDING_MODEL` | `paraphrase-multilingual-MiniLM-L12-v2` | sentence-transformers model name |
| `EMBED_BATCH_SIZE` | `100` | Messages per embedding batch |
| `EMBED_POLL_INTERVAL` | `30` | Seconds between idle polls |
| `EMBED_HTTP_ADDR` | `127.0.0.1:8000` | Bind address for on-demand /embed + /health |

### Transcriber (L3)

Located in `transcriber/`. Runs two concurrent functions:

1. **Poll loop** — reads `messages_media` where `media_type = 'audio' AND transcribed_at IS NULL AND NOT download_permanently_failed`, re-downloads missing files via the bridge if needed, posts raw audio to whisper.cpp `/inference`, writes `messages_media.transcription`.
2. **On-demand HTTP server** — listens on `ANALYZER_HTTP_ADDR` (default `127.0.0.1:8500`):
   - `POST /analyze` `{"chat_jid":"...","message_id":"..."}` -> `{"frame_paths":[...],"transcription":"...","duration":<float>,"frame_count":<int>}`
   - `GET  /health`  -> `{"status":"ok"}`

The `/analyze` handler downloads the video via the bridge, extracts up to 8 keyframes at 1 fps with ffmpeg, demuxes audio to 16 kHz mono WAV, and transcribes with Whisper. Videos longer than 600 seconds are rejected.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL DSN |
| `BRIDGE_URL` | `http://bridge:8080` | Bridge base URL (for media re-download) |
| `WHISPER_URL` | `http://127.0.0.1:8443` | whisper.cpp server base URL; audio posted to `{WHISPER_URL}/inference` |
| `WHISPER_MODEL` | `large-v3` | Informational only; the model is loaded by whisper-server |
| `WHISPER_LANGUAGE` | `ur` | Informational; the whisper-server auto-detects; set in compose only |
| `TRANSCRIBE_BATCH_SIZE` | `20` | Rows per cycle |
| `TRANSCRIBE_POLL_INTERVAL` | `30` | Seconds between idle polls |
| `ANALYZER_HTTP_ADDR` | `127.0.0.1:8500` | Bind address for on-demand /analyze + /health |

### Media-retry worker (L3)

A bridge-co-resident Go worker (`bridge/mediaretry/`, co-resident because re-download needs the live whatsmeow session). It polls `messages_media` for previously-requested media that failed to download (`download_attempts > 0`, requested-only — captured-but-never-requested media is left untouched), retries at the `MEDIA_RETRY_INTERVAL` cadence up to `MEDIA_MAX_DOWNLOAD_ATTEMPTS`, then sets `download_permanently_failed`. Scope is non-audio media (image, video, sticker, document); audio stays with the transcriber.

### Hybrid Search

`POST /api/search` accepts `{"query":"...","chat_jid":"...","limit":20}`.

When the caller supplies only `query` text, the bridge calls `POST http://embedder:8000/embed` to obtain a query vector at query time, then runs hybrid Reciprocal Rank Fusion over:
- PostgreSQL full-text search (`plainto_tsquery('simple', ...)` on `content_fts`)
- pgvector cosine distance (`<=>`) on `message_embeddings`

If the embedder is unreachable (2-second timeout), search degrades to FTS-only without erroring the request. Results carry a `match_type` field: `"fts"`, `"semantic"`, or `"both"`.

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

The MCP tool returns the result verbatim. The LLM host must **Read** each path in `frame_paths` (they are absolute image files) to inspect visual content.

### Voice Note Duration Fix

`bridge/media/ogg.go` parses the OGG Opus container in-process (no external commands) to derive the real audio duration and a 64-sample waveform from the granule position. Previously sent voice notes reported a hardcoded 30-second duration; they now report the real duration.

---

## Observability

### Dashboard

`dashboard/` is a FastAPI + HTMX application running on port 9090 (default). It is read-only: all SQL is SELECT-only via psycopg2.

**Recent restyle:** Tailwind v3.4 built with the standalone Tailwind CLI (no Node required), Tremor design language color tokens, dark/light mode toggle with no-flash script (respects OS preference and `localStorage.theme`), self-hosted FontAwesome icons served from `/static/fontawesome/`.

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

The MCP server's `TelemetryMiddleware` records every tool call to `POST /api/telemetry/tool` on the bridge, which writes to PostgreSQL:

- `telemetry_daily` — daily rolling counters: `messages_sent`, `messages_received`, `media_downloaded`, `media_sent`, `links_indexed`.
- `telemetry_tool_calls` — per-call rows: `tool_name`, `duration_ms`, `success`, `error_msg`, `called_at`.

---

## Connection Handling

The Go bridge implements a supervised connection state machine in `bridge/connection/`:

- Exponential backoff reconnection: 1s, 2s, 4s, 8s, 16s, 32s, 60s (repeating).
- Handles `TemporaryBan`, `ClientOutdated`, `ConnectFailure`, and `StreamReplaced` events from whatsmeow.
- The 10-minute cooldown marker returns exported `BridgeHoldExit=78`, the only supervisor hold status; panic-like or ordinary status 2 remains a restartable failure.
- Keepalive monitor (`bridge/connection/keepalive.go`) forces reconnect after 3 consecutive timeouts.
- Outbound send routes are rate-limited (token-bucket: ~2 sends/s sustained, burst 5, up to 300 ms human-timing jitter) and are never retried.

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

See [`CLAUDE.md`](CLAUDE.md) for the 6-layer model, the directory-to-layer map, where to place new code, and the hard rules (PostgreSQL-only, schema bridge-owned, no DB driver in L4, send routes never retried).

---

## Acknowledgements

The work done in [PR #1](https://github.com/NIXKnight/WhatsApp-MCP-Server/pull/1) is inspired by the open-source [`asimzeeshan/WhatsApp-bridge`](https://github.com/asimzeeshan/WhatsApp-bridge), reimplemented and extended for this stack.

## License

MIT License. See [LICENSE](LICENSE) for details.
