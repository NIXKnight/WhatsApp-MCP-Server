# WhatsApp Bridge

Go HTTP server that bridges to WhatsApp via [whatsmeow](https://github.com/tulir/whatsmeow) and exposes a REST API. The Python MCP server communicates with this bridge to deliver 12 MCP tools to Claude Code and Claude Desktop.

## Overview

The bridge connects to WhatsApp using the [whatsmeow](https://github.com/tulir/whatsmeow) library (WhatsApp Web protocol), stores messages in SQLite, and exposes a RESTful HTTP API on `127.0.0.1:8080`. It implements robust connection handling with exponential backoff, QR code pairing, and automatic session recovery.

Security: The API has no authentication and must only be accessible on the loopback interface (127.0.0.1 or localhost).

## Prerequisites

- Go 1.25 or later
- CGO enabled (required for `mattn/go-sqlite3` SQLite bindings)
- A WhatsApp account with multi-device support enabled

## Build and Run

### Build

```bash
cd bridge
go build -o whatsapp-bridge .
```

### Run

```bash
./whatsapp-bridge
```

On first run, the bridge enters QR pairing mode. Scan the displayed QR code with your WhatsApp mobile app (Settings → Linked Devices → Link a Device).

After QR scan succeeds, the bridge stays connected. Subsequent runs reconnect automatically without requiring QR re-pairing (session data is persisted in the `bridge/data/` directory).

### Docker Build

```bash
docker compose build bridge
```

The Dockerfile uses a multi-stage Alpine build with CGO enabled for SQLite.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BRIDGE_ADDR` | `127.0.0.1:8080` | TCP address for the HTTP server. Warning: the API has no auth; do not expose to network. |
| `BRIDGE_DATA_DIR` | `./data` | Directory for SQLite databases and session files. Created if missing; must be writable. |
| `BRIDGE_LOG_LEVEL` | `info` | Log verbosity: `debug`, `info`, `warn`, or `error`. Logs are JSON-formatted to stderr. |

Example:

```bash
BRIDGE_LOG_LEVEL=debug BRIDGE_DATA_DIR=/tmp/whatsapp-data ./whatsapp-bridge
```

## REST API Endpoints

All endpoints return JSON. Errors return a `{"error": "...", "code": "..."}` response with appropriate HTTP status codes.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/status` | Connection state, uptime, and message count |
| `GET` | `/api/messages` | List messages with filtering (chat_jid, sender, after, before, query, limit, page) |
| `GET` | `/api/messages/{id}/context` | Context around a single message |
| `GET` | `/api/chats` | List all known chats with pagination |
| `GET` | `/api/chats/{jid}` | Retrieve a single chat by JID |
| `GET` | `/api/contacts` | List all known contacts |
| `GET` | `/api/contacts/{jid}` | Retrieve a single contact by JID |
| `GET` | `/api/groups` | List all group chats |
| `GET` | `/api/groups/{jid}` | Retrieve full group metadata with participants |
| `GET` | `/api/unread` | List chats with unread messages (supports `flat=true` for flat message list) |
| `GET` | `/api/check` | Poll for new messages since timestamp (params: `since`, `limit`) |
| `POST` | `/api/send` | Send a plain-text message to a contact or group |
| `POST` | `/api/send/media` | Send media (image, video, audio, document) with optional caption |
| `POST` | `/api/download` | Download media from a received message |

Query parameters and request bodies use snake_case (e.g., `chat_jid`, `message_id`, `quoted_message_id`).

## QR Code Pairing Flow

1. **First Run**: Bridge starts and tries to load session from `data/` directory.
2. **No Session Found**: Bridge enters `StateQRWaiting`. A QR code is printed to the terminal (in half-block Unicode format for small terminal windows).
3. **Scan the Code**: Open WhatsApp mobile, go to Settings → Linked Devices → Link a Device, and scan the code with your phone.
4. **Success**: Once scanned, the bridge logs `QR code scanned successfully`, transitions to `StateConnected`, and saves the session.
5. **Automatic Reconnect**: On restart, the bridge loads the saved session and reconnects without QR (unless the session becomes invalid).

## Connection State Machine

The bridge implements a robust connection state machine:

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

- **Exponential Backoff**: Failed connections retry with delays of 1s, 2s, 4s, 8s, 16s, 32s, 60s (cycles repeating).
- **Cooldown Marker**: After a permanent disconnect (ban, logout, client outdated), a marker file is written to prevent immediate supervisor restart loops. The bridge exits with code 2, signaling that systemd/Docker should wait before retrying.
- **Keepalive Monitoring**: Forced reconnect after 3 consecutive timeout events.
- **Event Handlers**: Handle `Connected`, `Disconnected`, `LoggedOut`, `TemporaryBan`, `ClientOutdated`, `ConnectFailure`, and `StreamReplaced` events.

## Data Storage

### SQLite Database

Session and message data are stored in `BRIDGE_DATA_DIR/` (default: `./data/`):

- **`device.db`**: Whatsmeow session store (device credentials, key material, etc.)
- **`messages.db`**: Message archive with SQLite WAL mode enabled for better concurrency
  - Tables: `chats`, `messages`, `contacts`, `group_participants`
  - Indexed by chat_jid, timestamp, and sender for efficient querying

Both databases are automatically created on first run.

### WAL Mode

The message store uses SQLite Write-Ahead Logging (WAL mode) for better concurrency and write performance. Two additional files are created alongside `messages.db`:

- `messages.db-wal`: Write-ahead log
- `messages.db-shm`: Shared memory file

These are normal and expected; do not delete them while the bridge is running.

## Project Structure

```
bridge/
├── README.md                   This file
├── go.mod                      Go module definition
├── go.sum                      Dependency checksums
├── main.go                     Entry point (config, cooldown marker, lifecycle)
├── config/
│   └── config.go              Environment variable parsing and validation
├── bridge/
│   ├── client.go              Whatsmeow client initialization and event dispatch
│   ├── connection.go          Connection state machine (QR flow, backoff, reconnect)
│   ├── store.go               SQLite message store
│   └── events.go              Event handlers (Connected, Disconnected, etc.)
├── api/
│   ├── server.go              HTTP server setup, routes, middleware
│   ├── types.go               Request/response JSON structs
│   └── handlers.go            Endpoint handlers (send, download, query, etc.)
├── media/
│   ├── download.go            Media download and decryption
│   ├── upload.go              Media encryption and upload
│   ├── ogg.go                 OGG Opus PTT encoding
│   └── helpers.go             Utility functions
└── data/                       Runtime directory for SQLite and session files
    └── .gitkeep
```

## Graceful Shutdown

The bridge handles `SIGINT` and `SIGTERM` signals:

1. Stops accepting new HTTP requests (15s drain timeout)
2. Closes the WhatsApp client connection
3. Closes the SQLite connection pools
4. Exits cleanly

If a shutdown is requested before the initial WhatsApp connection is established, it exits immediately without writing a cooldown marker.

## Logging

Logs are JSON-formatted and sent to stderr. Each log line includes:

- Timestamp (ISO 8601)
- Level (DEBUG, INFO, WARN, ERROR)
- Component (e.g., "service", "client", "store", "http", "api")
- Message and key-value context

Example:

```json
{"time":"2025-01-23T10:45:30.123456Z","level":"INFO","msg":"WhatsApp connection established","service":"whatsapp-bridge-go"}
```

Control log verbosity via `BRIDGE_LOG_LEVEL`:

```bash
BRIDGE_LOG_LEVEL=debug ./whatsapp-bridge  # Debug: all messages
BRIDGE_LOG_LEVEL=info ./whatsapp-bridge   # Info and above (default)
```

## Troubleshooting

### QR Code Not Appearing

- Ensure your terminal window is wide enough (at least 80 columns).
- Increase `BRIDGE_LOG_LEVEL=debug` and check for errors during connection setup.

### Connection Drops Frequently

- Check network stability.
- WhatsApp may rate-limit repeated reconnections; consider increasing cooldown.
- Confirm the whatsmeow library version is up-to-date (see `go.mod` for current pin).

### "TemporaryBan" or "ClientOutdated" Events

- These are permanent disconnects. The bridge writes a cooldown marker and exits with code 2.
- Wait 10 minutes before restarting the bridge.
- If ClientOutdated persists, check if whatsmeow needs updating: `go get -u go.mau.fi/whatsmeow@latest`

### Port Already in Use

```bash
lsof -i :8080
# or
netstat -tulpn | grep 8080
```

Change `BRIDGE_ADDR` to an available port if needed.

## License

MIT License. See [LICENSE](LICENSE) for details.
