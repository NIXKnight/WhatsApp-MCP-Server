# WhatsApp Bridge Dashboard

A read-only operations dashboard for the WhatsApp bridge. FastAPI serves an
HTMX-driven page that shows bridge health and recent activity. It reads
PostgreSQL directly (queries only — never writes) and polls the bridge's
`/api/status` endpoint for connection state.

## Panels

| Panel | Source | Refresh |
|---|---|---|
| Connection | bridge `GET /api/status` | every 30s (HTMX) |
| Today's Telemetry | `telemetry_daily` | every 30s (HTMX) |
| Monitored Groups | `chats` | on page load |
| Recent Messages | `messages` + `chats` + `contacts` | every 30s (HTMX) |
| Indexed Links | `links` + `chats` | on page load |
| Recent Tool Calls | `telemetry_tool_calls` | on page load |

All timestamps are rendered in `Asia/Karachi`.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL DSN. Primary source. |
| `PG_DSN` | (unset) | Fallback DSN if `DATABASE_URL` is unset. |
| `BRIDGE_URL` | `http://127.0.0.1:8080` | Base URL of the Go bridge. |
| `DASHBOARD_HOST` | `127.0.0.1` | Bind host (the container image sets `0.0.0.0`). |
| `DASHBOARD_PORT` | `9090` | Listen port. |
| `CONFIG_PATH` | `../config.toml` | Optional. If present, `bridge.monitoring.watched_group_jids` curates the Monitored Groups panel. |

When no `config.toml` is found, the Monitored Groups panel lists every group
chat (`chats.is_group = TRUE`).

## Run locally

```bash
cd dashboard
uv sync
DATABASE_URL="postgres://whatsapp:whatsapp@localhost:5432/whatsapp?sslmode=disable" \
  BRIDGE_URL="http://localhost:8080" \
  uv run whatsapp-dashboard
```

Open http://127.0.0.1:9090.

## Run with Docker Compose

From the repository root the `dashboard` service is wired to start after
PostgreSQL and the bridge are healthy:

```bash
docker compose up -d dashboard
```

Open http://localhost:9090.

## Read-only guarantee

`db.py` opens every pooled connection with `set_session(readonly=True)`, so any
accidental write fails at the database. The application issues `SELECT`
statements exclusively.
