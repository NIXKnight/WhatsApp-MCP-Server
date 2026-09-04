"""FastAPI dashboard application (read-only, L6 operations and observability).

Serves an HTMX-driven operations view of the WhatsApp bridge:
bridge health (via the bridge ``GET /api/status`` endpoint) plus recent
activity read directly from PostgreSQL. Every database access is read-only.
"""

import os
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from whatsapp_dashboard import db

BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://127.0.0.1:8080")
HOST = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
PORT = int(os.environ.get("DASHBOARD_PORT", "9090"))

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

# Optional config.toml is supported only to surface a curated set of monitored
# group JIDs. The target deployment configures the database through
# DATABASE_URL, so config.toml is usually absent; when it is, WATCHED_JIDS is
# empty and the groups panel simply lists all groups.
_CONFIG_PATH = Path(
    os.environ.get(
        "CONFIG_PATH",
        Path(__file__).resolve().parent.parent.parent.parent / "config.toml",
    )
)


def _load_watched_jids() -> list[str]:
    """Read ``bridge.monitoring.watched_group_jids`` from config.toml if present."""
    if not _CONFIG_PATH.exists():
        return []
    try:
        import tomllib
    except ImportError:  # pragma: no cover - Python < 3.11 fallback
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            return []
    with open(_CONFIG_PATH, "rb") as f:
        cfg = tomllib.load(f)
    return cfg.get("bridge", {}).get("monitoring", {}).get("watched_group_jids", [])


WATCHED_JIDS = _load_watched_jids()

app = FastAPI(title="WhatsApp Bridge Dashboard")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


async def fetch_bridge_status() -> dict:
    """Fetch connection status from the Go bridge's ``/api/status`` endpoint."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{BRIDGE_URL}/api/status")
            if resp.status_code == 200:
                return resp.json()
    except Exception:
        pass
    return {"state": "UNREACHABLE", "is_connected": False}


# messages.timestamp is TIMESTAMPTZ in this schema (not epoch millis), so it is
# formatted directly with AT TIME ZONE. media_type lives on the messages row
# itself, so no join is needed for it.
_RECENT_MESSAGES_SELECT = """
    SELECT m.id, m.chat_jid, c.name AS chat_name, m.sender, m.sender_name,
           m.content, m.timestamp,
           TO_CHAR(m.timestamp AT TIME ZONE 'Asia/Karachi',
                   'DD Mon HH12:MI AM') AS time_fmt,
           m.is_from_me,
           COALESCE(m.media_type, '') AS media_type,
           m.push_name,
           COALESCE(NULLIF(ct.name, ''), NULLIF(ct.notify, ''),
                    NULLIF(m.push_name, ''), NULLIF(m.sender_name, ''),
                    m.sender) AS resolved_name
    FROM messages m
    LEFT JOIN chats c ON c.jid = m.chat_jid
    LEFT JOIN contacts ct ON ct.jid = m.sender
"""


def _fetch_recent_messages(watched_jids: list[str]) -> list[dict]:
    if watched_jids:
        placeholders = ",".join(["%s"] * len(watched_jids))
        return db.query(
            _RECENT_MESSAGES_SELECT
            + f" WHERE m.chat_jid IN ({placeholders})"
            + " ORDER BY m.timestamp DESC LIMIT 50",
            tuple(watched_jids),
        )
    return db.query(
        _RECENT_MESSAGES_SELECT + " ORDER BY m.timestamp DESC LIMIT 50"
    )


def _fetch_telemetry() -> dict:
    return db.query_one(
        "SELECT * FROM telemetry_daily WHERE date = CAST(CURRENT_DATE AS TEXT)"
    ) or {
        "date": "today",
        "messages_sent": 0,
        "messages_received": 0,
        "media_downloaded": 0,
        "media_sent": 0,
        "links_indexed": 0,
    }


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the full dashboard page."""
    status = await fetch_bridge_status()

    if WATCHED_JIDS:
        placeholders = ",".join(["%s"] * len(WATCHED_JIDS))
        groups = db.query(
            "SELECT jid, name, is_group, unread_count, last_message_time, "
            "last_message_preview "
            f"FROM chats WHERE jid IN ({placeholders}) "
            "ORDER BY last_message_time DESC NULLS LAST",
            tuple(WATCHED_JIDS),
        )
    else:
        groups = db.query(
            "SELECT jid, name, is_group, unread_count, last_message_time, "
            "last_message_preview "
            "FROM chats WHERE is_group = TRUE "
            "ORDER BY last_message_time DESC NULLS LAST"
        )

    recent_messages = _fetch_recent_messages(WATCHED_JIDS)
    telemetry = _fetch_telemetry()

    links = db.query(
        "SELECT l.*, c.name AS chat_name, "
        "TO_CHAR(l.created_at AT TIME ZONE 'Asia/Karachi', "
        "'DD Mon YYYY HH12:MI AM') AS time_fmt "
        "FROM links l "
        "LEFT JOIN chats c ON c.jid = l.chat_jid "
        "ORDER BY l.timestamp DESC NULLS LAST LIMIT 30"
    )

    tool_calls = db.query(
        "SELECT *, "
        "TO_CHAR(called_at AT TIME ZONE 'Asia/Karachi', "
        "'DD Mon YYYY HH12:MI AM') AS time_fmt "
        "FROM telemetry_tool_calls ORDER BY called_at DESC LIMIT 20"
    )

    platform_counts = db.query(
        "SELECT platform, COUNT(*) AS count FROM links "
        "GROUP BY platform ORDER BY count DESC"
    )

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "status": status,
            "groups": groups,
            "recent_messages": recent_messages,
            "telemetry": telemetry,
            "links": links,
            "tool_calls": tool_calls,
            "platform_counts": platform_counts,
            "watched_count": len(WATCHED_JIDS),
        },
    )


# HTMX partial endpoints for auto-refresh.


@app.get("/partials/status", response_class=HTMLResponse)
async def partial_status(request: Request):
    status = await fetch_bridge_status()
    return templates.TemplateResponse(
        request=request,
        name="partials/status.html",
        context={"status": status},
    )


@app.get("/partials/messages", response_class=HTMLResponse)
async def partial_messages(request: Request):
    recent_messages = _fetch_recent_messages(WATCHED_JIDS)
    return templates.TemplateResponse(
        request=request,
        name="partials/messages.html",
        context={"recent_messages": recent_messages},
    )


@app.get("/partials/telemetry", response_class=HTMLResponse)
async def partial_telemetry(request: Request):
    telemetry = _fetch_telemetry()
    return templates.TemplateResponse(
        request=request,
        name="partials/telemetry.html",
        context={"telemetry": telemetry},
    )


def start():
    """CLI entry point for local development."""
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
