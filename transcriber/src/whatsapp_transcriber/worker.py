"""Transcription worker (L3 enrichment): polls PostgreSQL for untranscribed audio, calls a persistent whisper.cpp ``whisper-server`` over HTTP, updates DB.

The database schema is owned by the bridge migrations (``bridge/migrations``).
This worker is strictly a data plane: it never creates tables or alters columns.
It reads/writes the bridge-owned ``messages_media`` table only.

Transcription backend:
  Audio is transcribed by a persistent whisper.cpp ``whisper-server`` reached
  over HTTP (``WHISPER_URL``, default ``http://127.0.0.1:8443``). The model
  (ggml-large-v3) stays resident in the server process, so each request is a
  warm inference — no per-file 3 GB model reload. The worker POSTs the raw
  audio file to ``/inference`` as multipart/form-data; the server runs with
  ``--convert`` and ffmpeg-decodes ogg/opus/etc. server-side, so the file is
  sent as-is with NO client-side pre-conversion. Language auto-detection is
  left to the server (chats are Urdu/English/Punjabi).

Resilience:
  * If a transcription request fails with a connection error (server not up, or
    the model is still loading), treat it as transient: back off and retry on a
    later cycle. A down/warming server never permanent-fails a row.
  * If the local audio file is missing, re-download via the bridge API.
  * Flip ``download_permanently_failed`` on terminal HTTP codes (403/404/410) or
    after N attempts so dead media (expired WhatsApp blobs) leaves the pool.

Target schema (bridge-owned ``messages_media``):
    message_id, chat_jid, media_type, mime_type, url, direct_path,
    local_path, download_attempts, download_last_error,
    download_last_attempt_at, download_permanently_failed,
    transcription, transcription_lang, transcribed_at

Usage:
    whatsapp-transcriber --pg "$DATABASE_URL" --whisper-url "$WHISPER_URL"

On-demand video analysis:
    Besides the poll loop, the worker exposes a small standard-library HTTP
    server for on-demand video extraction (the visual verdict is the caller's
    job — this only does the mechanical work):
        POST /analyze {"chat_jid": "...", "message_id": "..."}
            -> {"frame_paths": [...abs...], "transcription": "...",
                "duration": <float>, "frame_count": <int>}
        GET  /health  -> {"status": "ok"}
    It downloads the video via the bridge, samples keyframes (1 fps) and demuxes
    the audio with ffmpeg, transcribes the WAV with the same Whisper call, and
    returns the frame paths + transcript. It is stateless and never touches the
    poll loop's DB connection.

Environment variables (CLI args take precedence):
    DATABASE_URL        - PostgreSQL DSN (bridge convention; required)
    PG_DSN              - fallback DSN if DATABASE_URL is unset
    BRIDGE_URL          - bridge API base for re-download (default: http://bridge:8080)
    WHISPER_URL         - whisper.cpp whisper-server base URL
                          (default: http://127.0.0.1:8443); the worker POSTs
                          audio to ``{WHISPER_URL}/inference``
    TRANSCRIBE_BATCH_SIZE    - rows per cycle (default: 20)
    TRANSCRIBE_POLL_INTERVAL - seconds between polls when idle (default: 30)
    ANALYZER_HTTP_ADDR  - /analyze + /health bind address (default: 127.0.0.1:8500)
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import FrameType

import httpx
import psycopg2

logger = logging.getLogger("whatsapp_transcriber")

DEFAULT_WHISPER_URL = "http://127.0.0.1:8443"
DEFAULT_BRIDGE_URL = "http://bridge:8080"
DEFAULT_BATCH_SIZE = 20
DEFAULT_POLL_INTERVAL = 30
MAX_DOWNLOAD_ATTEMPTS = 8
MAX_BACKOFF = 300  # 5 minutes
RECONNECT_BACKOFF = 5
TRANSCRIBE_TIMEOUT = 300  # generous: whisper.cpp on a long voice note

# --- On-demand video analysis (auxiliary HTTP endpoint) -------------------
# Bind address for the auxiliary /analyze + /health server. Loopback by
# default so the extraction endpoint is not exposed off-host.
DEFAULT_ANALYZER_HTTP_ADDR = "127.0.0.1:8500"
# Caps for the mechanical extraction: keep work bounded so an accidental call
# on a multi-hour video cannot wedge ffmpeg or fill the disk.
ANALYZE_MAX_DURATION_S = 600  # reject videos longer than 10 minutes
ANALYZE_MAX_FRAMES = 8        # at fps=1, the first N seconds as keyframes
FFMPEG_TIMEOUT = 300          # ceiling on any single ffmpeg/ffprobe call

# Sentinel: a present-but-unprocessable audio file (server rejects the input as
# undecodable). Distinct from None (transient server failure -> retry) so the
# loop gives up instead of retrying forever.
UNPROCESSABLE = object()

running = True
consecutive_failures = 0


def handle_signal(signum: int, frame: FrameType | None) -> None:
    global running
    logger.info("received signal %s, shutting down...", signum)
    running = False


def resolve_dsn(cli_value: str) -> str:
    """Resolve the connection string, honoring the bridge's DATABASE_URL convention.

    Precedence: explicit CLI value > DATABASE_URL > PG_DSN.
    """
    return cli_value or os.environ.get("DATABASE_URL", "") or os.environ.get("PG_DSN", "")


def connect(dsn: str) -> "psycopg2.extensions.connection":
    """Open an autocommit connection (avoids idle-in-transaction between polls)."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def fetch_untranscribed(pg_conn: "psycopg2.extensions.connection", batch_size: int) -> list[dict]:
    """Fetch audio rows that need transcription.

    Skips rows whose download has permanently failed — those are expired
    WhatsApp media that no number of retries will recover.

    NOTE vs reference: the bridge schema has no ``is_revoked`` column, and the
    re-download material column is ``url`` (not ``media_url``). Both are adapted.
    """
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT mm.message_id AS id, mm.chat_jid,
                   mm.local_path, mm.url, mm.direct_path,
                   mm.file_length, mm.mime_type
            FROM messages_media mm
            JOIN messages m ON m.id = mm.message_id AND m.chat_jid = mm.chat_jid
            WHERE mm.media_type = 'audio'
              AND mm.transcribed_at IS NULL
              AND NOT mm.download_permanently_failed
            ORDER BY m.timestamp DESC
            LIMIT %s
            """,
            (batch_size,),
        )
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def record_redownload_failure(
    pg_conn: "psycopg2.extensions.connection",
    msg_id: str,
    chat_jid: str,
    error_text: str,
) -> None:
    """Bump download_attempts; flip permanently_failed on terminal codes or after N attempts.

    Terminal HTTP codes (403/404/410) mean the WhatsApp blob is gone — stop
    hammering bridge/WhatsApp for dead media.
    """
    permanent_patterns = (
        "status code 403",
        "status code 404",
        "status code 410",
        "HTTP 403",
        "HTTP 404",
        "HTTP 410",
        "MISSING_MEDIA_INFO",
        "NOT_FOUND",
    )
    permanent = any(p in error_text for p in permanent_patterns)
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE messages_media SET
                download_attempts = CASE WHEN %s THEN %s
                                         ELSE download_attempts + 1 END,
                download_last_attempt_at = NOW(),
                download_last_error = LEFT(%s, 500),
                download_permanently_failed = CASE
                    WHEN %s THEN TRUE
                    WHEN download_attempts + 1 >= %s THEN TRUE
                    ELSE download_permanently_failed END
            WHERE message_id = %s AND chat_jid = %s
            """,
            (
                permanent,
                MAX_DOWNLOAD_ATTEMPTS,
                error_text or "",
                permanent,
                MAX_DOWNLOAD_ATTEMPTS,
                msg_id,
                chat_jid,
            ),
        )


def transcribe_file(file_path: str, whisper_url: str):
    """Transcribe audio via the persistent whisper.cpp ``whisper-server``.

    POSTs the raw audio file to ``{whisper_url}/inference`` as
    multipart/form-data (``file``, ``response_format=json``,
    ``temperature=0.0``). The server runs with ``--convert`` and ffmpeg-decodes
    the input server-side, so the file is sent as-is — no pre-conversion here.
    Success response is ``{"text": " ...\\n"}``; we return ``text.strip()``.

    Returns:
        str          - transcription text (possibly empty for silence)
        None         - transient failure (server down / model loading / timeout
                       / 5xx) -> retry on a later cycle. A connection error is
                       explicitly transient so a warming server never wedges.
        UNPROCESSABLE- server rejected the input as undecodable (4xx that is not
                       a transient condition) -> give up so the row leaves the
                       pending pool instead of retrying forever.
    """
    if not os.path.isfile(file_path):
        return None

    try:
        with open(file_path, "rb") as audio:
            resp = httpx.post(
                f"{whisper_url}/inference",
                files={"file": (os.path.basename(file_path), audio, "application/octet-stream")},
                data={"response_format": "json", "temperature": "0.0"},
                timeout=TRANSCRIBE_TIMEOUT,
            )
    except httpx.ConnectError as e:
        # Server not up yet, or the 3 GB model is still loading. Transient by
        # definition — do NOT permanent-fail; retry on a later cycle.
        logger.warning("whisper-server unreachable (%s): %s", whisper_url, e)
        return None
    except httpx.TimeoutException:
        logger.warning("transcription timed out after %ds: %s", TRANSCRIBE_TIMEOUT, file_path)
        return None
    except httpx.HTTPError as e:
        logger.warning("transcription request error: %s", e)
        return None

    if resp.status_code >= 500:
        # Server-side fault (e.g. transient model/decoder hiccup) — retry later.
        logger.warning(
            "whisper-server %d for %s: %s",
            resp.status_code,
            file_path,
            resp.text[:500],
        )
        return None
    if resp.status_code >= 400:
        # Client error: the server could not decode/accept this input. The file
        # itself will never transcribe — mark unprocessable so it drains.
        logger.warning(
            "whisper-server rejected %s (%d): %s",
            file_path,
            resp.status_code,
            resp.text[:500],
        )
        return UNPROCESSABLE

    try:
        data = resp.json()
    except Exception as e:
        logger.warning("whisper-server returned non-JSON for %s: %s", file_path, e)
        return None

    return (data.get("text") or "").strip()


def redownload_audio(msg_id: str, chat_jid: str, bridge_url: str) -> tuple[str | None, str]:
    """Re-download audio via the bridge ``POST /api/download`` when the local file is missing.

    Request body matches the bridge DownloadRequest struct
    (bridge/api/types.go): ``{message_id, chat_jid}``. ``output_dir`` is omitted
    deliberately — the bridge then writes into its own data dir, and the worker
    (same host/user as the bridge) reads the returned ``file_path`` directly.
    Response is DownloadResponse: ``{file_path, media_type, file_size}``.

    Returns (file_path, "") on success, (None, "<error>") on failure. The error
    text feeds record_redownload_failure so terminal HTTP codes flip
    download_permanently_failed.
    """
    try:
        resp = httpx.post(
            f"{bridge_url}/api/download",
            json={"message_id": msg_id, "chat_jid": chat_jid},
            timeout=60.0,
        )
        if resp.status_code >= 400:
            # Surface the bridge's response body — it carries the upstream code we
            # use to classify permanent failures.
            try:
                body = resp.json()
                err = f"HTTP {resp.status_code}: {body}"
            except Exception:
                err = f"HTTP {resp.status_code}: {resp.text[:200]}"
            return (None, err)
        data = resp.json()
        return (data.get("file_path"), "")
    except Exception as e:
        return (None, f"request_error: {e}")


def _run_ffmpeg(args: list[str], what: str) -> tuple[bool, str]:
    """Run an ffmpeg/ffprobe command (list-form, no shell) with a timeout.

    ``-nostdin`` is included by callers so ffmpeg never blocks waiting on a
    controlling terminal. stdout/stderr are captured (not streamed) so request
    bodies / file contents never reach the access log. Returns (ok, stderr_tail)
    where stderr_tail is bounded and safe to log (ffmpeg diagnostics only — no
    secrets, no message content).
    """
    try:
        proc = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=FFMPEG_TIMEOUT,
            check=False,
        )
    except FileNotFoundError:
        return (False, f"{what}: binary not found")
    except subprocess.TimeoutExpired:
        return (False, f"{what}: timed out after {FFMPEG_TIMEOUT}s")
    if proc.returncode != 0:
        tail = (proc.stderr or b"").decode("utf-8", "replace")[-500:]
        return (False, f"{what}: exit {proc.returncode}: {tail}")
    return (True, "")


def probe_duration(path: str) -> float:
    """Return the media duration in seconds via ``ffprobe``, or 0.0 if unknown.

    ``ffprobe -v error -show_entries format=duration -of default=nk=1:nw=1``
    prints just the bare float. A missing/garbage value (some containers omit a
    format-level duration) maps to 0.0 — the caller treats 0.0 as "unknown",
    not "instant", so it neither rejects nor trusts a phantom length.
    """
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nk=1:nw=1",
                path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=FFMPEG_TIMEOUT,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return 0.0
    raw = (proc.stdout or b"").decode("utf-8", "replace").strip()
    try:
        return float(raw)
    except ValueError:
        return 0.0


def extract_frames(video: str, out_dir: str, max_frames: int) -> list[str]:
    """Sample up to ``max_frames`` keyframes (1 fps) as JPEGs into ``out_dir``.

    ``ffmpeg -nostdin -y -i <video> -vf fps=1 -frames:v <max_frames> -q:v 3
    <out_dir>/frame_%03d.jpg`` — one frame per second, capped, JPEG quality 3.
    Returns the SORTED list of files actually produced (ffmpeg may emit fewer
    than ``max_frames`` for a short clip). A non-zero exit still returns
    whatever frames landed on disk, so a partial extraction is usable.
    """
    os.makedirs(out_dir, exist_ok=True)
    pattern = os.path.join(out_dir, "frame_%03d.jpg")
    ok, err = _run_ffmpeg(
        [
            "ffmpeg",
            "-nostdin",
            "-y",
            "-i",
            video,
            "-vf",
            "fps=1",
            "-frames:v",
            str(max_frames),
            "-q:v",
            "3",
            pattern,
        ],
        "extract_frames",
    )
    if not ok:
        logger.warning("frame extraction issue: %s", err)
    return sorted(glob.glob(os.path.join(out_dir, "frame_*.jpg")))


def extract_audio_wav(video: str, out_dir: str) -> str | None:
    """Demux the audio track to 16 kHz mono WAV (whisper.cpp's native rate).

    ``ffmpeg -nostdin -y -i <video> -vn -ac 1 -ar 16000 <out_dir>/audio.wav``.
    Returns the WAV path on success, or None when the video carries no audio
    stream / ffmpeg fails (a silent or video-only clip is not an error — the
    caller returns an empty transcript).
    """
    os.makedirs(out_dir, exist_ok=True)
    wav = os.path.join(out_dir, "audio.wav")
    ok, err = _run_ffmpeg(
        [
            "ffmpeg",
            "-nostdin",
            "-y",
            "-i",
            video,
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            wav,
        ],
        "extract_audio_wav",
    )
    if not ok or not os.path.isfile(wav):
        logger.warning("audio extraction issue: %s", err)
        return None
    return wav


def analyze_video(
    chat_jid: str,
    message_id: str,
    bridge_url: str,
    whisper_url: str,
) -> dict:
    """Download a video and mechanically extract keyframes + an audio transcript.

    Pipeline (all stateless — no DB, no shared connection):
      1. Re-download the media via the bridge (``redownload_audio`` is media-type
         agnostic; it returns the on-disk path of any downloaded media).
      2. ``probe_duration``; reject anything longer than ``ANALYZE_MAX_DURATION_S``.
      3. ``extract_frames`` (1 fps, capped at ``ANALYZE_MAX_FRAMES``).
      4. ``extract_audio_wav`` then ``transcribe_file`` on the WAV.

    Returns a dict that is the wire contract:
        {"frame_paths": [<abs str>...], "transcription": <str>,
         "duration": <float>, "frame_count": <int>}
    On a hard failure (download produced nothing) returns
        {"error": <str>, "status": <int>}
    so the handler can map it to a 4xx/5xx. Soft failures (no audio, transcript
    timeout, partial frames) degrade gracefully into the success shape with an
    empty transcript and whatever frames were produced.
    """
    if not chat_jid or not message_id:
        return {"error": "chat_jid and message_id are required", "status": 400}

    # 1. Download the video bytes via the bridge. The function name says "audio"
    #    but it POSTs {message_id, chat_jid} and returns any media's path.
    local_path, err = redownload_audio(message_id, chat_jid, bridge_url)
    if not local_path or not os.path.isfile(local_path):
        # Hard failure: nothing to extract from.
        return {"error": f"download failed: {err or 'no file path returned'}", "status": 502}

    # 2. Duration gate. 0.0 == unknown (container omitted it) -> allow.
    duration = probe_duration(local_path)
    if duration > ANALYZE_MAX_DURATION_S:
        return {
            "error": (
                f"video too long: {duration:.0f}s exceeds "
                f"{ANALYZE_MAX_DURATION_S}s cap"
            ),
            "status": 422,
        }

    # Write frames + extracted audio NEXT TO the downloaded video, in the
    # directory the bridge already wrote it into. That directory is guaranteed
    # writable (the bridge just created the file there; under systemd both run
    # as the same user, under compose both share the bridge-data volume). This
    # avoids depending on a separate REDOWNLOAD_DIR/DEFAULT_DATA_ROOT that may
    # point at a non-existent, non-writable container path (e.g. /data) on a
    # systemd-from-source host.
    safe_message_id = re.sub(r"[^A-Za-z0-9._-]", "_", str(message_id))
    out_dir = os.path.join(os.path.dirname(local_path), f"analyze_{safe_message_id}")
    os.makedirs(out_dir, exist_ok=True)

    # 3. Frames. extract_frames returns whatever landed even on partial failure.
    frame_paths = [os.path.abspath(p) for p in extract_frames(local_path, out_dir, ANALYZE_MAX_FRAMES)]

    # 4. Audio -> transcript. Any failure here yields an empty transcript rather
    #    than failing the whole call (the frames are still useful).
    transcription = ""
    wav = extract_audio_wav(local_path, out_dir)
    if wav:
        result = transcribe_file(wav, whisper_url)
        if isinstance(result, str):
            transcription = result
        # None (transient) / UNPROCESSABLE (undecodable) -> leave transcript "".

    return {
        "frame_paths": frame_paths,
        "transcription": transcription,
        "duration": duration,
        "frame_count": len(frame_paths),
    }


def make_analyze_handler(
    bridge_url: str, whisper_url: str
) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to the bridge/whisper URLs.

    The handler is fully stateless: it only calls ``analyze_video`` (download +
    ffmpeg + Whisper over HTTP). It never touches the poll loop's psycopg2
    connection (psycopg2 connections are not thread-safe). Access logging is
    suppressed because request bodies carry ``chat_jid``/``message_id`` and must
    never reach the log (§14).
    """

    class AnalyzeHandler(BaseHTTPRequestHandler):
        def _send_json(self, code: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            # Suppress the default stderr access log: request lines would echo
            # chat_jid/message_id and we must never log message identifiers (§14).
            return

        def do_POST(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
            try:
                if self.path != "/analyze":
                    self._send_json(404, {"error": "not found"})
                    return

                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length > 0 else b""
                try:
                    payload = json.loads(raw) if raw else {}
                except (ValueError, UnicodeDecodeError):
                    self._send_json(400, {"error": "invalid JSON body"})
                    return
                if not isinstance(payload, dict):
                    self._send_json(400, {"error": "body must be a JSON object"})
                    return

                chat_jid = payload.get("chat_jid")
                message_id = payload.get("message_id")
                if not isinstance(chat_jid, str) or not chat_jid:
                    self._send_json(400, {"error": "missing or empty 'chat_jid'"})
                    return
                if not isinstance(message_id, str) or not message_id:
                    self._send_json(400, {"error": "missing or empty 'message_id'"})
                    return

                result = analyze_video(
                    chat_jid, message_id, bridge_url, whisper_url
                )
                if "error" in result:
                    status = int(result.get("status", 500))
                    self._send_json(status, {"error": result["error"]})
                    return

                self._send_json(200, result)
            except Exception:
                # Never let a handler exception kill the server thread. Do not
                # log the request body (it carries chat_jid/message_id).
                logger.exception("analyze request failed")
                try:
                    self._send_json(500, {"error": "analysis failed"})
                except Exception:
                    pass

        def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
            try:
                if self.path != "/health":
                    self._send_json(404, {"error": "not found"})
                    return
                self._send_json(200, {"status": "ok"})
            except Exception:
                logger.exception("health request failed")

    return AnalyzeHandler


def start_analyze_server(bridge_url: str, whisper_url: str) -> None:
    """Start the auxiliary /analyze + /health HTTP server in a daemon thread.

    Bind address comes from ``ANALYZER_HTTP_ADDR`` (``host:port``, default
    ``127.0.0.1:8500``). The server is auxiliary: a bind failure is logged but
    must NOT stop the poll loop, which is the worker's primary function. The
    thread is a daemon so process shutdown (the global ``running`` flag path) is
    unaffected.
    """
    addr = os.environ.get("ANALYZER_HTTP_ADDR", DEFAULT_ANALYZER_HTTP_ADDR)
    try:
        host, _, port_str = addr.rpartition(":")
        if not host or not port_str:
            raise ValueError(f"expected host:port, got {addr!r}")
        port = int(port_str)
    except ValueError as e:
        logger.error("invalid ANALYZER_HTTP_ADDR (%s); analyze http server disabled", e)
        return

    handler_cls = make_analyze_handler(bridge_url, whisper_url)
    try:
        httpd = ThreadingHTTPServer((host, port), handler_cls)
    except OSError as e:
        logger.error(
            "analyze http server bind failed on %s (%s); continuing without it", addr, e
        )
        return

    thread = threading.Thread(target=httpd.serve_forever, name="analyze-http", daemon=True)
    thread.start()
    logger.info("analyze http server listening on %s", addr)


def update_transcription(
    pg_conn: "psycopg2.extensions.connection",
    msg_id: str,
    chat_jid: str,
    text: str,
    lang: str | None = None,
) -> None:
    """Write the transcription back to messages_media."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE messages_media
               SET transcription = %s,
                   transcription_lang = COALESCE(%s, transcription_lang),
                   transcribed_at = NOW()
             WHERE message_id = %s AND chat_jid = %s
            """,
            (text, lang, msg_id, chat_jid),
        )


def update_local_path(
    pg_conn: "psycopg2.extensions.connection", msg_id: str, chat_jid: str, local_path: str
) -> None:
    """After a successful re-download, persist the new path."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE messages_media
               SET local_path = %s, downloaded_at = NOW()
             WHERE message_id = %s AND chat_jid = %s
            """,
            (local_path, msg_id, chat_jid),
        )


def run_loop(
    pg_dsn: str,
    whisper_url: str,
    bridge_url: str,
    batch_size: int,
    poll_interval: int,
) -> None:
    """Main transcription loop."""
    global consecutive_failures

    pg_conn = connect(pg_dsn)

    logger.info(
        "transcription worker started (whisper_url=%s, bridge=%s, poll=%ds)",
        whisper_url,
        bridge_url,
        poll_interval,
    )

    # Auxiliary on-demand video-analysis endpoint (frames + audio transcript).
    # Stateless (download + ffmpeg + Whisper); it never shares pg_conn. Bind
    # failure logs but never blocks the poll loop below. Frames/audio are written
    # next to the bridge-downloaded video, so no shared data-root env is needed.
    start_analyze_server(bridge_url, whisper_url)

    while running:
        try:
            # Repeated request failures (server down, model loading, GPU
            # contention) back off exponentially so we do not spin on a backend
            # that is not ready yet.
            if consecutive_failures >= 3:
                backoff = min(poll_interval * (2 ** (consecutive_failures - 2)), MAX_BACKOFF)
                logger.warning(
                    "%d consecutive transcription failures, backing off %ds",
                    consecutive_failures,
                    backoff,
                )
                time.sleep(backoff)
                consecutive_failures = 0
                continue

            messages = fetch_untranscribed(pg_conn, batch_size)
            if not messages:
                time.sleep(poll_interval)
                continue

            transcribed = 0
            failed = 0

            for msg in messages:
                if not running:
                    break

                msg_id = msg["id"]
                chat_jid = msg["chat_jid"]
                local_path = msg.get("local_path") or ""

                # Prefer the on-disk file; re-download via bridge when missing.
                if not local_path or not os.path.isfile(local_path):
                    new_path, err = redownload_audio(msg_id, chat_jid, bridge_url)
                    if new_path:
                        local_path = new_path
                        update_local_path(pg_conn, msg_id, chat_jid, local_path)
                    else:
                        record_redownload_failure(pg_conn, msg_id, chat_jid, err)
                        logger.debug("skip %s - re-download failed: %s", msg_id, err)
                        failed += 1
                        continue

                text = transcribe_file(local_path, whisper_url)
                if text is UNPROCESSABLE:
                    # Present file the server cannot decode — give up so it
                    # leaves the pool instead of retrying forever.
                    update_transcription(pg_conn, msg_id, chat_jid, "[unprocessable]")
                    logger.warning("marked unprocessable: %s", msg_id)
                    consecutive_failures = 0
                    transcribed += 1
                    continue
                if text is None:
                    consecutive_failures += 1
                    failed += 1
                    if consecutive_failures >= 3:
                        logger.warning("3 consecutive failures, backing off")
                        break
                    continue

                consecutive_failures = 0

                if text:
                    # transcription_lang left NULL: the server auto-detects and
                    # does not report the detected language back to us.
                    update_transcription(pg_conn, msg_id, chat_jid, text)
                    logger.info("transcribed %s: %s", msg_id, text[:80])
                    transcribed += 1
                else:
                    # Empty text (silence / no speech) — stamp so it is not
                    # retried and leaves the pending pool.
                    update_transcription(pg_conn, msg_id, chat_jid, "[no speech]")
                    transcribed += 1

            if transcribed or failed:
                logger.info("cycle: %d transcribed, %d failed", transcribed, failed)

            if failed == len(messages):
                time.sleep(poll_interval)

        except psycopg2.OperationalError:
            logger.warning("PostgreSQL connection lost, reconnecting...")
            try:
                pg_conn.close()
            except Exception:
                pass
            time.sleep(RECONNECT_BACKOFF)
            try:
                pg_conn = connect(pg_dsn)
            except Exception as e:
                logger.error("reconnect failed: %s", e)
                time.sleep(poll_interval)
        except Exception as e:
            logger.error("worker error: %s", e, exc_info=True)
            time.sleep(poll_interval)

    try:
        pg_conn.close()
    except Exception:
        pass
    logger.info("transcription worker stopped")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    parser = argparse.ArgumentParser(description="WhatsApp voice note transcription worker")
    parser.add_argument("--pg", default="")
    parser.add_argument(
        "--whisper-url",
        default=os.environ.get("WHISPER_URL", DEFAULT_WHISPER_URL),
        help="whisper.cpp whisper-server base URL; audio is POSTed to /inference",
    )
    parser.add_argument("--bridge", default=os.environ.get("BRIDGE_URL", DEFAULT_BRIDGE_URL))
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("TRANSCRIBE_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=int(os.environ.get("TRANSCRIBE_POLL_INTERVAL", DEFAULT_POLL_INTERVAL)),
    )
    args = parser.parse_args()

    dsn = resolve_dsn(args.pg)
    if not dsn:
        logger.error("no database DSN: set DATABASE_URL (or PG_DSN), or pass --pg")
        sys.exit(2)

    run_loop(
        dsn,
        args.whisper_url,
        args.bridge,
        args.batch_size,
        args.poll_interval,
    )


if __name__ == "__main__":
    main()
