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

Environment variables (CLI args take precedence):
    DATABASE_URL        - PostgreSQL DSN (bridge convention; required)
    PG_DSN              - fallback DSN if DATABASE_URL is unset
    BRIDGE_URL          - bridge API base for re-download (default: http://bridge:8080)
    WHISPER_URL         - whisper.cpp whisper-server base URL
                          (default: http://127.0.0.1:8443); the worker POSTs
                          audio to ``{WHISPER_URL}/inference``
    TRANSCRIBE_BATCH_SIZE    - rows per cycle (default: 20)
    TRANSCRIBE_POLL_INTERVAL - seconds between polls when idle (default: 30)
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
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
