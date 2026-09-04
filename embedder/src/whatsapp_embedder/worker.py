"""Embedding worker (L3 enrichment): reads messages from PostgreSQL, embeds them, stores vectors in pgvector.

The database schema is owned by the bridge migrations (``bridge/migrations``).
This worker is strictly a data plane: it never creates tables, alters columns, or
creates extensions. It reads ``messages``, writes ``message_embeddings``, and
stamps ``messages.embedded_at``.

Target schema (bridge-owned):
    messages           (id, chat_jid, content, timestamp, embedded_at, ...)
    message_embeddings (message_id, chat_jid, embedding vector(384), created_at)

Usage:
    whatsapp-embedder --pg "$DATABASE_URL"

It also exposes a small standard-library HTTP server so the bridge can embed a
search query at query time with the SAME warm model (parity with stored vectors):
    POST /embed  {"text": "..."} -> {"embedding": [..384 floats..]}
    GET  /health                 -> {"status": "ok", "model": "...", "dim": 384}

Environment variables (CLI args take precedence):
    DATABASE_URL        - PostgreSQL DSN (bridge convention; required)
    PG_DSN              - fallback DSN if DATABASE_URL is unset
    EMBEDDING_MODEL     - sentence-transformers model (default: paraphrase-multilingual-MiniLM-L12-v2)
    EMBED_BATCH_SIZE    - messages per batch (default: 100)
    EMBED_POLL_INTERVAL - seconds between polls when idle (default: 30)
    EMBED_HTTP_ADDR     - embed/health HTTP bind address (default: 127.0.0.1:8000)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import FrameType

import psycopg2
from sentence_transformers import SentenceTransformer

logger = logging.getLogger("whatsapp_embedder")

# 384-dim multilingual model — matches message_embeddings.embedding vector(384).
DEFAULT_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
DEFAULT_BATCH_SIZE = 100
DEFAULT_POLL_INTERVAL = 30
RECONNECT_BACKOFF = 5
EMBEDDING_DIM = 384
DEFAULT_HTTP_ADDR = "127.0.0.1:8000"

# Graceful shutdown flag, flipped by signal handlers.
running = True


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
    """Open an autocommit connection.

    Autocommit avoids leaving the connection idle-in-transaction between polls,
    which would pin a snapshot and bloat the bridge's write path.
    """
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def skip_non_embeddable(pg_conn: "psycopg2.extensions.connection") -> int:
    """Stamp messages that have nothing to embed so they leave the pending pool.

    Empty-content rows (media-only messages, system events) would otherwise sit
    in the un-embedded pool forever and trip supervisor alerts. We stamp
    ``embedded_at`` without inserting an embedding row — which is correct, they
    have no text to embed.

    NOTE: the bridge schema has no ``is_revoked`` column, so the reference
    worker's ``OR is_revoked = TRUE`` predicate is intentionally dropped.
    """
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE messages SET embedded_at = NOW()
            WHERE embedded_at IS NULL
              AND content = ''
            """
        )
        return cur.rowcount


def fetch_unembedded(pg_conn: "psycopg2.extensions.connection", batch_size: int) -> list[dict]:
    """Fetch messages with text that have not been embedded yet."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, chat_jid, content
            FROM messages
            WHERE embedded_at IS NULL
              AND content <> ''
            ORDER BY timestamp ASC
            LIMIT %s
            """,
            (batch_size,),
        )
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def encode_texts(model: SentenceTransformer, texts: list[str]):
    """Encode a batch of texts into embedding vectors.

    This is the single parity-critical call. Both the stored-message path
    (``embed_batch``) and the query-time HTTP endpoint (``/embed``) route through
    this function so a search query is embedded by the EXACT same model and
    normalization that produced the stored vectors. Default MEAN pooling, no
    ``normalize_embeddings``, no prefix, no instruction — do not change these args
    without re-embedding every stored vector.

    Returns the raw ``SentenceTransformer.encode`` output (a numpy ndarray of
    shape ``(len(texts), dim)``); callers iterate rows.
    """
    return model.encode(texts, show_progress_bar=False)


def embed_batch(
    model: SentenceTransformer,
    pg_conn: "psycopg2.extensions.connection",
    messages: list[dict],
) -> None:
    """Embed a batch and persist vectors, then stamp the source messages.

    Insert targets the bridge-owned ``message_embeddings`` columns only:
    (message_id, chat_jid, embedding). ``created_at`` defaults to NOW() in the
    schema. ON CONFLICT DO NOTHING keeps re-runs idempotent.
    """
    texts = [m["content"] for m in messages]
    embeddings = encode_texts(model, texts)

    with pg_conn.cursor() as cur:
        for msg, embedding in zip(messages, embeddings):
            vec_str = "[" + ",".join(str(float(v)) for v in embedding) + "]"
            cur.execute(
                """
                INSERT INTO message_embeddings (message_id, chat_jid, embedding)
                VALUES (%s, %s, %s::vector)
                ON CONFLICT (message_id, chat_jid) DO NOTHING
                """,
                (msg["id"], msg["chat_jid"], vec_str),
            )
            cur.execute(
                "UPDATE messages SET embedded_at = NOW() WHERE id = %s AND chat_jid = %s",
                (msg["id"], msg["chat_jid"]),
            )

    logger.info("embedded %d messages", len(messages))


def make_embed_handler(
    model: SentenceTransformer, model_name: str
) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to the warm, already-loaded model.

    The handler closes over the in-memory ``model`` so query embedding reuses the
    same loaded weights as the poll loop — it never re-loads the model and never
    touches the database. ``SentenceTransformer.encode`` is thread-safe for
    inference, so the daemon HTTP thread and the poll loop may both call
    ``encode_texts`` concurrently without a lock.
    """

    class EmbedHandler(BaseHTTPRequestHandler):
        def _send_json(self, code: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            # Suppress the default stderr access log: it would echo request lines
            # and we must never risk logging message/query content (§14).
            return

        def do_POST(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
            if self.path != "/embed":
                self._send_json(404, {"error": "not found"})
                return

            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length > 0 else b""
            try:
                payload = json.loads(raw) if raw else {}
            except (ValueError, UnicodeDecodeError):
                self._send_json(400, {"error": "invalid JSON body"})
                return

            text = payload.get("text") if isinstance(payload, dict) else None
            if not isinstance(text, str) or not text:
                self._send_json(400, {"error": "missing or empty 'text'"})
                return

            try:
                vector = encode_texts(model, [text])[0]
                embedding = [float(v) for v in vector]
            except Exception:
                # Do not crash the server thread; do not log the query text.
                logger.exception("embed request failed")
                self._send_json(500, {"error": "embedding failed"})
                return

            self._send_json(200, {"embedding": embedding})

        def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
            if self.path != "/health":
                self._send_json(404, {"error": "not found"})
                return
            self._send_json(
                200, {"status": "ok", "model": model_name, "dim": EMBEDDING_DIM}
            )

    return EmbedHandler


def start_http_server(model: SentenceTransformer, model_name: str) -> None:
    """Start the auxiliary embed/health HTTP server in a daemon thread.

    Bind address comes from ``EMBED_HTTP_ADDR`` (``host:port``, default
    ``127.0.0.1:8000``). The server is auxiliary: a bind failure is logged but
    must NOT stop the poll loop, which is the worker's primary function. The
    thread is a daemon so process shutdown (the global ``running`` flag path) is
    unaffected.
    """
    addr = os.environ.get("EMBED_HTTP_ADDR", DEFAULT_HTTP_ADDR)
    try:
        host, _, port_str = addr.rpartition(":")
        if not host or not port_str:
            raise ValueError(f"expected host:port, got {addr!r}")
        port = int(port_str)
    except ValueError as e:
        logger.error("invalid EMBED_HTTP_ADDR (%s); embed http server disabled", e)
        return

    handler_cls = make_embed_handler(model, model_name)
    try:
        httpd = ThreadingHTTPServer((host, port), handler_cls)
    except OSError as e:
        logger.error("embed http server bind failed on %s (%s); continuing without it", addr, e)
        return

    thread = threading.Thread(
        target=httpd.serve_forever, name="embed-http", daemon=True
    )
    thread.start()
    logger.info("embed http server listening on %s", addr)


def run_loop(pg_dsn: str, model_name: str, batch_size: int, poll_interval: int) -> None:
    """Main embedding loop."""
    logger.info("loading model '%s'...", model_name)
    model = SentenceTransformer(model_name)
    dim = model.get_sentence_embedding_dimension()
    logger.info("model loaded (dim=%d)", dim)
    if dim != EMBEDDING_DIM:
        # The schema column is vector(384); a mismatched model would fail every
        # insert. Fail fast and loud rather than poll-and-error forever.
        logger.error(
            "model dim %d != expected %d; embeddings will be rejected", dim, EMBEDDING_DIM
        )

    # Auxiliary query-embed endpoint, reusing this warm model. Started here so the
    # bridge can embed search queries with the SAME model that produced the stored
    # vectors. Bind failure logs but never blocks the poll loop below.
    start_http_server(model, model_name)

    pg_conn = connect(pg_dsn)
    logger.info("starting embedding loop (batch=%d, poll=%ds)", batch_size, poll_interval)

    while running:
        try:
            skipped = skip_non_embeddable(pg_conn)
            if skipped:
                logger.info("marked %d empty messages as embedded (skipped)", skipped)

            messages = fetch_unembedded(pg_conn, batch_size)
            if messages:
                embed_batch(model, pg_conn, messages)
            else:
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
            logger.error("embedding error: %s", e, exc_info=True)
            time.sleep(poll_interval)

    try:
        pg_conn.close()
    except Exception:
        pass
    logger.info("embedding worker stopped")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    parser = argparse.ArgumentParser(description="WhatsApp message embedding worker")
    parser.add_argument("--pg", default="")
    parser.add_argument("--model", default=os.environ.get("EMBEDDING_MODEL", DEFAULT_MODEL))
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("EMBED_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=int(os.environ.get("EMBED_POLL_INTERVAL", DEFAULT_POLL_INTERVAL)),
    )
    args = parser.parse_args()

    dsn = resolve_dsn(args.pg)
    if not dsn:
        logger.error("no database DSN: set DATABASE_URL (or PG_DSN), or pass --pg")
        sys.exit(2)

    run_loop(dsn, args.model, args.batch_size, args.poll_interval)


if __name__ == "__main__":
    main()
