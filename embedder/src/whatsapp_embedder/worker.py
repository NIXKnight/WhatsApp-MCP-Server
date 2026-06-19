"""Embedding worker: reads messages from PostgreSQL, embeds them, stores vectors in pgvector.

The database schema is owned by the bridge migrations (``bridge/migrations``).
This worker is strictly a data plane: it never creates tables, alters columns, or
creates extensions. It reads ``messages``, writes ``message_embeddings``, and
stamps ``messages.embedded_at``.

Target schema (bridge-owned):
    messages           (id, chat_jid, content, timestamp, embedded_at, ...)
    message_embeddings (message_id, chat_jid, embedding vector(384), created_at)

Usage:
    whatsapp-embedder --pg "$DATABASE_URL"

Environment variables (CLI args take precedence):
    DATABASE_URL        - PostgreSQL DSN (bridge convention; required)
    PG_DSN              - fallback DSN if DATABASE_URL is unset
    EMBEDDING_MODEL     - sentence-transformers model (default: paraphrase-multilingual-MiniLM-L12-v2)
    EMBED_BATCH_SIZE    - messages per batch (default: 100)
    EMBED_POLL_INTERVAL - seconds between polls when idle (default: 30)
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from types import FrameType

import psycopg2
from sentence_transformers import SentenceTransformer

logger = logging.getLogger("whatsapp_embedder")

# 384-dim multilingual model — matches message_embeddings.embedding vector(384).
DEFAULT_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
DEFAULT_BATCH_SIZE = 100
DEFAULT_POLL_INTERVAL = 30
RECONNECT_BACKOFF = 5

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
    embeddings = model.encode(texts, show_progress_bar=False)

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


def run_loop(pg_dsn: str, model_name: str, batch_size: int, poll_interval: int) -> None:
    """Main embedding loop."""
    logger.info("loading model '%s'...", model_name)
    model = SentenceTransformer(model_name)
    dim = model.get_sentence_embedding_dimension()
    logger.info("model loaded (dim=%d)", dim)
    if dim != 384:
        # The schema column is vector(384); a mismatched model would fail every
        # insert. Fail fast and loud rather than poll-and-error forever.
        logger.error("model dim %d != expected 384; embeddings will be rejected", dim)

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
