"""Read-only PostgreSQL access for the dashboard.

The target deployment is PostgreSQL-only. The DSN is taken from ``DATABASE_URL``
(the convention used by every other service in this repository) and falls back
to ``PG_DSN`` for parity with the reference dashboard.

A small ``psycopg2`` threaded connection pool is created lazily on first use.
Every query runs in a read-only transaction; this module never writes.
"""

import os
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras
import psycopg2.pool

# DATABASE_URL is the primary source (matches bridge/embedder/transcriber).
# PG_DSN is accepted as a fallback for compatibility with the reference.
PG_DSN = os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN") or ""

# Lazily-created threaded connection pool.
_pg_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Return the process-wide connection pool, creating it on first use."""
    global _pg_pool
    if _pg_pool is None:
        if not PG_DSN:
            raise RuntimeError(
                "No database DSN configured. Set DATABASE_URL (or PG_DSN)."
            )
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(1, 5, PG_DSN)
    return _pg_pool


@contextmanager
def get_db():
    """Yield a pooled connection forced into a read-only transaction."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        # Defensive: the dashboard must never write. A read-only session makes
        # any accidental INSERT/UPDATE/DELETE fail loudly at the database.
        conn.set_session(readonly=True, autocommit=True)
        yield conn
    finally:
        pool.putconn(conn)


def query(sql: str, params: tuple[Any, ...] = ()) -> list[dict]:
    """Execute a read-only query and return rows as dicts."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def query_one(sql: str, params: tuple[Any, ...] = ()) -> dict | None:
    """Execute a read-only query and return the first row, or ``None``."""
    results = query(sql, params)
    return results[0] if results else None
