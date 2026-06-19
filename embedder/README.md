# whatsapp-embedder

L3 enrichment worker. Polls the bridge-owned `messages` table for un-embedded
text, encodes it with a multilingual sentence-transformers model, and writes
384-dim vectors into `message_embeddings`.

The database schema is **owned by the bridge migrations** (`bridge/migrations`).
This worker issues no DDL — it only reads `messages` and writes
`message_embeddings` / stamps `messages.embedded_at`.

## Behavior

1. Stamp `embedded_at` on empty-content rows so they leave the pending pool
   (media-only / system messages have nothing to embed).
2. Fetch a batch of `messages WHERE embedded_at IS NULL AND content <> ''`
   (oldest first).
3. Encode with `paraphrase-multilingual-MiniLM-L12-v2` (384-dim, matches the
   `vector(384)` column).
4. `INSERT INTO message_embeddings (message_id, chat_jid, embedding)`
   (`ON CONFLICT DO NOTHING`), then `UPDATE messages SET embedded_at = NOW()`.
5. Sleep `EMBED_POLL_INTERVAL` when the pool is empty.

The connection runs in **autocommit** mode to avoid idle-in-transaction.

## Query-embed HTTP endpoint

A standard-library HTTP server (no extra deps) runs in a daemon thread alongside
the poll loop, serving query embeddings from the **same warm model** that
produced the stored vectors (parity is the whole point):

- `POST /embed` — body `{"text": "..."}` → `{"embedding": [..384 floats..]}`
  (`400` on missing/empty `text`, `500` on encode failure).
- `GET /health` — `{"status": "ok", "model": "<model>", "dim": 384}`.

Bind address is `EMBED_HTTP_ADDR` (default `127.0.0.1:8000`). The endpoint is
auxiliary: a bind failure is logged but never stops message embedding.

## Config

| Env | Default | Meaning |
|-----|---------|---------|
| `DATABASE_URL` | — | PostgreSQL DSN (bridge convention; required) |
| `PG_DSN` | — | Fallback DSN if `DATABASE_URL` is unset |
| `EMBEDDING_MODEL` | `paraphrase-multilingual-MiniLM-L12-v2` | sentence-transformers model (must be 384-dim) |
| `EMBED_BATCH_SIZE` | `100` | Messages per batch |
| `EMBED_POLL_INTERVAL` | `30` | Seconds between polls when idle |
| `EMBED_HTTP_ADDR` | `127.0.0.1:8000` | Bind address for the `/embed` + `/health` server |

## Run

```bash
uv sync
DATABASE_URL="postgres://whatsapp:whatsapp@localhost:5432/whatsapp?sslmode=disable" \
  uv run python -m whatsapp_embedder
```

Or via Docker Compose (`embedder` service at repo root).
