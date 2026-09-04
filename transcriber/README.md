# whatsapp-transcriber

L3 enrichment worker. Polls the bridge-owned `messages_media` table for audio rows without a transcription, re-downloads missing media via the bridge API, posts the raw audio file to a Whisper endpoint (which decodes ogg/opus server-side via `--convert`), and writes the result back.

The database schema is **owned by the bridge migrations** (`bridge/migrations`). This worker issues no DDL — it only reads/writes the `messages_media` table.

## Behavior

1. Fetch `messages_media WHERE media_type = 'audio' AND transcribed_at IS NULL AND NOT download_permanently_failed` (newest first).
2. If `local_path` is missing on disk, re-download via the bridge `POST /api/download` (`{message_id, chat_jid, output_dir}`); persist the returned `file_path` to `local_path`.
3. POST the raw audio file as-is to the Whisper `/inference` endpoint; the whisper-server runs with `--convert` and decodes ogg/opus server-side (no client-side conversion).
4. `UPDATE messages_media SET transcription = ..., transcribed_at = NOW()`.

### Resilience

- **Whisper offline** → exponential backoff kicking in at 3 consecutive failures: `poll_interval * 2^(consecutive_failures - 2)`, capped at 300s.
- **Terminal HTTP codes** (403/404/410) or **>= 8 re-download attempts** (`MAX_DOWNLOAD_ATTEMPTS = 8`) → `download_permanently_failed = TRUE` so dead media (expired WhatsApp blobs) leaves the pool.
- **Undecodable file** → stamped `[unprocessable]` (won't retry forever).
- **No speech** → stamped `[no speech]`.

The connection runs in **autocommit** mode to avoid idle-in-transaction.

## On-demand video analysis

Alongside the poll loop the worker exposes a stdlib HTTP server for on-demand video extraction (the visual verdict is the caller's job — this only does the mechanical work): `POST /analyze {"chat_jid","message_id"}` downloads the video via the bridge, samples keyframes (1 fps, capped at 8) and demuxes the audio with ffmpeg, transcribes the WAV via Whisper, and returns `{"frame_paths":[...abs...],"transcription":"...","duration":<float>,"frame_count":<int>}`; `GET /health` returns `{"status":"ok"}`. Bind via `ANALYZER_HTTP_ADDR` (default `127.0.0.1:8500`); a bind failure is logged but never stops the poll loop. The endpoint is stateless and never touches the poll loop's DB connection. Videos longer than 600s are rejected (`422`).

## Config

| Env | Default | Meaning |
|-----|---------|---------|
| `DATABASE_URL` | — | PostgreSQL DSN (bridge convention; required) |
| `PG_DSN` | — | Fallback DSN if `DATABASE_URL` is unset |
| `BRIDGE_URL` | `http://bridge:8080` | Bridge API base for re-download |
| `WHISPER_URL` | `http://127.0.0.1:8443` | Whisper API endpoint (external host process) |
| `WHISPER_MODEL` | `large-v3` | Informational only — consumed by the whisper-server; this worker never reads it. |
| `WHISPER_LANGUAGE` | `ur` | Informational only (set in compose) — the worker sends no language param; the whisper-server auto-detects. |
| `TRANSCRIBE_BATCH_SIZE` | `20` | Rows per cycle |
| `TRANSCRIBE_POLL_INTERVAL` | `30` | Seconds between polls when idle |
| `ANALYZER_HTTP_ADDR` | `127.0.0.1:8500` | `/analyze` + `/health` HTTP bind address |

Whisper is an **external host process** — point `WHISPER_URL` at it; no Whisper container is bundled.

## Run

```bash
uv sync
DATABASE_URL="postgres://whatsapp:whatsapp@localhost:5432/whatsapp?sslmode=disable" \
WHISPER_URL="http://127.0.0.1:8443" \
  uv run python -m whatsapp_transcriber
```

Or via Docker Compose (`transcriber` service at repo root).
