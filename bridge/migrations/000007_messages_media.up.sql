-- messages_media: normalized home for every media-related concern that the
-- enrichment workers (downloader, transcriber, OCR) operate on. Keyed by the
-- owning message; one row per media message. The legacy media columns on the
-- messages table are kept intact for the existing on-demand download path —
-- this table is additive and worker-owned.
--
-- NULL semantics: NULL = never attempted; an empty/marker value = attempted
-- with an empty result.
CREATE TABLE IF NOT EXISTS messages_media (
    message_id                  TEXT        NOT NULL,
    chat_jid                    TEXT        NOT NULL,

    -- identification
    media_type                  TEXT        NOT NULL CHECK (media_type IN ('image','video','audio','sticker','document')),
    mime_type                   TEXT,
    filename                    TEXT,
    file_length                 BIGINT,

    -- decryption / re-download material
    media_key                   BYTEA,
    file_sha256                 BYTEA,
    file_enc_sha256             BYTEA,
    url                         TEXT,
    direct_path                 TEXT,

    -- download lifecycle (worker-owned)
    local_path                  TEXT,
    downloaded_at               TIMESTAMPTZ,
    download_attempts           INTEGER     NOT NULL DEFAULT 0,
    download_last_error         TEXT,
    download_last_attempt_at    TIMESTAMPTZ,
    download_permanently_failed BOOLEAN     NOT NULL DEFAULT FALSE,

    -- transcription lifecycle (worker-owned)
    transcription               TEXT,
    transcription_lang          TEXT,
    transcribed_at              TIMESTAMPTZ,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ,

    PRIMARY KEY (message_id, chat_jid),
    FOREIGN KEY (message_id, chat_jid) REFERENCES messages(id, chat_jid) ON DELETE CASCADE
);

-- Downloader poll: media awaiting a local file that has not permanently failed.
CREATE INDEX IF NOT EXISTS idx_mm_pending_download ON messages_media (created_at)
    WHERE local_path IS NULL
      AND NOT download_permanently_failed
      AND media_type IN ('image','video','sticker','document');

-- Transcriber poll: audio rows with no transcription yet that have not failed.
CREATE INDEX IF NOT EXISTS idx_mm_pending_transcription ON messages_media (created_at)
    WHERE transcribed_at IS NULL
      AND NOT download_permanently_failed
      AND media_type = 'audio';

-- De-dup / content lookup by content hash.
CREATE INDEX IF NOT EXISTS idx_mm_file_sha256 ON messages_media (file_sha256)
    WHERE file_sha256 IS NOT NULL;

-- Keep updated_at fresh on every mutation.
CREATE OR REPLACE FUNCTION set_messages_media_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $fn$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $fn$;
DROP TRIGGER IF EXISTS trg_messages_media_updated_at ON messages_media;
CREATE TRIGGER trg_messages_media_updated_at BEFORE UPDATE ON messages_media
    FOR EACH ROW EXECUTE FUNCTION set_messages_media_updated_at();
