CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS chats (
    jid                  TEXT PRIMARY KEY,
    name                 TEXT        NOT NULL DEFAULT '',
    is_group             BOOLEAN     NOT NULL DEFAULT FALSE,
    unread_count         INTEGER     NOT NULL DEFAULT 0,
    last_message_time    TIMESTAMPTZ,
    last_message_preview TEXT        NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS contacts (
    jid    TEXT PRIMARY KEY,
    name   TEXT NOT NULL DEFAULT '',
    notify TEXT NOT NULL DEFAULT '',
    phone  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS messages (
    id                  TEXT        NOT NULL,
    chat_jid            TEXT        NOT NULL REFERENCES chats(jid),
    sender              TEXT        NOT NULL DEFAULT '',
    sender_name         TEXT        NOT NULL DEFAULT '',
    content             TEXT        NOT NULL DEFAULT '',
    timestamp           TIMESTAMPTZ,
    is_from_me          BOOLEAN     NOT NULL DEFAULT FALSE,
    media_type          TEXT        NOT NULL DEFAULT '',
    filename            TEXT        NOT NULL DEFAULT '',
    url                 TEXT        NOT NULL DEFAULT '',
    media_key           BYTEA,
    file_sha256         BYTEA,
    file_enc_sha256     BYTEA,
    file_length         BIGINT,
    push_name           TEXT        NOT NULL DEFAULT '',
    quoted_message_id   TEXT        NOT NULL DEFAULT '',
    quoted_participant  TEXT        NOT NULL DEFAULT '',
    embedded_at         TIMESTAMPTZ,
    content_fts         tsvector    GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED,
    PRIMARY KEY (id, chat_jid)
);

-- Primary query indexes
CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages (chat_jid, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_sender    ON messages (sender);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp DESC);

-- Trigram indexes for fast ILIKE substring search
CREATE INDEX IF NOT EXISTS idx_messages_content_trgm ON messages USING GIN (content gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_chats_name_trgm       ON chats    USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_contacts_name_trgm    ON contacts USING GIN (name gin_trgm_ops);

-- Full-text search index
CREATE INDEX IF NOT EXISTS idx_messages_content_fts ON messages USING GIN (content_fts);

-- BRIN index on timestamp (compact, effective for append-ordered data)
CREATE INDEX IF NOT EXISTS idx_messages_timestamp_brin ON messages USING BRIN (timestamp) WITH (pages_per_range = 128);

-- Partial indexes
CREATE INDEX IF NOT EXISTS idx_messages_media   ON messages (chat_jid, timestamp DESC) WHERE media_type != '';
CREATE INDEX IF NOT EXISTS idx_messages_from_me ON messages (chat_jid, timestamp DESC) WHERE is_from_me = TRUE;
CREATE INDEX IF NOT EXISTS idx_chats_unread     ON chats (last_message_time DESC)      WHERE unread_count > 0;
