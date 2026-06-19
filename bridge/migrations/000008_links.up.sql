-- links: URLs extracted from message text at capture time, classified by
-- platform. Powers the dashboard link feed and per-platform queries.
CREATE TABLE IF NOT EXISTS links (
    id          BIGSERIAL   PRIMARY KEY,
    url         TEXT        NOT NULL,
    platform    TEXT        NOT NULL DEFAULT 'other',
    title       TEXT        NOT NULL DEFAULT '',
    sender_jid  TEXT        NOT NULL DEFAULT '',
    chat_jid    TEXT        NOT NULL,
    message_id  TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_links_platform  ON links (platform);
CREATE INDEX IF NOT EXISTS idx_links_chat_time ON links (chat_jid, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_links_url       ON links (url);
