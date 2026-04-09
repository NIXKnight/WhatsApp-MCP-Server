CREATE TABLE IF NOT EXISTS message_embeddings (
    message_id TEXT        NOT NULL,
    chat_jid   TEXT        NOT NULL,
    embedding  vector(1536) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (message_id, chat_jid),
    FOREIGN KEY (message_id, chat_jid) REFERENCES messages(id, chat_jid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_message_embeddings_hnsw
    ON message_embeddings USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
