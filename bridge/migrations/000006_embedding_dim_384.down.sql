-- Revert embedding dimension 384 -> 1536.

DROP INDEX IF EXISTS idx_message_embeddings_hnsw;
DROP TABLE IF EXISTS message_embeddings;
CREATE TABLE message_embeddings (
    message_id TEXT         NOT NULL,
    chat_jid   TEXT         NOT NULL,
    embedding  vector(1536) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (message_id, chat_jid),
    FOREIGN KEY (message_id, chat_jid) REFERENCES messages(id, chat_jid) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_message_embeddings_hnsw
    ON message_embeddings USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

DROP INDEX IF EXISTS idx_chats_topic_embedding;
ALTER TABLE chats DROP COLUMN IF EXISTS topic_embedding;
ALTER TABLE chats ADD COLUMN topic_embedding vector(1536);
CREATE INDEX IF NOT EXISTS idx_chats_topic_embedding
    ON chats USING hnsw (topic_embedding vector_cosine_ops) WITH (m = 16, ef_construction = 100);

DROP INDEX IF EXISTS idx_contacts_name_embedding;
ALTER TABLE contacts DROP COLUMN IF EXISTS name_embedding;
ALTER TABLE contacts ADD COLUMN name_embedding vector(1536);
CREATE INDEX IF NOT EXISTS idx_contacts_name_embedding
    ON contacts USING hnsw (name_embedding vector_cosine_ops) WITH (m = 16, ef_construction = 100);
