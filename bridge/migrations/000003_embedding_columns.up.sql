ALTER TABLE contacts ADD COLUMN IF NOT EXISTS name_embedding vector(1536);
CREATE INDEX IF NOT EXISTS idx_contacts_name_embedding
    ON contacts USING hnsw (name_embedding vector_cosine_ops) WITH (m = 16, ef_construction = 100);

ALTER TABLE chats ADD COLUMN IF NOT EXISTS topic_embedding vector(1536);
CREATE INDEX IF NOT EXISTS idx_chats_topic_embedding
    ON chats USING hnsw (topic_embedding vector_cosine_ops) WITH (m = 16, ef_construction = 100);
