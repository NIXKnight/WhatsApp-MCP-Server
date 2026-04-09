DROP INDEX IF EXISTS idx_contacts_name_embedding;
DROP INDEX IF EXISTS idx_chats_topic_embedding;
ALTER TABLE contacts DROP COLUMN IF EXISTS name_embedding;
ALTER TABLE chats DROP COLUMN IF EXISTS topic_embedding;
