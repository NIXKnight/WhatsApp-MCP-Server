DROP TRIGGER IF EXISTS trg_messages_media_updated_at ON messages_media;
DROP FUNCTION IF EXISTS set_messages_media_updated_at();
DROP INDEX IF EXISTS idx_mm_file_sha256;
DROP INDEX IF EXISTS idx_mm_pending_transcription;
DROP INDEX IF EXISTS idx_mm_pending_download;
DROP TABLE IF EXISTS messages_media;
