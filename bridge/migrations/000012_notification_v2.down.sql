-- DESTRUCTIVE after the approved v2 retirement cutover only. Refuse to erase
-- v2 delivery metadata while any v2 outbox row remains.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM notification_outbox WHERE schema_version = 2) THEN
        RAISE EXCEPTION 'notification_v2_rows_remain'
            USING ERRCODE = '55000';
    END IF;
END
$$;

ALTER TABLE notification_outbox
    DROP CONSTRAINT notification_v2_chat_sequence_unique,
    DROP CONSTRAINT notification_v2_metadata_coherent,
    DROP CONSTRAINT notification_schema_version_supported,
    DROP COLUMN is_from_me,
    DROP COLUMN chat_kind,
    DROP COLUMN sender_alt_jid,
    DROP COLUMN sender_jid,
    DROP COLUMN chat_sequence,
    DROP COLUMN canonical_chat_jid,
    ADD CONSTRAINT notification_outbox_schema_version_check
        CHECK (schema_version = 1);

DROP TABLE notification_chat_sequences;
