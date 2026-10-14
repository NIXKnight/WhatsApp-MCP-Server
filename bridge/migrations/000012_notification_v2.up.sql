-- Additive v2 metadata. Existing and newly-created v1 rows remain coherent;
-- v2 writers explicitly set schema_version = 2.
CREATE TABLE notification_chat_sequences (
    canonical_chat_jid TEXT PRIMARY KEY,
    last_sequence BIGINT NOT NULL CHECK (last_sequence > 0)
);

ALTER TABLE notification_outbox
    DROP CONSTRAINT notification_outbox_schema_version_check,
    ADD COLUMN canonical_chat_jid TEXT,
    ADD COLUMN chat_sequence BIGINT,
    ADD COLUMN sender_jid TEXT,
    ADD COLUMN sender_alt_jid TEXT,
    ADD COLUMN chat_kind TEXT,
    ADD COLUMN is_from_me BOOLEAN,
    ADD CONSTRAINT notification_schema_version_supported
        CHECK (schema_version IN (1, 2)),
    ADD CONSTRAINT notification_v2_metadata_coherent CHECK (
        (
            schema_version = 1
            AND canonical_chat_jid IS NULL
            AND chat_sequence IS NULL
            AND sender_jid IS NULL
            AND sender_alt_jid IS NULL
            AND chat_kind IS NULL
            AND is_from_me IS NULL
        )
        OR
        (
            schema_version = 2
            AND canonical_chat_jid IS NOT NULL
            AND chat_sequence IS NOT NULL
            AND chat_sequence > 0
            AND sender_jid IS NOT NULL
            AND sender_jid ~ '^\d+@(s\.whatsapp\.net|lid|bot|hosted|hosted\.lid)$'
            AND (sender_alt_jid IS NULL OR sender_alt_jid ~ '^\d+@(s\.whatsapp\.net|lid|bot|hosted|hosted\.lid)$')
            AND chat_kind IS NOT NULL
            AND is_from_me IS NOT NULL
            AND (
                (
                    chat_kind = 'group'
                    AND chat_jid ~ '^\d+@g\.us$'
                    AND canonical_chat_jid = chat_jid
                )
                OR
                (
                    chat_kind = 'operator_private'
                    AND chat_jid ~ '^\d+@(s\.whatsapp\.net|lid)$'
                    AND canonical_chat_jid ~ '^\d+@(s\.whatsapp\.net|lid)$'
                )
            )
        )
    ),
    ADD CONSTRAINT notification_v2_chat_sequence_unique
        UNIQUE (canonical_chat_jid, chat_sequence);
