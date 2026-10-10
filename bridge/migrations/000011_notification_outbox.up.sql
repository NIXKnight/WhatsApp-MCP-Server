-- Additive only: no history backfill and no changes to existing messages.
CREATE TABLE notification_outbox (
    event_id TEXT PRIMARY KEY CHECK (event_id ~ '^[0-9a-f]{32}$'),
    schema_version SMALLINT NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    event_type TEXT NOT NULL DEFAULT 'whatsapp.message.received'
        CHECK (event_type = 'whatsapp.message.received'),
    chat_jid TEXT NOT NULL,
    message_id TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'accepted', 'ignored', 'dead')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_token TEXT,
    lease_until TIMESTAMPTZ,
    terminal_at TIMESTAMPTZ,
    last_http_status INTEGER NOT NULL DEFAULT 0,
    last_error_class TEXT NOT NULL DEFAULT '',
    CONSTRAINT notification_lease_fields_coherent CHECK (
        (state = 'leased' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (state <> 'leased' AND lease_token IS NULL AND lease_until IS NULL)
    ),
    CONSTRAINT notification_terminal_timestamp_coherent CHECK (
        (state IN ('accepted', 'ignored', 'dead') AND terminal_at IS NOT NULL)
        OR (state NOT IN ('accepted', 'ignored', 'dead') AND terminal_at IS NULL)
    ),
    CONSTRAINT notification_http_status_valid CHECK (last_http_status BETWEEN 0 AND 599),
    CONSTRAINT notification_expiry_valid CHECK (expires_at > received_at),
    UNIQUE (event_type, chat_jid, message_id),
    FOREIGN KEY (message_id, chat_jid) REFERENCES messages(id, chat_jid) ON DELETE CASCADE
);
CREATE INDEX idx_notification_due ON notification_outbox (next_attempt_at, received_at)
    WHERE state = 'pending';
CREATE INDEX idx_notification_lease ON notification_outbox (lease_until)
    WHERE state = 'leased';
CREATE INDEX idx_notification_expiry ON notification_outbox (expires_at)
    WHERE state IN ('pending', 'leased');
CREATE INDEX idx_notification_terminal ON notification_outbox (terminal_at)
    WHERE state IN ('accepted', 'ignored', 'dead');
