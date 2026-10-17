-- Durable correlation for messages created by bridge HTTP send endpoints.
-- There is intentionally no bridge_outbound_origins -> messages foreign key:
-- registration must commit before WhatsApp is called and therefore before the
-- echoed message exists. The additive messages column has a default so an older
-- binary's explicit INSERT/SELECT column lists remain compatible while it exists.
ALTER TABLE messages
    ADD COLUMN is_bridge_origin BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE bridge_outbound_origins (
    message_id TEXT NOT NULL CHECK (message_id <> ''),
    chat_jid TEXT NOT NULL
        CHECK (chat_jid ~ '^\d+@(s\.whatsapp\.net|g\.us|lid)$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    observed_at TIMESTAMPTZ,
    PRIMARY KEY (message_id, chat_jid),
    CONSTRAINT bridge_outbound_origin_observation_valid CHECK (
        observed_at IS NULL OR observed_at >= created_at
    )
);

-- Each retention class has an index matching its bounded prune predicate and
-- ordering. Unobserved uncertain sends use created_at; observed tombstones use
-- observed_at only after messages.is_bridge_origin is durable.
CREATE INDEX idx_bridge_outbound_origins_unobserved
    ON bridge_outbound_origins (created_at, message_id, chat_jid)
    WHERE observed_at IS NULL;

CREATE INDEX idx_bridge_outbound_origins_observed
    ON bridge_outbound_origins (observed_at, message_id, chat_jid)
    WHERE observed_at IS NOT NULL;
