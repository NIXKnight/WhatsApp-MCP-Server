-- DESTRUCTIVE: discards notification delivery/retry state only. Requires
-- separate explicit operator authorization; not routine operational rollback.
-- Existing chats, messages, WhatsApp sessions and keys are preserved.
DROP TABLE IF EXISTS notification_outbox;
