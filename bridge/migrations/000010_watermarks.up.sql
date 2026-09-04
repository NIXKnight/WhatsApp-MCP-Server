-- watermarks: per-chat server-side "last seen" marker used by the batch
-- trigger check (POST /api/check/triggers). Each row records the timestamp
-- through which the chat has already been reported, so subsequent checks only
-- return messages that arrived since. The marker is advanced by the trigger
-- check unless it is run in dry_run mode.
-- jid is intentionally not a foreign key to chats(jid): the trigger check may
-- initialize a watermark for a JID before any message (and thus any chats row)
-- has been observed for it.
CREATE TABLE IF NOT EXISTS watermarks (
    jid       TEXT        PRIMARY KEY,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
