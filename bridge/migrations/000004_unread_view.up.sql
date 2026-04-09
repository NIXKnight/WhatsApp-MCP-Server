CREATE MATERIALIZED VIEW IF NOT EXISTS unread_summary AS
SELECT
    c.jid, c.name, c.is_group, c.unread_count,
    c.last_message_time, c.last_message_preview,
    m.id AS last_message_id, m.sender AS last_message_sender,
    m.sender_name AS last_message_sender_name,
    m.content AS last_message_content, m.timestamp AS last_message_timestamp
FROM chats c
LEFT JOIN LATERAL (
    SELECT id, sender, sender_name, content, timestamp
    FROM messages WHERE chat_jid = c.jid ORDER BY timestamp DESC LIMIT 1
) m ON TRUE
WHERE c.unread_count > 0;

CREATE UNIQUE INDEX IF NOT EXISTS unread_summary_jid_idx ON unread_summary (jid);
