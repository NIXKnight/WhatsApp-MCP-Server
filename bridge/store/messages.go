package store

import (
	"database/sql"
	"fmt"
	"time"
)

// MessageRow is the database representation of a message.
type MessageRow struct {
	ID                string
	ChatJID           string
	Sender            string
	SenderName        string
	Content           string
	Timestamp         time.Time
	IsFromMe          bool
	MediaType         string
	Filename          string
	URL               string
	MediaKey          []byte
	FileSHA256        []byte
	FileEncSHA256     []byte
	FileLength        int64
	PushName          string
	QuotedMessageID   string
	QuotedParticipant string
	Snippet           string
}

// QueryMessagesParams holds filter parameters for ListMessages.
type QueryMessagesParams struct {
	ChatJID    string
	Sender     string
	After      time.Time
	Before     time.Time
	Query      string
	SearchMode string // "fts" for full-text search; default is ILIKE
	Limit      int
	Offset     int
}

// CompactMessage holds a reduced set of fields for token-efficient context windows.
type CompactMessage struct {
	ID          string    `json:"id"`
	SenderName  string    `json:"name"`
	Content     string    `json:"text"`
	Timestamp   time.Time `json:"ts"`
	QuotedMsgID string    `json:"quoted_id,omitempty"`
}

// UpsertMessage inserts or replaces a message row. Messages with no content
// and no media type are silently ignored.
func (s *Store) UpsertMessage(m *MessageRow) error {
	if m.Content == "" && m.MediaType == "" {
		return nil
	}
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO messages
			 (id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
			  media_type, filename, url, media_key, file_sha256, file_enc_sha256,
			  file_length, push_name, quoted_message_id, quoted_participant)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
			 ON CONFLICT(id, chat_jid) DO UPDATE SET
			     sender             = excluded.sender,
			     sender_name        = excluded.sender_name,
			     content            = excluded.content,
			     timestamp          = excluded.timestamp,
			     is_from_me         = excluded.is_from_me,
			     media_type         = excluded.media_type,
			     filename           = excluded.filename,
			     url                = excluded.url,
			     media_key          = excluded.media_key,
			     file_sha256        = excluded.file_sha256,
			     file_enc_sha256    = excluded.file_enc_sha256,
			     file_length        = excluded.file_length,
			     push_name          = excluded.push_name,
			     quoted_message_id  = excluded.quoted_message_id,
			     quoted_participant = excluded.quoted_participant`,
			m.ID, m.ChatJID, m.Sender, m.SenderName, m.Content,
			m.Timestamp, m.IsFromMe, m.MediaType, m.Filename, m.URL,
			m.MediaKey, m.FileSHA256, m.FileEncSHA256, m.FileLength,
			m.PushName, m.QuotedMessageID, m.QuotedParticipant,
		)
		return err
	})
}

// ListMessages returns messages matching the given filter.
// When SearchMode == "fts" and Query != "", full-text search is used with
// ts_headline snippets and rank ordering. Otherwise ILIKE is used.
func (s *Store) ListMessages(p QueryMessagesParams) ([]MessageRow, error) {
	where := []string{}
	al := &argList{}

	if p.ChatJID != "" {
		where = append(where, "chat_jid = "+al.add(p.ChatJID))
	}
	if p.Sender != "" {
		where = append(where, "sender = "+al.add(p.Sender))
	}
	if !p.After.IsZero() {
		where = append(where, "timestamp > "+al.add(p.After))
	}
	if !p.Before.IsZero() {
		where = append(where, "timestamp < "+al.add(p.Before))
	}

	useFTS := p.SearchMode == "fts" && p.Query != ""
	var snippetExpr string
	var orderBy string

	if useFTS {
		queryParam := al.add(p.Query)
		where = append(where, "content_fts @@ plainto_tsquery('simple', "+queryParam+")")
		snippetExpr = "ts_headline('simple', content, plainto_tsquery('simple', " + queryParam + "), 'MaxWords=35,MinWords=15,MaxFragments=2') AS snippet"
		orderBy = "ORDER BY ts_rank(content_fts, plainto_tsquery('simple', " + queryParam + ")) DESC"
	} else {
		if p.Query != "" {
			where = append(where, "content ILIKE "+al.add("%"+p.Query+"%"))
		}
		snippetExpr = "'' AS snippet"
		orderBy = "ORDER BY timestamp DESC"
	}

	q := `SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
	      media_type, filename, url, media_key, file_sha256, file_enc_sha256,
	      file_length, push_name, quoted_message_id, quoted_participant, ` + snippetExpr + `
	      FROM messages`
	if len(where) > 0 {
		q += " WHERE "
		for i, w := range where {
			if i > 0 {
				q += " AND "
			}
			q += w
		}
	}
	q += " " + orderBy + " LIMIT " + al.add(p.Limit) + " OFFSET " + al.add(p.Offset)

	rows, err := s.db.Query(q, al.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanMessages(rows)
}

// GetMessageContext returns the N messages before and after the given message,
// ordered chronologically. contextSize controls how many messages to fetch on
// each side (not a time window).
func (s *Store) GetMessageContext(messageID, chatJID string, contextSize int) ([]MessageRow, error) {
	// Resolve the target message and its timestamp in one query.
	targetRows, err := s.db.Query(
		`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
		 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
		 file_length, push_name, quoted_message_id, quoted_participant, '' AS snippet
		 FROM messages WHERE id = $1 AND chat_jid = $2`,
		messageID, chatJID,
	)
	if err != nil {
		return nil, fmt.Errorf("get message context target: %w", err)
	}
	targets, err := scanMessages(targetRows)
	targetRows.Close()
	if err != nil {
		return nil, fmt.Errorf("scan context target: %w", err)
	}
	if len(targets) == 0 {
		return nil, sql.ErrNoRows
	}
	target := targets[0]
	targetTS := target.Timestamp

	// Fetch N messages strictly before the target, newest-first, then reverse.
	beforeRows, err := s.db.Query(
		`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
		 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
		 file_length, push_name, quoted_message_id, quoted_participant, '' AS snippet
		 FROM messages
		 WHERE chat_jid = $1 AND timestamp < $2
		 ORDER BY timestamp DESC LIMIT $3`,
		chatJID, targetTS, contextSize,
	)
	if err != nil {
		return nil, fmt.Errorf("get message context before: %w", err)
	}
	before, err := scanMessages(beforeRows)
	beforeRows.Close()
	if err != nil {
		return nil, fmt.Errorf("scan context before: %w", err)
	}

	// Fetch N messages strictly after the target, oldest-first.
	afterRows, err := s.db.Query(
		`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
		 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
		 file_length, push_name, quoted_message_id, quoted_participant, '' AS snippet
		 FROM messages
		 WHERE chat_jid = $1 AND timestamp > $2
		 ORDER BY timestamp ASC LIMIT $3`,
		chatJID, targetTS, contextSize,
	)
	if err != nil {
		return nil, fmt.Errorf("get message context after: %w", err)
	}
	after, err := scanMessages(afterRows)
	afterRows.Close()
	if err != nil {
		return nil, fmt.Errorf("scan context after: %w", err)
	}

	// Reverse `before` so it reads oldest-first, then assemble the window.
	for i, j := 0, len(before)-1; i < j; i, j = i+1, j-1 {
		before[i], before[j] = before[j], before[i]
	}

	result := make([]MessageRow, 0, len(before)+1+len(after))
	result = append(result, before...)
	result = append(result, target)
	result = append(result, after...)
	return result, nil
}

// ListMessagesCompact returns the most recent `limit` messages for `chatJID`
// that have non-empty content, projected to the minimal CompactMessage shape.
func (s *Store) ListMessagesCompact(chatJID string, limit int) ([]CompactMessage, error) {
	rows, err := s.db.Query(
		`SELECT id,
		        COALESCE(sender_name, push_name, sender, '') AS name,
		        COALESCE(content, '') AS text,
		        timestamp,
		        COALESCE(quoted_message_id, '') AS quoted_id
		 FROM messages
		 WHERE chat_jid = $1 AND content != ''
		 ORDER BY timestamp DESC LIMIT $2`,
		chatJID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list messages compact: %w", err)
	}
	defer rows.Close()

	var msgs []CompactMessage
	for rows.Next() {
		var m CompactMessage
		var ts sql.NullTime
		if err := rows.Scan(&m.ID, &m.SenderName, &m.Content, &ts, &m.QuotedMsgID); err != nil {
			return nil, fmt.Errorf("scan compact message: %w", err)
		}
		if ts.Valid {
			m.Timestamp = ts.Time
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// GetMessageByID fetches a single message by ID and chatJID.
func (s *Store) GetMessageByID(messageID, chatJID string) (*MessageRow, error) {
	rows, err := s.db.Query(
		`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
		 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
		 file_length, push_name, quoted_message_id, quoted_participant, '' AS snippet
		 FROM messages WHERE id = $1 AND chat_jid = $2`,
		messageID, chatJID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	msgs, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, sql.ErrNoRows
	}
	return &msgs[0], nil
}

// ListMessagesSince returns all messages with timestamp > since, optionally
// filtered to a single chat when chatJID is non-empty.
func (s *Store) ListMessagesSince(since time.Time, chatJID string, limit int) ([]MessageRow, error) {
	var (
		rows *sql.Rows
		err  error
	)

	if chatJID != "" {
		rows, err = s.db.Query(
			`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
			 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
			 file_length, push_name, quoted_message_id, quoted_participant, '' AS snippet
			 FROM messages WHERE timestamp > $1 AND chat_jid = $2
			 ORDER BY timestamp ASC LIMIT $3`,
			since, chatJID, limit,
		)
	} else {
		rows, err = s.db.Query(
			`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
			 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
			 file_length, push_name, quoted_message_id, quoted_participant, '' AS snippet
			 FROM messages WHERE timestamp > $1
			 ORDER BY timestamp ASC LIMIT $2`,
			since, limit,
		)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanMessages(rows)
}

// ListMessagesSinceMultiChat returns messages with timestamp > since whose
// chat_jid is in the provided jids slice, ordered oldest-first. When jids is
// empty the function returns an empty slice without querying the database.
func (s *Store) ListMessagesSinceMultiChat(since time.Time, jids []string, limit int) ([]MessageRow, error) {
	if len(jids) == 0 {
		return nil, nil
	}
	rows, err := s.db.Query(
		`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
		 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
		 file_length, push_name, quoted_message_id, quoted_participant, '' AS snippet
		 FROM messages
		 WHERE chat_jid = ANY($1::text[]) AND timestamp > $2
		 ORDER BY timestamp ASC LIMIT $3`,
		jids, since, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list messages since multi-chat: %w", err)
	}
	defer rows.Close()

	return scanMessages(rows)
}

// CountMessages returns the total number of messages stored.
func (s *Store) CountMessages() (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&n)
	return n, err
}
