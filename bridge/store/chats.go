package store

import (
	"database/sql"
	"time"

	pgvector "github.com/pgvector/pgvector-go"
)

// ChatRow is the database representation of a chat.
type ChatRow struct {
	JID         string
	Name        string
	IsGroup     bool
	UnreadCount int
	LastMsgTime time.Time
	LastPreview string
}

// ChatWithScore pairs a ChatRow with a relevance score from vector search.
type ChatWithScore struct {
	ChatRow
	Relevance float64
}

// UpsertChat inserts or replaces a chat row.
func (s *Store) UpsertChat(jid, name string, isGroup bool, unread int, lastTime time.Time, preview string) error {
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO chats (jid, name, is_group, unread_count, last_message_time, last_message_preview)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT(jid) DO UPDATE SET
			     name                 = excluded.name,
			     is_group             = excluded.is_group,
			     unread_count         = excluded.unread_count,
			     last_message_time    = excluded.last_message_time,
			     last_message_preview = excluded.last_message_preview`,
			jid, name, isGroup, unread, lastTime, preview,
		)
		return err
	})
}

// GetChat returns the chat for the given JID, or sql.ErrNoRows if not found.
func (s *Store) GetChat(jid string) (*ChatRow, error) {
	row := s.db.QueryRow(
		`SELECT jid, name, is_group, unread_count, last_message_time, last_message_preview
		 FROM chats WHERE jid = $1`, jid,
	)
	return scanChat(row)
}

func scanChat(row *sql.Row) (*ChatRow, error) {
	c := &ChatRow{}
	var lastMsgTime sql.NullTime
	err := row.Scan(&c.JID, &c.Name, &c.IsGroup, &c.UnreadCount, &lastMsgTime, &c.LastPreview)
	if err != nil {
		return nil, err
	}
	if lastMsgTime.Valid {
		c.LastMsgTime = lastMsgTime.Time
	}
	return c, nil
}

// ListChats returns chats ordered by last_message_time DESC with optional
// full-text search on name.
func (s *Store) ListChats(search string, limit, offset int, groupsOnly bool) ([]ChatRow, error) {
	base := `SELECT jid, name, is_group, unread_count, last_message_time, last_message_preview FROM chats`
	where := []string{}
	al := &argList{}

	if search != "" {
		where = append(where, "name ILIKE "+al.add("%"+search+"%"))
	}
	if groupsOnly {
		where = append(where, "is_group = TRUE")
	}

	query := base
	if len(where) > 0 {
		query += " WHERE "
		for i, w := range where {
			if i > 0 {
				query += " AND "
			}
			query += w
		}
	}
	query += " ORDER BY last_message_time DESC NULLS LAST LIMIT " + al.add(limit) + " OFFSET " + al.add(offset)

	rows, err := s.db.Query(query, al.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chats []ChatRow
	for rows.Next() {
		var c ChatRow
		var lastMsgTime sql.NullTime
		if err := rows.Scan(&c.JID, &c.Name, &c.IsGroup, &c.UnreadCount, &lastMsgTime, &c.LastPreview); err != nil {
			return nil, err
		}
		if lastMsgTime.Valid {
			c.LastMsgTime = lastMsgTime.Time
		}
		chats = append(chats, c)
	}
	return chats, rows.Err()
}

// SearchChatsByTopic returns chats whose topic_embedding is closest to the
// provided embedding vector, ordered by cosine similarity descending.
func (s *Store) SearchChatsByTopic(embedding []float32, limit int) ([]ChatWithScore, error) {
	vec := pgvector.NewVector(embedding)
	rows, err := s.db.Query(
		`SELECT jid, name, is_group, unread_count, last_message_time, last_message_preview,
		        1 - (topic_embedding <=> $1) AS relevance
		 FROM chats WHERE topic_embedding IS NOT NULL
		 ORDER BY topic_embedding <=> $1 LIMIT $2`,
		vec, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ChatWithScore
	for rows.Next() {
		var cs ChatWithScore
		var lastMsgTime sql.NullTime
		if err := rows.Scan(
			&cs.JID, &cs.Name, &cs.IsGroup, &cs.UnreadCount,
			&lastMsgTime, &cs.LastPreview, &cs.Relevance,
		); err != nil {
			return nil, err
		}
		if lastMsgTime.Valid {
			cs.LastMsgTime = lastMsgTime.Time
		}
		result = append(result, cs)
	}
	return result, rows.Err()
}

// UpdateChatTopicEmbedding stores the topic embedding for the given chat JID.
func (s *Store) UpdateChatTopicEmbedding(jid string, embedding []float32) error {
	vec := pgvector.NewVector(embedding)
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE chats SET topic_embedding = $1 WHERE jid = $2`,
			vec, jid,
		)
		return err
	})
}

// ListUnreadChats returns all chats with unread_count > 0.
func (s *Store) ListUnreadChats() ([]ChatRow, error) {
	rows, err := s.db.Query(
		`SELECT jid, name, is_group, unread_count, last_message_time, last_message_preview
		 FROM chats WHERE unread_count > 0
		 ORDER BY last_message_time DESC NULLS LAST`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chats []ChatRow
	for rows.Next() {
		var c ChatRow
		var lastMsgTime sql.NullTime
		if err := rows.Scan(&c.JID, &c.Name, &c.IsGroup, &c.UnreadCount, &lastMsgTime, &c.LastPreview); err != nil {
			return nil, err
		}
		if lastMsgTime.Valid {
			c.LastMsgTime = lastMsgTime.Time
		}
		chats = append(chats, c)
	}
	return chats, rows.Err()
}

// CountChats returns the total number of chats stored.
func (s *Store) CountChats() (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM chats`).Scan(&n)
	return n, err
}

// MarkChatRead resets the unread_count for the given chat.
func (s *Store) MarkChatRead(jid string) error {
	err := s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE chats SET unread_count = 0 WHERE jid = $1`, jid)
		return err
	})
	if err == nil {
		s.scheduleUnreadRefresh()
	}
	return err
}

// IncrementUnread atomically increments unread_count by 1 for the given chat.
// The chat row must already exist (UpsertChat is called before this).
func (s *Store) IncrementUnread(chatJID string) error {
	err := s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE chats SET unread_count = unread_count + 1 WHERE jid = $1`,
			chatJID,
		)
		return err
	})
	if err == nil {
		s.scheduleUnreadRefresh()
	}
	return err
}

// ResetUnread sets unread_count to 0 for the given chat. Alias for MarkChatRead.
func (s *Store) ResetUnread(chatJID string) error {
	return s.MarkChatRead(chatJID)
}

// RefreshUnreadSummary refreshes the unread_summary materialized view concurrently.
func (s *Store) RefreshUnreadSummary() error {
	_, err := s.db.Exec(`REFRESH MATERIALIZED VIEW CONCURRENTLY unread_summary`)
	return err
}

// scheduleUnreadRefresh debounces materialized view refreshes: at most one
// refresh fires per 5-second window after a MarkChatRead or IncrementUnread call.
func (s *Store) scheduleUnreadRefresh() {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	if s.refreshTimer != nil {
		s.refreshTimer.Stop()
	}
	s.refreshTimer = time.AfterFunc(5*time.Second, func() {
		if err := s.RefreshUnreadSummary(); err != nil {
			s.log.Warn("unread_summary refresh failed", "err", err)
		}
	})
}
