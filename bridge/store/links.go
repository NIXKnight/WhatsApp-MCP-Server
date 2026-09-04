package store

import (
	"database/sql"
	"fmt"
	"time"
)

// LinkRow is the database representation of a links row: a URL extracted from
// message text at capture time, classified by platform.
type LinkRow struct {
	ID        int64     `json:"id"`
	URL       string    `json:"url"`
	Platform  string    `json:"platform"`
	Title     string    `json:"title"`
	SenderJID string    `json:"sender_jid"`
	ChatJID   string    `json:"chat_jid"`
	MessageID string    `json:"message_id"`
	Timestamp time.Time `json:"timestamp"`
	CreatedAt time.Time `json:"created_at"`
}

// InsertLink appends a captured link. Links are append-only; no de-dup is done
// at the store layer (the indexer already de-dups within a single message).
func (s *Store) InsertLink(l *LinkRow) error {
	if l.URL == "" || l.ChatJID == "" {
		return nil
	}
	platform := l.Platform
	if platform == "" {
		platform = "other"
	}
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO links (url, platform, title, sender_jid, chat_jid, message_id, timestamp)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			l.URL, platform, l.Title, l.SenderJID, l.ChatJID, l.MessageID, nullTime(l.Timestamp),
		)
		return err
	})
}

// LinkQuery holds filter parameters for QueryLinks.
type LinkQuery struct {
	ChatJID  string
	Platform string
	After    time.Time
	Before   time.Time
	Limit    int
	Offset   int
}

// QueryLinks returns links matching the filter, newest-first, plus the total
// matching count (ignoring limit/offset) for pagination.
func (s *Store) QueryLinks(q LinkQuery) ([]LinkRow, int, error) {
	if q.Limit <= 0 {
		q.Limit = 50
	}

	where := []string{"1=1"}
	al := &argList{}
	if q.ChatJID != "" {
		where = append(where, "chat_jid = "+al.add(q.ChatJID))
	}
	if q.Platform != "" {
		where = append(where, "platform = "+al.add(q.Platform))
	}
	if !q.After.IsZero() {
		where = append(where, "timestamp > "+al.add(q.After))
	}
	if !q.Before.IsZero() {
		where = append(where, "timestamp < "+al.add(q.Before))
	}

	whereSQL := ""
	for i, w := range where {
		if i > 0 {
			whereSQL += " AND "
		}
		whereSQL += w
	}

	var total int
	if err := s.db.QueryRow(
		fmt.Sprintf("SELECT COUNT(*) FROM links WHERE %s", whereSQL), al.args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count links: %w", err)
	}

	q2 := fmt.Sprintf(`
		SELECT id, url, platform, title, sender_jid, chat_jid, message_id, timestamp, created_at
		FROM links WHERE %s ORDER BY timestamp DESC NULLS LAST LIMIT %s OFFSET %s`,
		whereSQL, al.add(q.Limit), al.add(q.Offset))

	rows, err := s.db.Query(q2, al.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query links: %w", err)
	}
	defer rows.Close()

	var links []LinkRow
	for rows.Next() {
		var l LinkRow
		var ts, created sql.NullTime
		if err := rows.Scan(&l.ID, &l.URL, &l.Platform, &l.Title, &l.SenderJID,
			&l.ChatJID, &l.MessageID, &ts, &created); err != nil {
			return nil, 0, fmt.Errorf("scan link: %w", err)
		}
		if ts.Valid {
			l.Timestamp = ts.Time
		}
		if created.Valid {
			l.CreatedAt = created.Time
		}
		links = append(links, l)
	}
	return links, total, rows.Err()
}

func nullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}
