// Package bridge implements the WhatsApp protocol layer including the PostgreSQL
// message store, client lifecycle management, and event handling.
package bridge

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	pgvector "github.com/pgvector/pgvector-go"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/migrations"
)

// WriteOp is a single write operation submitted to the store's single-writer goroutine.
type WriteOp struct {
	fn   func(*sql.Tx) error
	done chan error
}

// Store manages the PostgreSQL message database with a single writer goroutine.
type Store struct {
	db           *sql.DB
	writeCh      chan WriteOp
	log          *slog.Logger
	done         chan struct{}
	doneOnce     sync.Once
	writerWG     sync.WaitGroup // tracks the writer goroutine
	refreshMu    sync.Mutex
	refreshTimer *time.Timer
}

// argList builds a numbered placeholder list ($1, $2, …) for dynamic queries.
type argList struct {
	args []interface{}
}

func (a *argList) add(v interface{}) string {
	a.args = append(a.args, v)
	return fmt.Sprintf("$%d", len(a.args))
}

// NewStore opens a PostgreSQL connection using databaseURL and starts the
// background writer goroutine.
func NewStore(databaseURL string, log *slog.Logger) (*Store, error) {
	connConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	db := stdlib.OpenDB(*connConfig)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := runMigrations(databaseURL); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	s := &Store{
		db:      db,
		writeCh: make(chan WriteOp, 1000),
		log:     log,
		done:    make(chan struct{}),
	}

	s.writerWG.Add(1)
	go s.writerLoop()
	return s, nil
}

// Close signals the writer goroutine to drain and exit, waits for it to
// finish, then closes the database handle.
func (s *Store) Close() error {
	// Signal shutdown exactly once.
	s.doneOnce.Do(func() {
		close(s.done)
	})
	// Cancel any pending debounced refresh.
	s.refreshMu.Lock()
	if s.refreshTimer != nil {
		s.refreshTimer.Stop()
	}
	s.refreshMu.Unlock()
	// Wait for the writer goroutine to drain remaining ops and exit.
	s.writerWG.Wait()
	// Now it is safe to close the DB handle.
	return s.db.Close()
}

// writerLoop serialises all writes through a single goroutine.
func (s *Store) writerLoop() {
	defer s.writerWG.Done()
	for {
		select {
		case op, ok := <-s.writeCh:
			if !ok {
				return
			}
			op.done <- s.execWrite(op.fn)
		case <-s.done:
			// Drain remaining ops already queued before exiting.
			for {
				select {
				case op := <-s.writeCh:
					op.done <- s.execWrite(op.fn)
				default:
					return
				}
			}
		}
	}
}

func (s *Store) execWrite(fn func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// submit enqueues a write operation and waits for the result.
// Returns an error immediately if the store is already shut down.
func (s *Store) submit(fn func(*sql.Tx) error) error {
	op := WriteOp{fn: fn, done: make(chan error, 1)}
	select {
	case s.writeCh <- op:
	case <-s.done:
		return fmt.Errorf("store is closed")
	default:
		s.log.Warn("write channel saturated, dropping write operation")
		return fmt.Errorf("write channel saturated")
	}
	// Wait for result, but also handle the case where the store shuts down
	// while we are waiting (writer drains then exits).
	select {
	case err := <-op.done:
		return err
	case <-s.done:
		// The writer is draining; our op may still complete. Wait a bit.
		return <-op.done
	}
}

// runMigrations applies pending schema migrations using golang-migrate.
func runMigrations(databaseURL string) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("migration source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, databaseURL)
	if err != nil {
		return fmt.Errorf("migration init: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migration up: %w", err)
	}
	return nil
}

// ---- Chat CRUD ----------------------------------------------------------

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

// ChatRow is the database representation of a chat.
type ChatRow struct {
	JID         string
	Name        string
	IsGroup     bool
	UnreadCount int
	LastMsgTime time.Time
	LastPreview string
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

// ChatWithScore pairs a ChatRow with a relevance score from vector search.
type ChatWithScore struct {
	ChatRow
	Relevance float64
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

// ---- Contact CRUD -------------------------------------------------------

// UpsertContact inserts or replaces a contact row.
func (s *Store) UpsertContact(jid, name, notify, phone string) error {
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO contacts (jid, name, notify, phone)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT(jid) DO UPDATE SET
			     name   = excluded.name,
			     notify = excluded.notify,
			     phone  = excluded.phone`,
			jid, name, notify, phone,
		)
		return err
	})
}

// ContactRow is the database representation of a contact.
type ContactRow struct {
	JID    string
	Name   string
	Notify string
	Phone  string
}

// GetContact returns the contact for the given JID.
func (s *Store) GetContact(jid string) (*ContactRow, error) {
	c := &ContactRow{}
	err := s.db.QueryRow(
		`SELECT jid, name, notify, phone FROM contacts WHERE jid = $1`, jid,
	).Scan(&c.JID, &c.Name, &c.Notify, &c.Phone)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// ListContacts returns all contacts ordered by name.
func (s *Store) ListContacts() ([]ContactRow, error) {
	rows, err := s.db.Query(`SELECT jid, name, notify, phone FROM contacts ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var contacts []ContactRow
	for rows.Next() {
		var c ContactRow
		if err := rows.Scan(&c.JID, &c.Name, &c.Notify, &c.Phone); err != nil {
			return nil, err
		}
		contacts = append(contacts, c)
	}
	return contacts, rows.Err()
}

// SimilarContactPair holds a pair of contacts with similar names.
type SimilarContactPair struct {
	JIDA       string
	NameA      string
	JIDB       string
	NameB      string
	Similarity float64
}

// FindSimilarContacts uses LATERAL KNN to find contact pairs whose name
// embeddings are above the given cosine similarity threshold.
func (s *Store) FindSimilarContacts(threshold float64, limit int) ([]SimilarContactPair, error) {
	rows, err := s.db.Query(
		`SELECT a.jid, a.name, b.jid, b.name,
		        1 - (a.name_embedding <=> b.name_embedding) AS similarity
		 FROM contacts a
		 CROSS JOIN LATERAL (
		     SELECT jid, name, name_embedding
		     FROM contacts
		     WHERE jid > a.jid AND name_embedding IS NOT NULL
		     ORDER BY name_embedding <=> a.name_embedding
		     LIMIT 5
		 ) b
		 WHERE a.name_embedding IS NOT NULL
		   AND 1 - (a.name_embedding <=> b.name_embedding) > $1
		 ORDER BY similarity DESC
		 LIMIT $2`,
		threshold, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []SimilarContactPair
	for rows.Next() {
		var p SimilarContactPair
		if err := rows.Scan(&p.JIDA, &p.NameA, &p.JIDB, &p.NameB, &p.Similarity); err != nil {
			return nil, err
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

// UpdateContactEmbedding stores the name embedding for the given contact JID.
func (s *Store) UpdateContactEmbedding(jid string, embedding []float32) error {
	vec := pgvector.NewVector(embedding)
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE contacts SET name_embedding = $1 WHERE jid = $2`,
			vec, jid,
		)
		return err
	})
}

// ---- Message CRUD -------------------------------------------------------

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

// CompactMessage holds a reduced set of fields for token-efficient context windows.
type CompactMessage struct {
	ID          string    `json:"id"`
	SenderName  string    `json:"name"`
	Content     string    `json:"text"`
	Timestamp   time.Time `json:"ts"`
	QuotedMsgID string    `json:"quoted_id,omitempty"`
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

// SearchResult is a single hit from a hybrid FTS + semantic search.
type SearchResult struct {
	ID         string
	ChatJID    string
	Content    string
	Timestamp  time.Time
	SenderName string
	Score      float64
	Snippet    string
	MatchType  string // "fts", "semantic", or "both"
}

// SearchHybrid performs Reciprocal Rank Fusion over FTS and/or vector results.
// At least one of query or embedding must be non-empty. When only one signal is
// provided the corresponding single-path SQL is used; when both are provided
// full RRF is applied.
func (s *Store) SearchHybrid(query string, embedding []float32, chatJID string, limit int) ([]SearchResult, error) {
	hasQuery := query != ""
	hasEmbedding := len(embedding) > 0

	if !hasQuery && !hasEmbedding {
		return nil, fmt.Errorf("at least one of query or embedding must be provided")
	}

	if limit <= 0 {
		limit = 20
	}

	switch {
	case hasQuery && hasEmbedding:
		return s.searchHybridRRF(query, embedding, chatJID, limit)
	case hasQuery:
		return s.searchFTSOnly(query, chatJID, limit)
	default:
		return s.searchVectorOnly(embedding, chatJID, limit)
	}
}

func (s *Store) searchHybridRRF(query string, embedding []float32, chatJID string, limit int) ([]SearchResult, error) {
	al := &argList{}
	queryParam := al.add(query)
	vecParam := al.add(pgvector.NewVector(embedding))

	chatFilter := ""
	chatFilterVec := ""
	if chatJID != "" {
		cp := al.add(chatJID)
		chatFilter = "AND chat_jid = " + cp
		chatFilterVec = "AND me.chat_jid = " + cp
	}

	limitParam := al.add(limit)

	q := `WITH fts_results AS (
    SELECT id, chat_jid,
           ROW_NUMBER() OVER (ORDER BY ts_rank(content_fts, plainto_tsquery('simple', ` + queryParam + `)) DESC) AS rn,
           ts_headline('simple', content, plainto_tsquery('simple', ` + queryParam + `), 'MaxWords=35,MinWords=15,MaxFragments=2') AS snippet
    FROM messages
    WHERE content_fts @@ plainto_tsquery('simple', ` + queryParam + `)
    ` + chatFilter + `
    LIMIT 100
),
vec_results AS (
    SELECT me.message_id AS id, me.chat_jid,
           ROW_NUMBER() OVER (ORDER BY me.embedding <=> ` + vecParam + ` ASC) AS rn
    FROM message_embeddings me
    ` + chatFilterVec + `
    ORDER BY me.embedding <=> ` + vecParam + `
    LIMIT 100
),
combined AS (
    SELECT COALESCE(f.id, v.id) AS id, COALESCE(f.chat_jid, v.chat_jid) AS chat_jid,
           COALESCE(1.0/(60.0+f.rn), 0) + COALESCE(1.0/(60.0+v.rn), 0) AS score,
           CASE WHEN f.id IS NOT NULL AND v.id IS NOT NULL THEN 'both'
                WHEN f.id IS NOT NULL THEN 'fts' ELSE 'semantic' END AS match_type,
           COALESCE(f.snippet, '') AS snippet
    FROM fts_results f FULL OUTER JOIN vec_results v ON f.id = v.id AND f.chat_jid = v.chat_jid
)
SELECT c.id, c.chat_jid, m.content, m.timestamp, m.sender_name, c.score, c.snippet, c.match_type
FROM combined c JOIN messages m ON m.id = c.id AND m.chat_jid = c.chat_jid
ORDER BY c.score DESC LIMIT ` + limitParam

	rows, err := s.db.Query(q, al.args...)
	if err != nil {
		return nil, fmt.Errorf("hybrid search: %w", err)
	}
	defer rows.Close()
	return scanSearchResults(rows)
}

func (s *Store) searchFTSOnly(query, chatJID string, limit int) ([]SearchResult, error) {
	al := &argList{}
	queryParam := al.add(query)

	chatFilter := ""
	if chatJID != "" {
		chatFilter = "AND chat_jid = " + al.add(chatJID)
	}
	limitParam := al.add(limit)

	q := `SELECT id, chat_jid, content, timestamp, sender_name,
	      ts_rank(content_fts, plainto_tsquery('simple', ` + queryParam + `)) AS score,
	      ts_headline('simple', content, plainto_tsquery('simple', ` + queryParam + `), 'MaxWords=35,MinWords=15,MaxFragments=2') AS snippet,
	      'fts' AS match_type
	      FROM messages
	      WHERE content_fts @@ plainto_tsquery('simple', ` + queryParam + `)
	      ` + chatFilter + `
	      ORDER BY score DESC LIMIT ` + limitParam

	rows, err := s.db.Query(q, al.args...)
	if err != nil {
		return nil, fmt.Errorf("fts search: %w", err)
	}
	defer rows.Close()
	return scanSearchResults(rows)
}

func (s *Store) searchVectorOnly(embedding []float32, chatJID string, limit int) ([]SearchResult, error) {
	al := &argList{}
	vecParam := al.add(pgvector.NewVector(embedding))

	chatFilter := ""
	if chatJID != "" {
		chatFilter = "AND me.chat_jid = " + al.add(chatJID)
	}
	limitParam := al.add(limit)

	q := `SELECT m.id, m.chat_jid, m.content, m.timestamp, m.sender_name,
	      1 - (me.embedding <=> ` + vecParam + `) AS score,
	      '' AS snippet, 'semantic' AS match_type
	      FROM message_embeddings me
	      JOIN messages m ON m.id = me.message_id AND m.chat_jid = me.chat_jid
	      WHERE TRUE ` + chatFilter + `
	      ORDER BY me.embedding <=> ` + vecParam + ` LIMIT ` + limitParam

	rows, err := s.db.Query(q, al.args...)
	if err != nil {
		return nil, fmt.Errorf("vector search: %w", err)
	}
	defer rows.Close()
	return scanSearchResults(rows)
}

func scanSearchResults(rows *sql.Rows) ([]SearchResult, error) {
	var results []SearchResult
	for rows.Next() {
		var r SearchResult
		var ts sql.NullTime
		if err := rows.Scan(
			&r.ID, &r.ChatJID, &r.Content, &ts,
			&r.SenderName, &r.Score, &r.Snippet, &r.MatchType,
		); err != nil {
			return nil, err
		}
		if ts.Valid {
			r.Timestamp = ts.Time
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// MessageWithScore is a message paired with a similarity distance score.
type MessageWithScore struct {
	ID         string
	ChatJID    string
	Content    string
	Timestamp  time.Time
	SenderName string
	Distance   float64
}

// FindSimilarMessages returns messages whose embeddings are closest to the
// embedding of the specified (messageID, chatJID) message, excluding itself.
func (s *Store) FindSimilarMessages(messageID, chatJID string, limit int) ([]MessageWithScore, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.Query(
		`SELECT m.id, m.chat_jid, m.content, m.timestamp, m.sender_name,
		        me.embedding <=> (SELECT embedding FROM message_embeddings WHERE message_id = $1 AND chat_jid = $2) AS distance
		 FROM message_embeddings me
		 JOIN messages m ON m.id = me.message_id AND m.chat_jid = me.chat_jid
		 WHERE NOT (me.message_id = $1 AND me.chat_jid = $2)
		 ORDER BY distance
		 LIMIT $3`,
		messageID, chatJID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("find similar messages: %w", err)
	}
	defer rows.Close()

	var results []MessageWithScore
	for rows.Next() {
		var ms MessageWithScore
		var ts sql.NullTime
		if err := rows.Scan(
			&ms.ID, &ms.ChatJID, &ms.Content, &ts, &ms.SenderName, &ms.Distance,
		); err != nil {
			return nil, err
		}
		if ts.Valid {
			ms.Timestamp = ts.Time
		}
		results = append(results, ms)
	}
	return results, rows.Err()
}

func scanMessages(rows *sql.Rows) ([]MessageRow, error) {
	var msgs []MessageRow
	for rows.Next() {
		var m MessageRow
		var ts sql.NullTime
		var fileLength sql.NullInt64
		err := rows.Scan(
			&m.ID, &m.ChatJID, &m.Sender, &m.SenderName, &m.Content,
			&ts, &m.IsFromMe, &m.MediaType, &m.Filename, &m.URL,
			&m.MediaKey, &m.FileSHA256, &m.FileEncSHA256, &fileLength,
			&m.PushName, &m.QuotedMessageID, &m.QuotedParticipant, &m.Snippet,
		)
		if err != nil {
			return nil, err
		}
		if ts.Valid {
			m.Timestamp = ts.Time
		}
		if fileLength.Valid {
			m.FileLength = fileLength.Int64
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}
