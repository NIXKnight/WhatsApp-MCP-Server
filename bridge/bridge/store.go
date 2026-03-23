// Package bridge implements the WhatsApp protocol layer including the SQLite
// message store, client lifecycle management, and event handling.
package bridge

import (
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// WriteOp is a single write operation submitted to the store's single-writer goroutine.
type WriteOp struct {
	fn   func(*sql.Tx) error
	done chan error
}

// Store manages the SQLite message database with a single writer goroutine and
// a separate read-only connection pool to eliminate SQLITE_BUSY errors.
type Store struct {
	writeDB  *sql.DB
	readDB   *sql.DB
	writeCh  chan WriteOp
	log      *slog.Logger
	done     chan struct{}
	doneOnce sync.Once
	writerWG sync.WaitGroup // tracks the writer goroutine
}

// NewStore opens (or creates) the message database at dataDir/messages.db and
// starts the background writer goroutine.
func NewStore(dataDir string, log *slog.Logger) (*Store, error) {
	dsn := fmt.Sprintf(
		"file:%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on&cache=shared",
		filepath.Join(dataDir, "messages.db"),
	)

	writeDB, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open write db: %w", err)
	}
	writeDB.SetMaxOpenConns(1) // serialise all writes through a single connection

	readDSN := fmt.Sprintf(
		"file:%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on&mode=ro&cache=shared",
		filepath.Join(dataDir, "messages.db"),
	)
	readDB, err := sql.Open("sqlite3", readDSN)
	if err != nil {
		writeDB.Close()
		return nil, fmt.Errorf("open read db: %w", err)
	}
	readDB.SetMaxOpenConns(5)

	if err := migrate(writeDB); err != nil {
		writeDB.Close()
		readDB.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	s := &Store{
		writeDB: writeDB,
		readDB:  readDB,
		writeCh: make(chan WriteOp, 1000),
		log:     log,
		done:    make(chan struct{}),
	}

	s.writerWG.Add(1)
	go s.writerLoop()
	return s, nil
}

// Close signals the writer goroutine to drain and exit, waits for it to
// finish, then closes both database handles in order.
func (s *Store) Close() error {
	// Signal shutdown exactly once.
	s.doneOnce.Do(func() {
		close(s.done)
	})
	// Wait for the writer goroutine to drain remaining ops and exit.
	s.writerWG.Wait()
	// Now it is safe to close the DB handles.
	if err := s.writeDB.Close(); err != nil {
		s.readDB.Close() //nolint:errcheck
		return fmt.Errorf("close write db: %w", err)
	}
	return s.readDB.Close()
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
	tx, err := s.writeDB.Begin()
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

// migrate applies the schema DDL to the database.
func migrate(db *sql.DB) error {
	const schema = `
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS chats (
    jid                  TEXT PRIMARY KEY,
    name                 TEXT,
    is_group             BOOLEAN DEFAULT FALSE,
    unread_count         INTEGER DEFAULT 0,
    last_message_time    TIMESTAMP,
    last_message_preview TEXT
);

CREATE TABLE IF NOT EXISTS contacts (
    jid    TEXT PRIMARY KEY,
    name   TEXT,
    notify TEXT,
    phone  TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    id                  TEXT,
    chat_jid            TEXT,
    sender              TEXT,
    sender_name         TEXT,
    content             TEXT,
    timestamp           TIMESTAMP,
    is_from_me          BOOLEAN,
    media_type          TEXT,
    filename            TEXT,
    url                 TEXT,
    media_key           BLOB,
    file_sha256         BLOB,
    file_enc_sha256     BLOB,
    file_length         INTEGER,
    push_name           TEXT,
    quoted_message_id   TEXT,
    quoted_participant  TEXT,
    PRIMARY KEY (id, chat_jid),
    FOREIGN KEY (chat_jid) REFERENCES chats(jid)
);

CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_jid, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_sender    ON messages(sender);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp DESC);
`
	_, err := db.Exec(schema)
	return err
}

// ---- Chat CRUD ----------------------------------------------------------

// UpsertChat inserts or replaces a chat row.
func (s *Store) UpsertChat(jid, name string, isGroup bool, unread int, lastTime time.Time, preview string) error {
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO chats (jid, name, is_group, unread_count, last_message_time, last_message_preview)
			 VALUES (?, ?, ?, ?, ?, ?)
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
	row := s.readDB.QueryRow(
		`SELECT jid, name, is_group, unread_count, last_message_time, last_message_preview
		 FROM chats WHERE jid = ?`, jid,
	)
	return scanChat(row)
}

func scanChat(row *sql.Row) (*ChatRow, error) {
	c := &ChatRow{}
	err := row.Scan(&c.JID, &c.Name, &c.IsGroup, &c.UnreadCount, &c.LastMsgTime, &c.LastPreview)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// ListChats returns chats ordered by last_message_time DESC with optional
// full-text search on name.
func (s *Store) ListChats(query string, limit, offset int, groupsOnly bool) ([]ChatRow, error) {
	var (
		rows *sql.Rows
		err  error
	)
	base := `SELECT jid, name, is_group, unread_count, last_message_time, last_message_preview FROM chats`
	where := []string{}
	args := []interface{}{}

	if query != "" {
		where = append(where, "name LIKE ?")
		args = append(args, "%"+query+"%")
	}
	if groupsOnly {
		where = append(where, "is_group = 1")
	}

	sql := base
	if len(where) > 0 {
		sql += " WHERE "
		for i, w := range where {
			if i > 0 {
				sql += " AND "
			}
			sql += w
		}
	}
	sql += " ORDER BY last_message_time DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err = s.readDB.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chats []ChatRow
	for rows.Next() {
		var c ChatRow
		if err := rows.Scan(&c.JID, &c.Name, &c.IsGroup, &c.UnreadCount, &c.LastMsgTime, &c.LastPreview); err != nil {
			return nil, err
		}
		chats = append(chats, c)
	}
	return chats, rows.Err()
}

// ---- Contact CRUD -------------------------------------------------------

// UpsertContact inserts or replaces a contact row.
func (s *Store) UpsertContact(jid, name, notify, phone string) error {
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO contacts (jid, name, notify, phone)
			 VALUES (?, ?, ?, ?)
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
	err := s.readDB.QueryRow(
		`SELECT jid, name, notify, phone FROM contacts WHERE jid = ?`, jid,
	).Scan(&c.JID, &c.Name, &c.Notify, &c.Phone)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// ListContacts returns all contacts ordered by name.
func (s *Store) ListContacts() ([]ContactRow, error) {
	rows, err := s.readDB.Query(`SELECT jid, name, notify, phone FROM contacts ORDER BY name`)
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
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
	ChatJID string
	Sender  string
	After   time.Time
	Before  time.Time
	Query   string
	Limit   int
	Offset  int
}

// ListMessages returns messages matching the given filter.
func (s *Store) ListMessages(p QueryMessagesParams) ([]MessageRow, error) {
	where := []string{}
	args := []interface{}{}

	if p.ChatJID != "" {
		where = append(where, "chat_jid = ?")
		args = append(args, p.ChatJID)
	}
	if p.Sender != "" {
		where = append(where, "sender = ?")
		args = append(args, p.Sender)
	}
	if !p.After.IsZero() {
		where = append(where, "timestamp > ?")
		args = append(args, p.After)
	}
	if !p.Before.IsZero() {
		where = append(where, "timestamp < ?")
		args = append(args, p.Before)
	}
	if p.Query != "" {
		where = append(where, "content LIKE ?")
		args = append(args, "%"+p.Query+"%")
	}

	q := `SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
	      media_type, filename, url, media_key, file_sha256, file_enc_sha256,
	      file_length, push_name, quoted_message_id, quoted_participant
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
	q += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
	args = append(args, p.Limit, p.Offset)

	rows, err := s.readDB.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanMessages(rows)
}

// GetMessageContext returns the N messages before and after the given message.
func (s *Store) GetMessageContext(messageID, chatJID string, contextSize int) ([]MessageRow, error) {
	// Find the target message's timestamp.
	var ts time.Time
	err := s.readDB.QueryRow(
		`SELECT timestamp FROM messages WHERE id = ? AND chat_jid = ?`,
		messageID, chatJID,
	).Scan(&ts)
	if err != nil {
		return nil, err
	}

	rows, err := s.readDB.Query(
		`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
		 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
		 file_length, push_name, quoted_message_id, quoted_participant
		 FROM messages
		 WHERE chat_jid = ? AND timestamp BETWEEN ? AND ?
		 ORDER BY timestamp ASC`,
		chatJID,
		ts.Add(-time.Duration(contextSize)*time.Hour),
		ts.Add(time.Duration(contextSize)*time.Hour),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanMessages(rows)
}

// GetMessageByID fetches a single message by ID and chatJID.
func (s *Store) GetMessageByID(messageID, chatJID string) (*MessageRow, error) {
	rows, err := s.readDB.Query(
		`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
		 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
		 file_length, push_name, quoted_message_id, quoted_participant
		 FROM messages WHERE id = ? AND chat_jid = ?`,
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
	rows, err := s.readDB.Query(
		`SELECT jid, name, is_group, unread_count, last_message_time, last_message_preview
		 FROM chats WHERE unread_count > 0
		 ORDER BY last_message_time DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chats []ChatRow
	for rows.Next() {
		var c ChatRow
		if err := rows.Scan(&c.JID, &c.Name, &c.IsGroup, &c.UnreadCount, &c.LastMsgTime, &c.LastPreview); err != nil {
			return nil, err
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
		rows, err = s.readDB.Query(
			`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
			 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
			 file_length, push_name, quoted_message_id, quoted_participant
			 FROM messages WHERE timestamp > ? AND chat_jid = ?
			 ORDER BY timestamp ASC LIMIT ?`,
			since, chatJID, limit,
		)
	} else {
		rows, err = s.readDB.Query(
			`SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
			 media_type, filename, url, media_key, file_sha256, file_enc_sha256,
			 file_length, push_name, quoted_message_id, quoted_participant
			 FROM messages WHERE timestamp > ?
			 ORDER BY timestamp ASC LIMIT ?`,
			since, limit,
		)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanMessages(rows)
}

// CountMessages returns the total number of messages stored.
func (s *Store) CountMessages() (int64, error) {
	var n int64
	err := s.readDB.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&n)
	return n, err
}

// CountChats returns the total number of chats stored.
func (s *Store) CountChats() (int64, error) {
	var n int64
	err := s.readDB.QueryRow(`SELECT COUNT(*) FROM chats`).Scan(&n)
	return n, err
}

// MarkChatRead resets the unread_count for the given chat.
func (s *Store) MarkChatRead(jid string) error {
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE chats SET unread_count = 0 WHERE jid = ?`, jid)
		return err
	})
}

// IncrementUnread atomically increments unread_count by 1 for the given chat.
// The chat row must already exist (UpsertChat is called before this).
func (s *Store) IncrementUnread(chatJID string) error {
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE chats SET unread_count = unread_count + 1 WHERE jid = ?`,
			chatJID,
		)
		return err
	})
}

// ResetUnread sets unread_count to 0 for the given chat. Alias for MarkChatRead.
func (s *Store) ResetUnread(chatJID string) error {
	return s.MarkChatRead(chatJID)
}

func scanMessages(rows *sql.Rows) ([]MessageRow, error) {
	var msgs []MessageRow
	for rows.Next() {
		var m MessageRow
		err := rows.Scan(
			&m.ID, &m.ChatJID, &m.Sender, &m.SenderName, &m.Content,
			&m.Timestamp, &m.IsFromMe, &m.MediaType, &m.Filename, &m.URL,
			&m.MediaKey, &m.FileSHA256, &m.FileEncSHA256, &m.FileLength,
			&m.PushName, &m.QuotedMessageID, &m.QuotedParticipant,
		)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}
