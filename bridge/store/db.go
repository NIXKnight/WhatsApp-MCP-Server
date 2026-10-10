// Package store implements the L3 PostgreSQL persistence layer for the WhatsApp
// bridge — the system's source of truth. It owns the connection pool, the
// golang-migrate migration runner, a single-writer goroutine that serialises
// all writes, and the domain query methods for messages, chats, contacts, and
// hybrid (FTS + pgvector RRF) search.
package store

import (
	"context"
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

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/migrations"
)

// EmbeddingDim is the fixed dimensionality of all pgvector columns in the
// schema (message_embeddings.embedding, chats.topic_embedding,
// contacts.name_embedding). It matches the MiniLM enrichment embedder and the
// vector(384) columns created by migration 000006. All embedding validation
// must reference this constant so the API can never drift from the schema.
const EmbeddingDim = 384

// WriteOp is a single write operation submitted to the store's single-writer goroutine.
type WriteOp struct {
	ctx  context.Context
	fn   func(*sql.Tx) error
	done chan error
}

// Store manages the PostgreSQL message database with a single writer goroutine.
type Store struct {
	db           *sql.DB
	writeCh      chan WriteOp
	log          *slog.Logger
	done         chan struct{}
	admissionMu  sync.Mutex // prevents admission after shutdown begins
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
	// Close admission before the writer starts draining.
	s.admissionMu.Lock()
	s.doneOnce.Do(func() { close(s.done) })
	s.admissionMu.Unlock()
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
			op.done <- s.execWriteContext(op.ctx, op.fn)
		case <-s.done:
			// Drain remaining ops already queued before exiting.
			for {
				select {
				case op := <-s.writeCh:
					op.done <- s.execWriteContext(op.ctx, op.fn)
				default:
					return
				}
			}
		}
	}
}

func (s *Store) execWriteContext(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// submit preserves synchronous commit acknowledgement for existing callers.
func (s *Store) submit(fn func(*sql.Tx) error) error {
	return s.submitContext(context.Background(), fn)
}

// submitContext bounds queue waiting and SQL execution. On cancellation an
// admitted operation may have committed; callers must not read closure-owned
// results on error. The buffered completion channel lets the writer finish.
func (s *Store) submitContext(ctx context.Context, fn func(*sql.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	op := WriteOp{ctx: ctx, fn: fn, done: make(chan error, 1)}
	s.admissionMu.Lock()
	select {
	case <-s.done:
		s.admissionMu.Unlock()
		return fmt.Errorf("store is closed")
	default:
	}
	select {
	case s.writeCh <- op:
		s.admissionMu.Unlock()
	default:
		s.admissionMu.Unlock()
		s.log.Warn("write channel saturated, dropping write operation")
		return fmt.Errorf("write channel saturated")
	}
	select {
	case err := <-op.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
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

// scanMessages scans a *sql.Rows produced by the standard message SELECT
// column list into MessageRow values. The expected column order is the 17 base
// message columns, then the snippet column, then the two messages_media
// transcription columns (transcription, transcribed_at) supplied by the LEFT
// JOIN in the message queries.
func scanMessages(rows *sql.Rows) ([]MessageRow, error) {
	var msgs []MessageRow
	for rows.Next() {
		var m MessageRow
		var ts sql.NullTime
		var fileLength sql.NullInt64
		var transcribedAt sql.NullTime
		err := rows.Scan(
			&m.ID, &m.ChatJID, &m.Sender, &m.SenderName, &m.Content,
			&ts, &m.IsFromMe, &m.MediaType, &m.Filename, &m.URL,
			&m.MediaKey, &m.FileSHA256, &m.FileEncSHA256, &fileLength,
			&m.PushName, &m.QuotedMessageID, &m.QuotedParticipant, &m.Snippet,
			&m.Transcription, &transcribedAt,
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
		if transcribedAt.Valid {
			m.TranscribedAt = transcribedAt.Time
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}
