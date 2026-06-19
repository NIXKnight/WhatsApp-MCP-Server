package store

import (
	"database/sql"
	"fmt"
	"time"
)

// MediaRow is the database representation of a messages_media row. It captures
// everything the enrichment workers (downloader, transcriber, OCR) need to act
// on a media message. Worker-owned fields (LocalPath, DownloadedAt, the
// download_* lifecycle, Transcription, TranscriptionLang, TranscribedAt) are
// not written by the capture-side upsert and are therefore left untouched on
// re-ingest.
type MediaRow struct {
	MessageID     string
	ChatJID       string
	MediaType     string // image|video|audio|sticker|document
	MimeType      string
	Filename      string
	FileLength    int64
	MediaKey      []byte
	FileSHA256    []byte
	FileEncSHA256 []byte
	URL           string
	DirectPath    string

	// Worker-owned read-back fields (populated by SELECTs, not the capture upsert).
	LocalPath                 string
	DownloadedAt              time.Time
	DownloadAttempts          int
	DownloadPermanentlyFailed bool
	Transcription             string
	TranscriptionLang         string
	TranscribedAt             time.Time
}

// UpsertMessageMedia inserts or updates the messages_media row for a media
// message captured from a live event or history sync.
//
// Identity/crypto fields (media_key, file_sha256, file_enc_sha256, url,
// direct_path, mime_type, filename, file_length) are COALESCE-preserved: a
// later re-ingest that arrives with these fields stripped (a common history
// sync case) keeps the originally captured values instead of nulling them,
// which would render the media permanently undecryptable.
//
// media_type is always overwritten, since a later, fuller ingest may correct
// it. Worker-owned columns are absent from the UPDATE list, so a re-ingest
// never clobbers a downloaded path or an existing transcription.
func (s *Store) UpsertMessageMedia(m *MediaRow) error {
	if m.MessageID == "" || m.ChatJID == "" || m.MediaType == "" {
		return nil
	}
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO messages_media
			 (message_id, chat_jid, media_type, mime_type, filename, file_length,
			  media_key, file_sha256, file_enc_sha256, url, direct_path)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			 ON CONFLICT (message_id, chat_jid) DO UPDATE SET
			     media_type      = excluded.media_type,
			     mime_type       = COALESCE(excluded.mime_type, messages_media.mime_type),
			     filename        = COALESCE(excluded.filename, messages_media.filename),
			     file_length     = COALESCE(excluded.file_length, messages_media.file_length),
			     media_key       = COALESCE(excluded.media_key, messages_media.media_key),
			     file_sha256     = COALESCE(excluded.file_sha256, messages_media.file_sha256),
			     file_enc_sha256 = COALESCE(excluded.file_enc_sha256, messages_media.file_enc_sha256),
			     url             = COALESCE(excluded.url, messages_media.url),
			     direct_path     = COALESCE(excluded.direct_path, messages_media.direct_path)`,
			m.MessageID, m.ChatJID, m.MediaType,
			nullString(m.MimeType), nullString(m.Filename), nullInt64(m.FileLength),
			nullBytes(m.MediaKey), nullBytes(m.FileSHA256), nullBytes(m.FileEncSHA256),
			nullString(m.URL), nullString(m.DirectPath),
		)
		return err
	})
}

// MarkMediaDownloaded records a successful download: stores the local path,
// stamps downloaded_at, and resets the failure state.
func (s *Store) MarkMediaDownloaded(messageID, chatJID, localPath string) error {
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE messages_media
			    SET local_path = $1,
			        downloaded_at = NOW(),
			        download_last_error = NULL,
			        download_permanently_failed = FALSE
			  WHERE message_id = $2 AND chat_jid = $3`,
			localPath, messageID, chatJID,
		)
		return err
	})
}

// MarkMediaDownloadFailed increments the attempt counter and records the error.
// When permanent is true the row is flagged so polling workers skip it.
func (s *Store) MarkMediaDownloadFailed(messageID, chatJID, errMsg string, permanent bool) error {
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE messages_media
			    SET download_attempts = download_attempts + 1,
			        download_last_error = $1,
			        download_last_attempt_at = NOW(),
			        download_permanently_failed = $2
			  WHERE message_id = $3 AND chat_jid = $4`,
			errMsg, permanent, messageID, chatJID,
		)
		return err
	})
}

// SetMediaTranscription stores a transcription result and stamps transcribed_at.
func (s *Store) SetMediaTranscription(messageID, chatJID, transcription, lang string) error {
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`UPDATE messages_media
			    SET transcription = $1,
			        transcription_lang = $2,
			        transcribed_at = NOW()
			  WHERE message_id = $3 AND chat_jid = $4`,
			transcription, nullString(lang), messageID, chatJID,
		)
		return err
	})
}

// GetMessageMedia returns the messages_media row for a message, or
// sql.ErrNoRows when the message has no media row.
func (s *Store) GetMessageMedia(messageID, chatJID string) (*MediaRow, error) {
	row := s.db.QueryRow(
		`SELECT message_id, chat_jid, media_type,
		        COALESCE(mime_type, ''), COALESCE(filename, ''), COALESCE(file_length, 0),
		        media_key, file_sha256, file_enc_sha256,
		        COALESCE(url, ''), COALESCE(direct_path, ''),
		        COALESCE(local_path, ''), downloaded_at, download_attempts,
		        download_permanently_failed,
		        COALESCE(transcription, ''), COALESCE(transcription_lang, ''), transcribed_at
		   FROM messages_media WHERE message_id = $1 AND chat_jid = $2`,
		messageID, chatJID,
	)
	return scanMediaRow(row)
}

// ListPendingTranscriptions returns audio media rows that have not been
// transcribed yet, have a local file present, and have not permanently failed.
// The transcriber worker polls this.
func (s *Store) ListPendingTranscriptions(limit int) ([]MediaRow, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(
		`SELECT message_id, chat_jid, media_type,
		        COALESCE(mime_type, ''), COALESCE(filename, ''), COALESCE(file_length, 0),
		        media_key, file_sha256, file_enc_sha256,
		        COALESCE(url, ''), COALESCE(direct_path, ''),
		        COALESCE(local_path, ''), downloaded_at, download_attempts,
		        download_permanently_failed,
		        COALESCE(transcription, ''), COALESCE(transcription_lang, ''), transcribed_at
		   FROM messages_media
		  WHERE media_type = 'audio'
		    AND transcribed_at IS NULL
		    AND NOT download_permanently_failed
		    AND local_path IS NOT NULL
		  ORDER BY created_at ASC
		  LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending transcriptions: %w", err)
	}
	defer rows.Close()

	var out []MediaRow
	for rows.Next() {
		m, err := scanMediaRowFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *m)
	}
	return out, rows.Err()
}

func scanMediaRow(row *sql.Row) (*MediaRow, error) {
	var m MediaRow
	var dlAt, trAt sql.NullTime
	err := row.Scan(
		&m.MessageID, &m.ChatJID, &m.MediaType,
		&m.MimeType, &m.Filename, &m.FileLength,
		&m.MediaKey, &m.FileSHA256, &m.FileEncSHA256,
		&m.URL, &m.DirectPath,
		&m.LocalPath, &dlAt, &m.DownloadAttempts,
		&m.DownloadPermanentlyFailed,
		&m.Transcription, &m.TranscriptionLang, &trAt,
	)
	if err != nil {
		return nil, err
	}
	if dlAt.Valid {
		m.DownloadedAt = dlAt.Time
	}
	if trAt.Valid {
		m.TranscribedAt = trAt.Time
	}
	return &m, nil
}

func scanMediaRowFromRows(rows *sql.Rows) (*MediaRow, error) {
	var m MediaRow
	var dlAt, trAt sql.NullTime
	err := rows.Scan(
		&m.MessageID, &m.ChatJID, &m.MediaType,
		&m.MimeType, &m.Filename, &m.FileLength,
		&m.MediaKey, &m.FileSHA256, &m.FileEncSHA256,
		&m.URL, &m.DirectPath,
		&m.LocalPath, &dlAt, &m.DownloadAttempts,
		&m.DownloadPermanentlyFailed,
		&m.Transcription, &m.TranscriptionLang, &trAt,
	)
	if err != nil {
		return nil, err
	}
	if dlAt.Valid {
		m.DownloadedAt = dlAt.Time
	}
	if trAt.Valid {
		m.TranscribedAt = trAt.Time
	}
	return &m, nil
}

// ---- NULL helpers: empty/zero capture values map to SQL NULL so that the
// COALESCE-preserving upsert treats "not provided" distinctly from "set to
// empty". ----

func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullInt64(n int64) interface{} {
	if n == 0 {
		return nil
	}
	return n
}

func nullBytes(b []byte) interface{} {
	if len(b) == 0 {
		return nil
	}
	return b
}
