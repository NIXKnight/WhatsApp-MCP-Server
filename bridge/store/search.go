package store

import (
	"database/sql"
	"fmt"
	"time"

	pgvector "github.com/pgvector/pgvector-go"
)

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

// MessageWithScore is a message paired with a similarity distance score.
type MessageWithScore struct {
	ID         string
	ChatJID    string
	Content    string
	Timestamp  time.Time
	SenderName string
	Distance   float64
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
