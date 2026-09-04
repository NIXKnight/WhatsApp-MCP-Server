package store

import (
	"database/sql"

	pgvector "github.com/pgvector/pgvector-go"
)

// ContactRow is the database representation of a contact.
type ContactRow struct {
	JID    string
	Name   string
	Notify string
	Phone  string
}

// SimilarContactPair holds a pair of contacts with similar names.
type SimilarContactPair struct {
	JIDA       string
	NameA      string
	JIDB       string
	NameB      string
	Similarity float64
}

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
