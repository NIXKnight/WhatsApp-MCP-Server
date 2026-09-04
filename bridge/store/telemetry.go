package store

import (
	"database/sql"
	"fmt"
	"time"
)

// DailyTelemetry holds the per-day rolling counters.
type DailyTelemetry struct {
	Date             string `json:"date"`
	MessagesSent     int    `json:"messages_sent"`
	MessagesReceived int    `json:"messages_received"`
	MediaDownloaded  int    `json:"media_downloaded"`
	MediaSent        int    `json:"media_sent"`
	LinksIndexed     int    `json:"links_indexed"`
}

// ToolCall is one recorded MCP tool invocation with its latency and outcome.
type ToolCall struct {
	ID         int64     `json:"id"`
	ToolName   string    `json:"tool_name"`
	DurationMs int       `json:"duration_ms"`
	Success    bool      `json:"success"`
	ErrorMsg   string    `json:"error_msg,omitempty"`
	CalledAt   time.Time `json:"called_at"`
}

func today() string {
	return time.Now().Format("2006-01-02")
}

// allowedTelemetryFields whitelists the daily-counter columns that may be
// incremented. The field name is interpolated into SQL, so it MUST come from
// this map and never from caller input.
var allowedTelemetryFields = map[string]bool{
	"messages_sent":     true,
	"messages_received": true,
	"media_downloaded":  true,
	"media_sent":        true,
	"links_indexed":     true,
}

// IncrementTelemetry bumps the named daily counter by one for today's date.
// Unknown field names are rejected (logged and ignored) to keep the
// interpolation safe.
func (s *Store) IncrementTelemetry(field string) error {
	if !allowedTelemetryFields[field] {
		s.log.Warn("rejected unknown telemetry field", "field", field)
		return fmt.Errorf("unknown telemetry field: %s", field)
	}
	date := today()
	// field is whitelisted above, safe to interpolate.
	query := fmt.Sprintf(`
		INSERT INTO telemetry_daily (date, %s) VALUES ($1, 1)
		ON CONFLICT (date) DO UPDATE SET %s = telemetry_daily.%s + 1`, field, field, field)
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(query, date)
		return err
	})
}

// GetDailyTelemetry returns the counters for the given date (defaults to
// today). A date with no row yields a zero-valued struct, not an error.
func (s *Store) GetDailyTelemetry(date string) (*DailyTelemetry, error) {
	if date == "" {
		date = today()
	}
	t := &DailyTelemetry{Date: date}
	err := s.db.QueryRow(
		`SELECT date, messages_sent, messages_received, media_downloaded, media_sent, links_indexed
		 FROM telemetry_daily WHERE date = $1`, date,
	).Scan(&t.Date, &t.MessagesSent, &t.MessagesReceived, &t.MediaDownloaded, &t.MediaSent, &t.LinksIndexed)
	if err == sql.ErrNoRows {
		return t, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get daily telemetry: %w", err)
	}
	return t, nil
}

// RecordToolCall inserts a per-tool latency/success row. Called by the
// telemetry recording endpoint added in Phase 2b.
func (s *Store) RecordToolCall(tc *ToolCall) error {
	if tc.ToolName == "" {
		return nil
	}
	return s.submit(func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO telemetry_tool_calls (tool_name, duration_ms, success, error_msg)
			 VALUES ($1, $2, $3, $4)`,
			tc.ToolName, tc.DurationMs, tc.Success, tc.ErrorMsg,
		)
		return err
	})
}

// ToolCallQuery holds filter parameters for QueryToolCalls.
type ToolCallQuery struct {
	ToolName string
	Limit    int
	Offset   int
}

// QueryToolCalls returns recorded tool calls, newest-first, plus the total
// matching count for pagination.
func (s *Store) QueryToolCalls(q ToolCallQuery) ([]ToolCall, int, error) {
	if q.Limit <= 0 {
		q.Limit = 50
	}

	where := "1=1"
	al := &argList{}
	if q.ToolName != "" {
		where += " AND tool_name = " + al.add(q.ToolName)
	}

	var total int
	if err := s.db.QueryRow(
		fmt.Sprintf("SELECT COUNT(*) FROM telemetry_tool_calls WHERE %s", where), al.args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count tool calls: %w", err)
	}

	q2 := fmt.Sprintf(`
		SELECT id, tool_name, duration_ms, success, error_msg, called_at
		FROM telemetry_tool_calls WHERE %s ORDER BY called_at DESC LIMIT %s OFFSET %s`,
		where, al.add(q.Limit), al.add(q.Offset))

	rows, err := s.db.Query(q2, al.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query tool calls: %w", err)
	}
	defer rows.Close()

	var calls []ToolCall
	for rows.Next() {
		var tc ToolCall
		var calledAt sql.NullTime
		if err := rows.Scan(&tc.ID, &tc.ToolName, &tc.DurationMs, &tc.Success, &tc.ErrorMsg, &calledAt); err != nil {
			return nil, 0, fmt.Errorf("scan tool call: %w", err)
		}
		if calledAt.Valid {
			tc.CalledAt = calledAt.Time
		}
		calls = append(calls, tc)
	}
	return calls, total, rows.Err()
}
