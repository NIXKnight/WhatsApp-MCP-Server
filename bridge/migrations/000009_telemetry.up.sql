-- telemetry_daily: per-day rolling counters surfaced on the dashboard.
CREATE TABLE IF NOT EXISTS telemetry_daily (
    date              TEXT    PRIMARY KEY,
    messages_sent     INTEGER NOT NULL DEFAULT 0,
    messages_received INTEGER NOT NULL DEFAULT 0,
    media_downloaded  INTEGER NOT NULL DEFAULT 0,
    media_sent        INTEGER NOT NULL DEFAULT 0,
    links_indexed     INTEGER NOT NULL DEFAULT 0
);

-- telemetry_tool_calls: per-tool latency/success rows. Written by the
-- recording endpoint added in Phase 2b.
CREATE TABLE IF NOT EXISTS telemetry_tool_calls (
    id          BIGSERIAL   PRIMARY KEY,
    tool_name   TEXT        NOT NULL,
    duration_ms INTEGER     NOT NULL,
    success     BOOLEAN     NOT NULL,
    error_msg   TEXT        NOT NULL DEFAULT '',
    called_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_tool ON telemetry_tool_calls (tool_name, called_at DESC);
