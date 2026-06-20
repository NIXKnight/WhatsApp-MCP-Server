// Package config provides L2 environment-based configuration for the WhatsApp bridge.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// defaultEmbedderTimeout bounds each query-time embedding request to the L3
// embedder service. Search degrades to FTS-only when this deadline is exceeded,
// so it is kept short to avoid stalling interactive searches.
const defaultEmbedderTimeout = 2 * time.Second

// defaultAnalyzerTimeout bounds each media-analysis request to the L3
// transcriber service. Analysis is heavy (video frame extraction, audio
// transcription) so the deadline is generous relative to other outbound calls.
const defaultAnalyzerTimeout = 180 * time.Second

// Media-retry worker defaults. The worker re-downloads non-audio media that was
// requested on-demand and failed (download_attempts > 0). The poll interval is
// itself the cross-cycle backoff between attempts — a 403/410 is usually a stale
// signed URL that a later download call refreshes, so spacing retries by the
// interval is the recovery mechanism (no tight in-loop retry).
const (
	defaultMediaRetryInterval       = 3 * time.Minute
	defaultMediaMaxDownloadAttempts = 3
	defaultMediaRetryBatchSize      = 10
)

// Config holds all runtime configuration for the bridge.
type Config struct {
	// Addr is the TCP address to bind the HTTP server to (default: 127.0.0.1:8080).
	Addr string
	// DataDir is the directory for media file storage.
	DataDir string
	// DatabaseURL is the PostgreSQL connection string (required).
	DatabaseURL string
	// LogLevel controls slog output verbosity (debug, info, warn, error).
	LogLevel slog.Level
	// EmbedderURL is the base URL of the L3 embedder service used for
	// query-time vector embeddings (default: http://embedder:8000).
	EmbedderURL string
	// EmbedderTimeout bounds each outbound embedding request.
	EmbedderTimeout time.Duration
	// AnalyzerURL is the base URL of the L3 transcriber service used for
	// on-demand media analysis (default: http://transcriber:8500).
	AnalyzerURL string
	// AnalyzerTimeout bounds each outbound media-analysis request.
	AnalyzerTimeout time.Duration

	// MediaRetryEnabled gates the background media re-download worker. When
	// false the goroutine is never started (env MEDIA_RETRY_ENABLED, default true).
	MediaRetryEnabled bool
	// MediaRetryInterval is the poll cadence of the media-retry worker and
	// doubles as the cross-cycle backoff between attempts
	// (env MEDIA_RETRY_INTERVAL, default 3m).
	MediaRetryInterval time.Duration
	// MediaMaxDownloadAttempts bounds how many times a single media row is
	// retried before it is flagged download_permanently_failed
	// (env MEDIA_MAX_DOWNLOAD_ATTEMPTS, default 3).
	MediaMaxDownloadAttempts int
	// MediaRetryBatchSize bounds how many retry-eligible rows a single cycle
	// processes (env MEDIA_RETRY_BATCH_SIZE, default 10).
	MediaRetryBatchSize int
}

// Load reads configuration from environment variables, applying defaults for
// any unset variables. It validates the data directory exists and is writable,
// and warns if the bind address is not restricted to the loopback interface.
func Load() (*Config, error) {
	addr := getEnv("BRIDGE_ADDR", "127.0.0.1:8080")
	dataDir := getEnv("BRIDGE_DATA_DIR", "./data")
	logLevelStr := getEnv("BRIDGE_LOG_LEVEL", "info")
	embedderURL := getEnv("EMBEDDER_URL", "http://embedder:8000")
	analyzerURL := getEnv("ANALYZER_URL", "http://transcriber:8500")

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	logLevel, err := parseLogLevel(logLevelStr)
	if err != nil {
		return nil, fmt.Errorf("invalid BRIDGE_LOG_LEVEL %q: %w", logLevelStr, err)
	}

	// Security: warn if the API is being bound to a non-loopback address.
	// This bridge has no authentication; it must not be reachable from the network.
	if !isLoopbackAddr(addr) {
		fmt.Fprintf(os.Stderr,
			"WARNING: BRIDGE_ADDR %q does not start with 127.0.0.1 or localhost. "+
				"The bridge API has no authentication and should only be accessible locally.\n",
			addr,
		)
	}

	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolving BRIDGE_DATA_DIR: %w", err)
	}

	if err := ensureDataDir(absDataDir); err != nil {
		return nil, fmt.Errorf("preparing data directory %q: %w", absDataDir, err)
	}

	return &Config{
		Addr:                     addr,
		DataDir:                  absDataDir,
		DatabaseURL:              databaseURL,
		LogLevel:                 logLevel,
		EmbedderURL:              embedderURL,
		EmbedderTimeout:          defaultEmbedderTimeout,
		AnalyzerURL:              analyzerURL,
		AnalyzerTimeout:          defaultAnalyzerTimeout,
		MediaRetryEnabled:        getEnvBool("MEDIA_RETRY_ENABLED", true),
		MediaRetryInterval:       getEnvDuration("MEDIA_RETRY_INTERVAL", defaultMediaRetryInterval),
		MediaMaxDownloadAttempts: getEnvInt("MEDIA_MAX_DOWNLOAD_ATTEMPTS", defaultMediaMaxDownloadAttempts),
		MediaRetryBatchSize:      getEnvInt("MEDIA_RETRY_BATCH_SIZE", defaultMediaRetryBatchSize),
	}, nil
}

// isLoopbackAddr returns true when addr is bound to the loopback interface.
func isLoopbackAddr(addr string) bool {
	return strings.HasPrefix(addr, "127.0.0.1:") ||
		strings.HasPrefix(addr, "localhost:") ||
		strings.HasPrefix(addr, "[::1]:")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// getEnvBool parses a boolean env var (1/t/T/TRUE/true/0/f/false/...). An unset
// or unparseable value falls back to def, with a warning on a parse failure.
func getEnvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: %s=%q is not a boolean, using default %v\n", key, v, def)
		return def
	}
	return b
}

// getEnvDuration parses a Go duration env var (e.g. "3m", "90s"). An unset or
// unparseable value falls back to def, with a warning on a parse failure.
func getEnvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: %s=%q is not a duration, using default %s\n", key, v, def)
		return def
	}
	return d
}

// getEnvInt parses an integer env var. An unset, unparseable, or non-positive
// value falls back to def, with a warning on a parse failure.
func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		fmt.Fprintf(os.Stderr, "WARNING: %s=%q is not a positive integer, using default %d\n", key, v, def)
		return def
	}
	return n
}

func parseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unknown level %q, expected debug|info|warn|error", s)
	}
}

// ensureDataDir creates the directory if it does not exist and verifies
// write permissions by creating and removing a temporary file.
func ensureDataDir(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	// Verify write permissions.
	probe := filepath.Join(dir, ".write_probe")
	f, err := os.Create(probe)
	if err != nil {
		return fmt.Errorf("write permission check failed: %w", err)
	}
	f.Close()
	os.Remove(probe)

	return nil
}
