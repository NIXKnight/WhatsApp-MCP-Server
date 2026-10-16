// Package config provides L2 environment-based configuration for the WhatsApp bridge.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
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

// NonLoopbackWarningToken is the fixed warning emitted when the bridge API is
// configured on a non-loopback address.
const NonLoopbackWarningToken = "bridge_config_non_loopback_BRIDGE_ADDR"

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
	// Notifications controls the opt-in metadata-only incoming-message relay.
	Notifications Notifications
	// Managed selects the supervisor-managed, non-interactive lifecycle.
	Managed bool
	// Warnings contains sorted, deduplicated, value-free optional-setting warnings.
	Warnings []string
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
// any unset variables. It validates the data directory exists and is writable.
// Loading is deliberately output-free; callers decide when warnings are safe
// to emit.
func Load() (*Config, error) {
	managed, err := parseManaged(os.Getenv)
	if err != nil {
		return nil, err
	}
	notifications, err := loadNotifications(os.Getenv)
	if err != nil {
		return nil, err
	}
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

	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolving BRIDGE_DATA_DIR: %w", err)
	}
	if err := ensureDataDir(absDataDir); err != nil {
		return nil, fmt.Errorf("preparing data directory %q: %w", absDataDir, err)
	}

	warnings := newWarningCollector()
	cfg := &Config{
		Notifications:            notifications,
		Managed:                  managed,
		Addr:                     addr,
		DataDir:                  absDataDir,
		DatabaseURL:              databaseURL,
		LogLevel:                 logLevel,
		EmbedderURL:              embedderURL,
		EmbedderTimeout:          defaultEmbedderTimeout,
		AnalyzerURL:              analyzerURL,
		AnalyzerTimeout:          defaultAnalyzerTimeout,
		MediaRetryEnabled:        getEnvBoolWithWarnings(os.Getenv, "MEDIA_RETRY_ENABLED", true, warnings),
		MediaRetryInterval:       getEnvDurationWithWarnings(os.Getenv, "MEDIA_RETRY_INTERVAL", defaultMediaRetryInterval, warnings),
		MediaMaxDownloadAttempts: getEnvIntWithWarnings(os.Getenv, "MEDIA_MAX_DOWNLOAD_ATTEMPTS", defaultMediaMaxDownloadAttempts, warnings),
		MediaRetryBatchSize:      getEnvIntWithWarnings(os.Getenv, "MEDIA_RETRY_BATCH_SIZE", defaultMediaRetryBatchSize, warnings),
	}
	cfg.Warnings = warnings.sorted()
	return cfg, nil
}

func parseManaged(getenv func(string) string) (bool, error) {
	switch getenv("BRIDGE_MANAGED") {
	case "":
		return false, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, errors.New("BRIDGE_MANAGED_invalid")
	}
}

// NonLoopbackWarning returns a fixed, value-free warning token for a
// non-loopback bind address, or an empty string for a loopback address.
func NonLoopbackWarning(addr string) string {
	if isLoopbackAddr(addr) {
		return ""
	}
	return NonLoopbackWarningToken
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

type warningCollector map[string]struct{}

func newWarningCollector() warningCollector { return make(warningCollector) }

func (w warningCollector) add(class, key string) {
	if w != nil {
		w["bridge_config_invalid_"+class+"_"+key] = struct{}{}
	}
}

func (w warningCollector) sorted() []string {
	out := make([]string, 0, len(w))
	for warning := range w {
		out = append(out, warning)
	}
	sort.Strings(out)
	return out
}

// getEnvBool parses a boolean env var. An unset or unparseable value falls
// back to def. Config loading uses the collector variant to retain a fixed
// warning without writing output.
func getEnvBool(key string, def bool) bool {
	return getEnvBoolWithWarnings(os.Getenv, key, def, nil)
}

func getEnvBoolWithWarnings(getenv func(string) string, key string, def bool, warnings warningCollector) bool {
	v := getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		warnings.add("bool", key)
		return def
	}
	return b
}

// getEnvDuration parses a Go duration env var (e.g. "3m", "90s"). An unset
// or unparseable value falls back to def.
func getEnvDuration(key string, def time.Duration) time.Duration {
	return getEnvDurationWithWarnings(os.Getenv, key, def, nil)
}

func getEnvDurationWithWarnings(getenv func(string) string, key string, def time.Duration, warnings warningCollector) time.Duration {
	v := getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		warnings.add("duration", key)
		return def
	}
	return d
}

// getEnvInt parses a positive integer env var. An unset, unparseable, or
// non-positive value falls back to def.
func getEnvInt(key string, def int) int {
	return getEnvIntWithWarnings(os.Getenv, key, def, nil)
}

func getEnvIntWithWarnings(getenv func(string) string, key string, def int, warnings warningCollector) int {
	v := getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		warnings.add("int", key)
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
