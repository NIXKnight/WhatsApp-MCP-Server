// Package config provides environment-based configuration for the WhatsApp bridge.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
}

// Load reads configuration from environment variables, applying defaults for
// any unset variables. It validates the data directory exists and is writable,
// and warns if the bind address is not restricted to the loopback interface.
func Load() (*Config, error) {
	addr := getEnv("BRIDGE_ADDR", "127.0.0.1:8080")
	dataDir := getEnv("BRIDGE_DATA_DIR", "./data")
	logLevelStr := getEnv("BRIDGE_LOG_LEVEL", "info")

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
		Addr:        addr,
		DataDir:     absDataDir,
		DatabaseURL: databaseURL,
		LogLevel:    logLevel,
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
