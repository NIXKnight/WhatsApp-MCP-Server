// Command whatsapp-bridge-go is a REST API server that connects to WhatsApp
// via whatsmeow and exposes endpoints for a Python MCP server to consume.
//
// Usage:
//
//	./whatsapp-bridge-go
//
// Configuration via environment variables:
//
//	BRIDGE_ADDR      — HTTP bind address (default: 127.0.0.1:8080)
//	BRIDGE_DATA_DIR  — data directory for SQLite and media (default: ./data)
//	BRIDGE_LOG_LEVEL — log verbosity: debug|info|warn|error (default: info)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/api"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/bridge"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/config"
)

// cooldownDuration is the delay imposed after a permanent disconnect before
// the bridge is allowed to restart and attempt a new connection. A Docker or
// systemd supervisor that restarts the process immediately will receive exit
// code 2 and should be configured to treat that as "do not restart".
const cooldownDuration = 10 * time.Minute

// cooldownMarkerPath returns the path of the cooldown marker file.
func cooldownMarkerPath(dataDir string) string {
	return filepath.Join(dataDir, "cooldown")
}

// checkCooldownMarker reads the cooldown marker file and returns the time until
// which the bridge must not reconnect. Returns an error if no valid marker exists.
func checkCooldownMarker(dataDir string) (time.Time, error) {
	data, err := os.ReadFile(cooldownMarkerPath(dataDir))
	if err != nil {
		return time.Time{}, err
	}
	unixMs, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("malformed cooldown marker: %w", err)
	}
	return time.UnixMilli(unixMs), nil
}

// writeCooldownMarker writes a marker file recording that the bridge must not
// reconnect until now+cooldownDuration.
func writeCooldownMarker(dataDir string) {
	until := time.Now().Add(cooldownDuration)
	data := []byte(strconv.FormatInt(until.UnixMilli(), 10))
	path := cooldownMarkerPath(dataDir)
	if err := os.WriteFile(path, data, 0600); err != nil {
		slog.Error("failed to write cooldown marker", "path", path, "err", err)
	}
}

// clearCooldownMarker removes the cooldown marker after a successful connection.
func clearCooldownMarker(dataDir string) {
	_ = os.Remove(cooldownMarkerPath(dataDir))
}

func main() {
	// ---- Load configuration ------------------------------------------------
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration error", "err", err)
		os.Exit(1)
	}

	// ---- Structured logger -------------------------------------------------
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	})
	log := slog.New(handler).With("service", "whatsapp-bridge-go")
	slog.SetDefault(log)

	log.Info("starting", "addr", cfg.Addr, "data_dir", cfg.DataDir)

	// ---- Cooldown check ----------------------------------------------------
	// If a previous run wrote a cooldown marker (permanent disconnect / ban),
	// refuse to start until the cooldown expires. Exit code 2 signals to
	// systemd/Docker restart policies that this is a deliberate hold-off.
	if until, err := checkCooldownMarker(cfg.DataDir); err == nil && until.After(time.Now()) {
		log.Error("connection cooldown active, refusing to start",
			"until", until.Format(time.RFC3339),
			"remaining", time.Until(until).Round(time.Second).String(),
		)
		os.Exit(2)
	}

	// ---- Message store -----------------------------------------------------
	store, err := bridge.NewStore(cfg.DataDir, log.With("component", "store"))
	if err != nil {
		log.Error("failed to open message store", "err", err)
		os.Exit(1)
	}

	// ---- WhatsApp client ---------------------------------------------------
	client, err := bridge.NewClient(cfg.DataDir, store, log.With("component", "client"))
	if err != nil {
		log.Error("failed to create WhatsApp client", "err", err)
		store.Close()
		os.Exit(1)
	}

	// ---- HTTP API server ---------------------------------------------------
	// Start the HTTP server before connecting to WhatsApp so that /api/status
	// is reachable during QR code scanning. Docker health checks and other
	// probes will get a 200 response with state=QR_WAITING instead of a
	// "connection refused" error.
	h := api.NewHandler(client, store, log.With("component", "api"))
	srv := api.NewServer(cfg.Addr, h, log.With("component", "http"))

	srvErrCh := make(chan error, 1)
	go func() {
		srvErrCh <- srv.Start()
	}()

	// ---- Signal handler for graceful shutdown ------------------------------
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// ---- Connect to WhatsApp (non-blocking) --------------------------------
	connectCtx, connectCancel := context.WithCancel(context.Background())
	defer connectCancel()

	connectErrCh := make(chan error, 1)
	go func() {
		connectErrCh <- client.Connect(connectCtx)
	}()

	// Wait for connection, a fatal HTTP server error, or a signal.
	select {
	case err := <-connectErrCh:
		if err != nil {
			log.Error("WhatsApp connection failed", "err", err)
			// If the client ended up in a permanently disconnected state (ban,
			// logged out, client outdated, etc.) write a cooldown marker so that
			// a supervisor restart does not immediately hammer the server again.
			if client.State() == bridge.StatePermanentlyDisconnected {
				writeCooldownMarker(cfg.DataDir)
				log.Error("permanent disconnect detected, cooldown marker written",
					"cooldown", cooldownDuration.String(),
				)
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer shutdownCancel()
				_ = srv.Shutdown(shutdownCtx)
				store.Close()
				os.Exit(2) // exit code 2: tell supervisor not to restart yet
			}
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer shutdownCancel()
			_ = srv.Shutdown(shutdownCtx)
			store.Close()
			os.Exit(1)
		}
	case sig := <-sigCh:
		log.Info("received signal before connection established, shutting down", "signal", sig)
		connectCancel()
		client.Disconnect()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
		store.Close()
		os.Exit(0)
	case err := <-srvErrCh:
		log.Error("HTTP server error before connection established", "err", err)
		connectCancel()
		client.Disconnect()
		store.Close()
		os.Exit(1)
	}

	// Successful connection: remove any stale cooldown marker.
	clearCooldownMarker(cfg.DataDir)
	log.Info("WhatsApp connection established")

	// ---- Wait for termination signal ---------------------------------------
	select {
	case sig := <-sigCh:
		log.Info("received signal, initiating graceful shutdown", "signal", sig)
	case err := <-srvErrCh:
		log.Error("HTTP server error", "err", err)
	}

	// ---- Graceful shutdown sequence ----------------------------------------
	// 1. Drain in-flight HTTP requests (15 s deadline).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("HTTP server shutdown did not complete cleanly", "err", err)
	}
	log.Info("HTTP server stopped")

	// 2. Disconnect from WhatsApp.
	client.Disconnect()
	log.Info("WhatsApp client disconnected")

	// 3. Close the message store.
	if err := store.Close(); err != nil {
		log.Warn("store close error", "err", err)
	}
	log.Info("shutdown complete")
}
