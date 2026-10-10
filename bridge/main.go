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
	"sync"
	"syscall"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/analyzer"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/api"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/client"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/config"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/embed"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/mediaretry"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/notify"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
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

func main() { os.Exit(run()) }

func run() int {
	// ---- Load configuration ------------------------------------------------
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration error", "err", err)
		return 1
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
		return 2
	}

	// ---- Message store -----------------------------------------------------
	st, err := store.NewStore(cfg.DatabaseURL, log.With("component", "store"))
	if err != nil {
		log.Error("failed to open message store", "err", err)
		return 1
	}

	resources := bridgeCleanup{closeStore: st.Close, log: log}
	defer resources.run()
	var options []client.Option
	if cfg.Notifications.Enabled {
		options = append(options, client.WithIncomingNotifications(cfg.Notifications.ChatAllowlist, cfg.Notifications.MaxAge))
	}

	// ---- WhatsApp client ---------------------------------------------------
	waClient, err := client.NewClient(cfg.DatabaseURL, cfg.DataDir, st, log.With("component", "client"), options...)
	if err != nil {
		log.Error("failed to create WhatsApp client", "err", err)
		return 1
	}

	resources.disconnect = waClient.Disconnect

	// ---- HTTP API server ---------------------------------------------------
	// Start the HTTP server before connecting to WhatsApp so that /api/status
	// is reachable during QR code scanning. Docker health checks and other
	// probes will get a 200 response with state=QR_WAITING instead of a
	// "connection refused" error.
	embedder := embed.New(cfg.EmbedderURL, cfg.EmbedderTimeout)
	analyzer := analyzer.New(cfg.AnalyzerURL, cfg.AnalyzerTimeout)
	h := api.NewHandler(waClient, st, embedder, analyzer, cfg.MediaMaxDownloadAttempts, log.With("component", "api"))
	srv := api.NewServer(cfg.Addr, h, log.With("component", "http"))
	resources.stopHTTP = srv.Shutdown

	srvErrCh := make(chan error, 1)
	go func() {
		srvErrCh <- srv.Start()
	}()

	// ---- Signal handler for graceful shutdown ------------------------------
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// ---- Connect to WhatsApp (non-blocking) --------------------------------
	connectCtx, connectCancel := context.WithCancel(context.Background())
	resources.cancelConnect = connectCancel

	// Notifications do not depend on WhatsApp connection state. Disabled mode
	// constructs no sender and performs only bounded local maintenance.
	var sender *notify.Sender
	if cfg.Notifications.Enabled {
		sender, err = notify.NewSender(cfg.Notifications)
		if err != nil {
			log.Error("notification initialization failed", "class", "configuration_invalid")
			return 1
		}
	}
	notifyCtx, notifyCancel := context.WithCancel(context.Background())
	var notifyWG sync.WaitGroup
	notifyWorker := notify.NewWorker(st, cfg.Notifications, sender, log.With("component", "notify"))
	notifyWG.Add(1)
	go func() { defer notifyWG.Done(); notifyWorker.Run(notifyCtx) }()
	var mediaRetryCancel context.CancelFunc
	var mediaRetryWG sync.WaitGroup
	resources.stopWorkers = func() {
		notifyCancel()
		if mediaRetryCancel != nil {
			mediaRetryCancel()
		}
		notifyWG.Wait()
		mediaRetryWG.Wait()
		if sender != nil {
			sender.Close()
		}
	}

	connectErrCh := make(chan error, 1)
	go func() {
		connectErrCh <- waClient.Connect(connectCtx)
	}()

	// Wait for connection, a fatal HTTP server error, or a signal.
	select {
	case err := <-connectErrCh:
		if err != nil {
			log.Error("WhatsApp connection failed", "err", err)
			// If the client ended up in a permanently disconnected state (ban,
			// logged out, client outdated, etc.) write a cooldown marker so that
			// a supervisor restart does not immediately hammer the server again.
			if waClient.State() == client.StatePermanentlyDisconnected {
				writeCooldownMarker(cfg.DataDir)
				log.Error("permanent disconnect detected, cooldown marker written",
					"cooldown", cooldownDuration.String(),
				)
				return 2 // exit code 2: tell supervisor not to restart yet
			}
			return 1
		}
	case sig := <-sigCh:
		log.Info("received signal before connection established, shutting down", "signal", sig)
		return 0
	case err := <-srvErrCh:
		log.Error("HTTP server error before connection established", "err", err)
		return 1
	}

	// Successful connection: remove any stale cooldown marker.
	clearCooldownMarker(cfg.DataDir)
	log.Info("WhatsApp connection established")

	// ---- Background media-retry worker -------------------------------------
	// Re-download non-audio media that was requested on-demand and failed. It is
	// started only after the session is up (downloading needs the live whatsmeow
	// client) and is cancelled + awaited during graceful shutdown. When disabled
	// the goroutine never starts.
	if cfg.MediaRetryEnabled {
		mrCtx, cancel := context.WithCancel(context.Background())
		mediaRetryCancel = cancel
		worker := mediaretry.New(
			waClient, st,
			cfg.MediaRetryInterval, cfg.MediaMaxDownloadAttempts, cfg.MediaRetryBatchSize,
			log.With("component", "mediaretry"),
		)
		mediaRetryWG.Add(1)
		go func() {
			defer mediaRetryWG.Done()
			worker.Run(mrCtx)
		}()
	} else {
		log.Info("media-retry worker disabled")
	}

	// ---- Wait for termination signal ---------------------------------------
	select {
	case sig := <-sigCh:
		log.Info("received signal, initiating graceful shutdown", "signal", sig)
	case err := <-srvErrCh:
		log.Error("HTTP server error", "err", err)
	}

	return 0
}

// bridgeCleanup is populated as resources are acquired and deferred once.
// Thus every return (including connection failure/cooldown exit 2) stops and
// waits for the dispatcher before callback ingress or the store is closed.
type bridgeCleanup struct {
	stopHTTP      func(context.Context) error
	cancelConnect context.CancelFunc
	stopWorkers   func()
	disconnect    func()
	closeStore    func() error
	log           *slog.Logger
}

func (r *bridgeCleanup) run() {
	if r.cancelConnect != nil {
		r.cancelConnect()
	}
	if r.stopHTTP != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := r.stopHTTP(ctx); err != nil {
			r.log.Warn("HTTP shutdown incomplete", "class", "shutdown_failed")
		}
		cancel()
	}
	if r.stopWorkers != nil {
		r.stopWorkers()
	}
	if r.disconnect != nil {
		r.disconnect()
	}
	if r.closeStore != nil {
		if err := r.closeStore(); err != nil {
			r.log.Warn("store close failed", "class", "store_failed")
		}
	}
}
