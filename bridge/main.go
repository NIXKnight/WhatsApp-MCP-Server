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
	"errors"
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

// cooldownDuration is the delay imposed after a QR timeout or permanent
// disconnect before the bridge may attempt a new connection.
const cooldownDuration = 10 * time.Minute

// BridgeHoldExit is the dedicated supervisor hold status for pairing,
// cooldown, QR-timeout, and permanent-disconnect dispositions.
const BridgeHoldExit = 78

var (
	errManagedUnpaired = errors.New("bridge_managed_unpaired")
	errBridgeCooldown  = errors.New("bridge_cooldown_active")
)

func cooldownMarkerPath(dataDir string) string {
	return filepath.Join(dataDir, "cooldown")
}

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

func writeCooldownMarker(dataDir string) {
	until := time.Now().Add(cooldownDuration)
	data := []byte(strconv.FormatInt(until.UnixMilli(), 10))
	if err := os.WriteFile(cooldownMarkerPath(dataDir), data, 0600); err != nil {
		slog.Error("cooldown_marker_write_failed")
	}
}

func clearCooldownMarker(dataDir string) {
	_ = os.Remove(cooldownMarkerPath(dataDir))
}

func managedPreflight(managed, paired bool) error {
	if managed && !paired {
		return errManagedUnpaired
	}
	return nil
}

func classifyBridgeExit(err error, signaled bool) int {
	if signaled {
		return 0
	}
	if errors.Is(err, errManagedUnpaired) ||
		errors.Is(err, errBridgeCooldown) ||
		errors.Is(err, client.ErrQRTimeout) ||
		errors.Is(err, client.ErrPermanentDisconnect) {
		return BridgeHoldExit
	}
	if err != nil {
		return 1
	}
	return 0
}

// finalizeBridgeExit is the sole cooldown-write policy. Only QR timeout and
// permanent loss create a marker, and the marker is written before returning
// the hold status.
func finalizeBridgeExit(err error, signaled bool, dataDir string, marker func(string)) int {
	code := classifyBridgeExit(err, signaled)
	if code == BridgeHoldExit &&
		(errors.Is(err, client.ErrQRTimeout) || errors.Is(err, client.ErrPermanentDisconnect)) {
		marker(dataDir)
	}
	return code
}

// waitForBridgeTerminal keeps server failure, signal, and permanent loss in a
// single testable wait boundary.
func waitForBridgeTerminal(signalCh <-chan os.Signal, serverCh <-chan error, permanentCh <-chan error) (error, bool) {
	select {
	case <-signalCh:
		return nil, true
	case err := <-serverCh:
		if err == nil {
			err = errors.New("bridge_server_event_invalid")
		}
		return err, false
	case err := <-permanentCh:
		if err == nil {
			err = errors.New("bridge_terminal_event_invalid")
		}
		return err, false
	}
}

func main() { os.Exit(run()) }

func run() int {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration error", "err", err)
		return 1
	}

	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel})
	log := slog.New(handler).With("service", "whatsapp-bridge-go")
	slog.SetDefault(log)

	// Construct the message store and device-store-backed client before any
	// startup output so managed pairing preflight can remain a sole-line hold.
	st, err := store.NewStore(cfg.DatabaseURL, log.With("component", "store"))
	if err != nil {
		if cfg.Managed {
			log.Error("bridge_store_init_failed")
		} else {
			log.Error("failed to open message store", "err", err)
		}
		return 1
	}

	managedUnpaired := false
	resources := bridgeCleanup{
		closeStore:          st.Close,
		log:                 log,
		managed:             cfg.Managed,
		suppressDiagnostics: &managedUnpaired,
	}
	defer resources.run()

	options := []client.Option{client.WithManagedMode(cfg.Managed)}
	if cfg.Notifications.Enabled {
		options = append(options, client.WithIncomingNotifications(cfg.Notifications.ChatAllowlist, cfg.Notifications.MaxAge))
	}
	waClient, err := client.NewClient(
		cfg.DatabaseURL,
		cfg.DataDir,
		st,
		log.With("component", "client"),
		options...,
	)
	if err != nil {
		if cfg.Managed {
			log.Error("bridge_client_init_failed")
		} else {
			log.Error("failed to create WhatsApp client", "err", err)
		}
		return 1
	}
	resources.disconnect = waClient.Disconnect

	if err := managedPreflight(cfg.Managed, waClient.IsPaired()); err != nil {
		managedUnpaired = true
		_, _ = fmt.Fprintln(os.Stderr, errManagedUnpaired.Error())
		return classifyBridgeExit(err, false)
	}

	// Pairing preflight deliberately precedes cooldown, all warning emission,
	// startup logs, listener/workers, and WhatsApp Connect.
	if until, err := checkCooldownMarker(cfg.DataDir); err == nil && until.After(time.Now()) {
		log.Error("bridge_cooldown_active")
		return classifyBridgeExit(errBridgeCooldown, false)
	}
	for _, warning := range cfg.Warnings {
		log.Warn(warning)
	}
	if warning := config.NonLoopbackWarning(cfg.Addr); warning != "" {
		log.Warn(warning)
	}
	log.Info("starting", "addr", cfg.Addr, "data_dir", cfg.DataDir)

	embedder := embed.New(cfg.EmbedderURL, cfg.EmbedderTimeout)
	mediaAnalyzer := analyzer.New(cfg.AnalyzerURL, cfg.AnalyzerTimeout)
	h := api.NewHandler(waClient, st, embedder, mediaAnalyzer, cfg.MediaMaxDownloadAttempts, log.With("component", "api"))
	srv := api.NewServer(cfg.Addr, h, log.With("component", "http"))
	resources.stopHTTP = srv.Shutdown

	srvErrCh := make(chan error, 1)
	go func() { srvErrCh <- srv.Start() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	connectCtx, connectCancel := context.WithCancel(context.Background())
	resources.cancelConnect = connectCancel

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
	go func() { connectErrCh <- waClient.Connect(connectCtx) }()

	select {
	case err = <-connectErrCh:
		if err != nil {
			if cfg.Managed {
				switch {
				case errors.Is(err, client.ErrQRTimeout):
					log.Error("qr_timeout")
				case errors.Is(err, client.ErrPermanentDisconnect):
					// The winning events callback already emitted the authoritative token.
				default:
					log.Error("bridge_connect_failed")
				}
			} else {
				log.Error("WhatsApp connection failed", "err", err)
			}
			return finalizeBridgeExit(err, false, cfg.DataDir, writeCooldownMarker)
		}
	case sig := <-sigCh:
		log.Info("received signal before connection established, shutting down", "signal", sig)
		return finalizeBridgeExit(nil, true, cfg.DataDir, writeCooldownMarker)
	case err = <-srvErrCh:
		if err == nil {
			err = errors.New("bridge_server_event_invalid")
		}
		if cfg.Managed {
			log.Error("bridge_http_failed")
		} else {
			log.Error("HTTP server error before connection established", "err", err)
		}
		return finalizeBridgeExit(err, false, cfg.DataDir, writeCooldownMarker)
	}

	clearCooldownMarker(cfg.DataDir)
	log.Info("WhatsApp connection established")

	if cfg.MediaRetryEnabled {
		mrCtx, cancel := context.WithCancel(context.Background())
		mediaRetryCancel = cancel
		worker := mediaretry.New(
			waClient,
			st,
			cfg.MediaRetryInterval,
			cfg.MediaMaxDownloadAttempts,
			cfg.MediaRetryBatchSize,
			log.With("component", "mediaretry"),
		)
		mediaRetryWG.Add(1)
		go func() { defer mediaRetryWG.Done(); worker.Run(mrCtx) }()
	} else {
		log.Info("media-retry worker disabled")
	}

	err, signaled := waitForBridgeTerminal(sigCh, srvErrCh, waClient.PermanentDisconnects())
	if signaled {
		log.Info("received signal, initiating graceful shutdown")
	} else if errors.Is(err, client.ErrPermanentDisconnect) {
		// The event layer owns the fixed managed diagnostic.
	} else if cfg.Managed {
		log.Error("bridge_http_failed")
	} else {
		log.Error("HTTP server error", "err", err)
	}
	return finalizeBridgeExit(err, signaled, cfg.DataDir, writeCooldownMarker)
}

// bridgeCleanup is populated as resources are acquired and deferred once.
type bridgeCleanup struct {
	stopHTTP            func(context.Context) error
	cancelConnect       context.CancelFunc
	stopWorkers         func()
	disconnect          func()
	closeStore          func() error
	log                 *slog.Logger
	managed             bool
	suppressDiagnostics *bool
}

func (r *bridgeCleanup) diagnosticsSuppressed() bool {
	return r.suppressDiagnostics != nil && *r.suppressDiagnostics
}

func (r *bridgeCleanup) run() {
	if r.cancelConnect != nil {
		r.cancelConnect()
	}
	if r.stopHTTP != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := r.stopHTTP(ctx); err != nil && !r.diagnosticsSuppressed() {
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
		if err := r.closeStore(); err != nil && !r.diagnosticsSuppressed() {
			if r.managed {
				r.log.Warn("bridge_store_close_failed")
			} else {
				r.log.Warn("store close failed", "class", "store_failed")
			}
		}
	}
}
