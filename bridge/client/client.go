// Package client implements L2 capture: it wraps a whatsmeow.Client with the
// application's message store and connection state machine, and translates
// whatsmeow events into store writes. It re-exports the connection-state
// constants and Client.State so that callers (the HTTP API and main) depend on
// this package rather than reaching into the connection package directly.
package client

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/connection"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// ConnectionState is re-exported from the connection package so callers can
// depend solely on the client package.
type ConnectionState = connection.ConnectionState

// Connection-state constants re-exported from the connection package.
const (
	StateDisconnected            = connection.StateDisconnected
	StateConnecting              = connection.StateConnecting
	StateQRWaiting               = connection.StateQRWaiting
	StateConnected               = connection.StateConnected
	StatePermanentlyDisconnected = connection.StatePermanentlyDisconnected
)

// Fixed lifecycle sentinels are aliases of the connection package values so
// errors.Is retains identity through this package boundary.
var (
	ErrQRTimeout           = connection.ErrQRTimeout
	ErrPermanentDisconnect = connection.ErrPermanentDisconnect
)

// maxKeepaliveFailures is the number of consecutive keep-alive timeouts after
// which the client forces a reconnect.
const maxKeepaliveFailures = 3

// Client wraps a whatsmeow.Client with the application's message store and
// connection state machine.
type Client struct {
	WA      *whatsmeow.Client
	Store   *store.Store
	conn    *connection.Connection
	log     *slog.Logger
	dataDir string
	managed bool
	// The handler registration is removed on terminal shutdown, draining ingress.
	eventHandlerID uint32
	stopOnce       sync.Once
	notifications  notificationPolicy

	// keepAlive tracks consecutive KeepAliveTimeout events. It is accessed only
	// from the whatsmeow event handler goroutine, but is concurrency-safe.
	keepAlive *connection.KeepaliveTracker
}

// NewClient initialises the whatsmeow device store, creates the whatsmeow
// client, attaches event handlers, and prepares (but does not start) the
// connection state machine.
func NewClient(databaseURL string, dataDir string, st *store.Store, log *slog.Logger, options ...Option) (*Client, error) {
	// Apply options before constructing any dependency so managed mode can select
	// discard-only dependency logging from the first operation onward.
	c := &Client{
		Store:     st,
		log:       log,
		dataDir:   dataDir,
		keepAlive: connection.NewKeepaliveTracker(maxKeepaliveFailures),
	}
	for _, option := range options {
		option(c)
	}

	waLogger, dbLog := dependencyLoggers(log, c.managed)
	container, err := sqlstore.New(context.Background(), "pgx", databaseURL, dbLog)
	if err != nil {
		return nil, fmt.Errorf("open whatsapp device store: %w", err)
	}

	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			deviceStore = container.NewDevice()
			if !c.managed {
				log.Info("no existing device found, created new device")
			}
		} else {
			return nil, fmt.Errorf("get first device: %w", err)
		}
	}

	wa := whatsmeow.NewClient(deviceStore, waLogger)
	if wa == nil {
		return nil, fmt.Errorf("whatsmeow.NewClient returned nil")
	}

	// Disable whatsmeow's internal keepalive-triggered auto-reconnect. The
	// bridge manages its own reconnect state machine (Connection.scheduleReconnect),
	// so having two concurrent reconnect loops is redundant and makes behaviour
	// non-deterministic. Our loop already handles KeepAliveTimeout events.
	wa.EnableAutoReconnect = false
	c.WA = wa
	c.conn = connection.NewConnection(wa, log, c.managed)
	c.eventHandlerID = wa.AddEventHandler(c.handleEvent)

	return c, nil
}

// WithManagedMode selects managed lifecycle behavior. False preserves the
// foreground constructor, logging, and QR behavior.
func WithManagedMode(managed bool) Option {
	return func(c *Client) { c.managed = managed }
}

func dependencyLoggers(log *slog.Logger, managed bool) (waLog.Logger, waLog.Logger) {
	if managed {
		return waLog.Noop, waLog.Noop
	}
	return newWALogger(log), newWALogger(log.With("component", "wastore"))
}

// Connect starts the WhatsApp connection. If no session exists, it displays
// a QR code for first-time pairing. It blocks until the session is
// established or the context is cancelled.
func (c *Client) Connect(ctx context.Context) error {
	return c.conn.Connect(ctx)
}

// Disconnect stops the reconnection state machine and then cleanly
// disconnects from WhatsApp servers. Always call Stop() before Disconnect()
// to prevent reconnect goroutines from racing the disconnect.
func (c *Client) Disconnect() {
	c.stopOnce.Do(func() {
		c.conn.Stop()
		c.WA.Disconnect()
		// whatsmeow's removal takes the dispatcher write lock, waiting for
		// active callbacks and excluding future ones. Never call from a callback.
		c.WA.RemoveEventHandler(c.eventHandlerID)
	})
}

// State returns the current connection state.
func (c *Client) State() ConnectionState {
	return c.conn.State()
}

// IsPaired reports authoritative pairing state from the WhatsMeow device ID.
func (c *Client) IsPaired() bool {
	return c != nil && c.WA != nil && c.WA.Store != nil && c.WA.Store.ID != nil
}

// PermanentDisconnects exposes the manager's one-shot fixed terminal event.
func (c *Client) PermanentDisconnects() <-chan error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.PermanentDisconnects()
}

// IsConnected reports the underlying live transport state.
func (c *Client) IsConnected() bool {
	return c != nil && c.WA != nil && c.WA.IsConnected()
}

// IsLoggedIn reports the underlying live login state.
func (c *Client) IsLoggedIn() bool {
	return c != nil && c.WA != nil && c.WA.IsLoggedIn()
}

// DataDir returns the configured data directory.
func (c *Client) DataDir() string {
	return c.dataDir
}

// waLogAdapter bridges slog to whatsmeow's waLog.Logger interface.
type waLogAdapter struct {
	log *slog.Logger
}

func newWALogger(log *slog.Logger) waLog.Logger {
	return &waLogAdapter{log: log}
}

func (a *waLogAdapter) Debugf(msg string, args ...interface{}) {
	a.log.Debug(fmt.Sprintf(msg, args...))
}

func (a *waLogAdapter) Infof(msg string, args ...interface{}) {
	a.log.Info(fmt.Sprintf(msg, args...))
}

func (a *waLogAdapter) Warnf(msg string, args ...interface{}) {
	a.log.Warn(fmt.Sprintf(msg, args...))
}

func (a *waLogAdapter) Errorf(msg string, args ...interface{}) {
	a.log.Error(fmt.Sprintf(msg, args...))
}

func (a *waLogAdapter) Sub(module string) waLog.Logger {
	return &waLogAdapter{log: a.log.With("module", module)}
}
