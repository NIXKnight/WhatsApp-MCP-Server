package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Client wraps a whatsmeow.Client with the application's message store and
// connection state machine.
type Client struct {
	WA      *whatsmeow.Client
	Store   *Store
	conn    *Connection
	log     *slog.Logger
	dataDir string

	// keepAliveFailures counts consecutive KeepAliveTimeout events. It is
	// accessed only from the whatsmeow event handler goroutine so no mutex is
	// required.
	keepAliveFailures int
}

// NewClient initialises the whatsmeow device store, creates the whatsmeow
// client, attaches event handlers, and prepares (but does not start) the
// connection state machine.
func NewClient(dataDir string, store *Store, log *slog.Logger) (*Client, error) {
	waLogger := newWALogger(log)
	dbLog := newWALogger(log.With("component", "wastore"))

	deviceStorePath := filepath.Join(dataDir, "whatsapp.db")
	deviceDSN := fmt.Sprintf("file:%s?_foreign_keys=on", deviceStorePath)

	container, err := sqlstore.New("sqlite3", deviceDSN, dbLog)
	if err != nil {
		return nil, fmt.Errorf("open whatsapp device store: %w", err)
	}

	deviceStore, err := container.GetFirstDevice()
	if err != nil {
		if err == sql.ErrNoRows {
			deviceStore = container.NewDevice()
			log.Info("no existing device found, created new device")
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

	c := &Client{
		WA:      wa,
		Store:   store,
		log:     log,
		dataDir: dataDir,
	}

	// Wire event handlers before creating the connection manager so that
	// Connected/Disconnected events from the initial connect are handled.
	conn := newConnection(wa, log)
	c.conn = conn

	wa.AddEventHandler(c.handleEvent)

	return c, nil
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
	c.conn.Stop()
	c.WA.Disconnect()
}

// State returns the current connection state.
func (c *Client) State() ConnectionState {
	return c.conn.State()
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
