package connection

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mdp/qrterminal"
	"go.mau.fi/whatsmeow"
)

// connectTimeout is how long reconnect() waits for events.Connected before
// returning an error.
const connectTimeout = 30 * time.Second

// backoffDurations defines the exponential back-off schedule. The cap is
// reset after it is hit so the sequence repeats.
var backoffDurations = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	32 * time.Second,
	60 * time.Second,
}

// Connection manages the WhatsApp connection lifecycle including the QR flow,
// automatic reconnection with exponential back-off, and a generation counter
// to guard against stale reconnect loops.
type Connection struct {
	wa         *whatsmeow.Client
	log        *slog.Logger
	state      atomic.Int32
	generation atomic.Int64 // incremented on each connect attempt
	retryCount atomic.Int32
	stopCh     chan struct{} // closed when a permanent disconnect is requested
	stopOnce   sync.Once

	// connectedMu guards connectedCh: a new channel is created on each
	// reconnect attempt and closed when events.Connected fires.
	connectedMu sync.Mutex
	connectedCh chan struct{}
}

// NewConnection creates a Connection bound to the given whatsmeow client.
func NewConnection(wa *whatsmeow.Client, log *slog.Logger) *Connection {
	return &Connection{
		wa:          wa,
		log:         log,
		stopCh:      make(chan struct{}),
		connectedCh: make(chan struct{}),
	}
}

// resetConnectedCh creates a fresh (unclosed) connectedCh for the next
// connect attempt and returns it.
func (c *Connection) resetConnectedCh() chan struct{} {
	c.connectedMu.Lock()
	defer c.connectedMu.Unlock()
	ch := make(chan struct{})
	c.connectedCh = ch
	return ch
}

// Connect performs the initial connection. If the device has no ID yet,
// it enters the QR flow. The call blocks until connected, permanently
// disconnected, or ctx is cancelled.
func (c *Connection) Connect(ctx context.Context) error {
	c.setState(StateConnecting)
	c.generation.Add(1)

	if c.wa.Store.ID == nil {
		return c.qrConnect(ctx)
	}
	return c.reconnect(ctx)
}

// qrConnect starts the QR code pairing flow and blocks until the user scans
// the code or ctx is cancelled.
func (c *Connection) qrConnect(ctx context.Context) error {
	c.setState(StateQRWaiting)

	qrChan, err := c.wa.GetQRChannel(ctx)
	if err != nil {
		c.setState(StateDisconnected)
		return err
	}

	if err := c.wa.Connect(); err != nil {
		c.setState(StateDisconnected)
		return err
	}

	for {
		select {
		case <-ctx.Done():
			c.wa.Disconnect()
			c.setState(StateDisconnected)
			return ctx.Err()
		case evt, ok := <-qrChan:
			if !ok {
				return nil
			}
			switch evt.Event {
			case "code":
				c.log.Info("scan QR code to pair your device")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			case "success":
				c.log.Info("QR code scanned successfully")
				c.setState(StateConnected)
				return nil
			case "timeout":
				c.setState(StateDisconnected)
				return qrConnect_retryError("QR code scan timed out")
			case "error":
				c.setState(StateDisconnected)
				return fmt.Errorf("QR error event: %w", evt.Error)
			}
		}
	}
}

type qrConnect_retryError string

func (e qrConnect_retryError) Error() string { return string(e) }

// reconnect connects an already-paired device and blocks until events.Connected
// fires (up to connectTimeout) or the context is cancelled.
func (c *Connection) reconnect(ctx context.Context) error {
	if c.State() == StatePermanentlyDisconnected {
		return fmt.Errorf("permanently disconnected")
	}

	connCh := c.resetConnectedCh()

	if err := c.wa.Connect(); err != nil {
		c.setState(StateDisconnected)
		return err
	}

	// Block until the Connected event arrives, timeout, stop, or ctx cancel.
	timeout := time.NewTimer(connectTimeout)
	defer timeout.Stop()

	select {
	case <-connCh:
		// HandleConnected already called setState(StateConnected).
		return nil
	case <-timeout.C:
		c.setState(StateDisconnected)
		return context.DeadlineExceeded
	case <-c.stopCh:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HandleConnected is called from the event handler when events.Connected fires.
func (c *Connection) HandleConnected() {
	c.retryCount.Store(0)
	c.setState(StateConnected)

	// Signal any goroutine waiting in reconnect().
	c.connectedMu.Lock()
	ch := c.connectedCh
	c.connectedMu.Unlock()

	// Close is idempotent via select; use non-blocking close to avoid panic if
	// already closed (e.g. HandleConnected called twice).
	select {
	case <-ch:
		// Already closed.
	default:
		close(ch)
	}
}

// HandleDisconnected is called from the event handler when events.Disconnected
// fires. whatsmeow's Disconnected event is a transient websocket close;
// whatsmeow will attempt its own reconnect, but we also schedule our own
// reconnect to ensure state recovery.
func (c *Connection) HandleDisconnected() {
	c.setState(StateDisconnected)
	c.scheduleReconnect()
}

// HandleLoggedOut is called when events.LoggedOut fires.
func (c *Connection) HandleLoggedOut() {
	c.HandlePermanentDisconnect("logged out")
}

// HandlePermanentDisconnect transitions to StatePermanentlyDisconnected and
// cancels all pending reconnect goroutines by closing stopCh. Call this for
// any condition that must not trigger automatic reconnection (bans, stream
// replacement, outdated client, etc.).
func (c *Connection) HandlePermanentDisconnect(reason string) {
	c.log.Error("permanent disconnect", "reason", reason)
	c.setState(StatePermanentlyDisconnected)
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}

// HandleStreamError is called when events.StreamError fires.
func (c *Connection) HandleStreamError() {
	c.log.Warn("stream error, scheduling reconnect")
	c.setState(StateDisconnected)
	c.scheduleReconnect()
}

// scheduleReconnect waits for the appropriate back-off duration, then
// re-invokes reconnect. A generation counter guards against cascades: if the
// generation has changed while we were sleeping, a different goroutine has
// already started reconnecting and we abort.
func (c *Connection) scheduleReconnect() {
	gen := c.generation.Add(1)

	idx := int(c.retryCount.Load())
	if idx >= len(backoffDurations) {
		c.retryCount.Store(0)
		idx = 0
	}
	delay := backoffDurations[idx]
	attempt := c.retryCount.Add(1)

	c.log.Info("reconnect scheduled", "delay", delay, "attempt", attempt)

	go func() {
		select {
		case <-time.After(delay):
		case <-c.stopCh:
			return
		}

		// Abort if another reconnect has started.
		if c.generation.Load() != gen {
			return
		}

		if c.State() == StatePermanentlyDisconnected {
			return
		}

		c.setState(StateConnecting)
		// Use a background context for scheduled reconnects; Stop() cancels via stopCh.
		if err := c.reconnect(context.Background()); err != nil {
			c.log.Warn("reconnect failed", "err", err)
			// Do not overwrite a permanent disconnect state that was set
			// concurrently (e.g. by HandlePermanentDisconnect or HandleLoggedOut).
			if c.State() != StatePermanentlyDisconnected {
				c.setState(StateDisconnected)
				c.scheduleReconnect()
			}
		}
	}()
}

// Stop cancels all pending reconnect goroutines and must be called before
// wa.Disconnect() to avoid a reconnect racing with the disconnect.
func (c *Connection) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}
