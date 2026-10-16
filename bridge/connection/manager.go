package connection

import (
	"context"
	"errors"
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

// Fixed lifecycle sentinels returned to the process-level supervisor policy.
var (
	ErrQRTimeout           = errors.New("qr_timeout")
	ErrPermanentDisconnect = errors.New("permanent_disconnect")
)

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
	stopCh     chan struct{} // closed when shutdown or permanent loss is requested
	stopOnce   sync.Once

	// connectedMu guards connectedCh. When nested, transitionMu is always outer.
	connectedMu sync.Mutex
	connectedCh chan struct{}

	permanentDisconnect chan error
	permanentOnce       sync.Once
	transitionMu        sync.Mutex
	permanent           atomic.Bool

	// These instance seams are bound once in production. Tests may replace them
	// before qrConnect starts.
	getQRChannel func(context.Context) (<-chan whatsmeow.QRChannelItem, error)
	connect      func() error
	disconnect   func()
	managed      bool
}

// NewConnection creates a Connection bound to the given whatsmeow client.
// Omitting managed preserves foreground behavior for existing callers.
func NewConnection(wa *whatsmeow.Client, log *slog.Logger, managedMode ...bool) *Connection {
	managed := len(managedMode) > 0 && managedMode[0]
	return &Connection{
		wa:                  wa,
		log:                 log,
		stopCh:              make(chan struct{}),
		connectedCh:         make(chan struct{}),
		permanentDisconnect: make(chan error, 1),
		getQRChannel:        wa.GetQRChannel,
		connect:             wa.Connect,
		disconnect:          wa.Disconnect,
		managed:             managed,
	}
}

// PermanentDisconnects returns the live Connection's one-shot terminal event.
func (c *Connection) PermanentDisconnects() <-chan error {
	if c == nil {
		return nil
	}
	return c.permanentDisconnect
}

func (c *Connection) publishPermanentDisconnect() {
	if c == nil {
		return
	}
	c.permanentOnce.Do(func() {
		select {
		case c.permanentDisconnect <- ErrPermanentDisconnect:
		default:
		}
	})
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
	if !c.transitionState(StateConnecting) {
		return ErrPermanentDisconnect
	}
	c.generation.Add(1)

	if c.wa.Store.ID == nil {
		return c.qrConnect(ctx)
	}
	return c.reconnect(ctx)
}

// qrConnect starts the QR code pairing flow and blocks until the user scans
// the code or ctx is cancelled.
func (c *Connection) qrConnect(ctx context.Context) error {
	if !c.transitionState(StateQRWaiting) {
		return ErrPermanentDisconnect
	}

	qrChan, err := c.getQRChannel(ctx)
	if err != nil {
		c.transitionState(StateDisconnected)
		return err
	}

	if err := c.connect(); err != nil {
		c.transitionState(StateDisconnected)
		return err
	}

	for {
		select {
		case <-ctx.Done():
			c.disconnect()
			c.transitionState(StateDisconnected)
			return ctx.Err()
		case <-c.stopCh:
			if c.permanent.Load() {
				return ErrPermanentDisconnect
			}
			c.transitionState(StateDisconnected)
			return context.Canceled
		case evt, ok := <-qrChan:
			if !ok {
				if c.permanent.Load() {
					return ErrPermanentDisconnect
				}
				select {
				case err := <-c.PermanentDisconnects():
					return err
				default:
					return nil
				}
			}
			switch evt.Event {
			case whatsmeow.QRChannelEventCode:
				c.log.Info("scan QR code to pair your device")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			case whatsmeow.QRChannelSuccess.Event:
				if !c.markConnected() {
					return ErrPermanentDisconnect
				}
				c.log.Info("QR code scanned successfully")
				return nil
			case whatsmeow.QRChannelTimeout.Event:
				if !c.transitionState(StateDisconnected) && c.permanent.Load() {
					return ErrPermanentDisconnect
				}
				return ErrQRTimeout
			case whatsmeow.QRChannelEventError:
				if !c.transitionState(StateDisconnected) && c.permanent.Load() {
					return ErrPermanentDisconnect
				}
				return fmt.Errorf("QR error event: %w", evt.Error)
			}
		}
	}
}

// reconnect connects an already-paired device and blocks until events.Connected
// fires (up to connectTimeout) or the context is cancelled.
func (c *Connection) reconnect(ctx context.Context) error {
	if c.permanent.Load() {
		return ErrPermanentDisconnect
	}

	connCh := c.resetConnectedCh()

	if err := c.wa.Connect(); err != nil {
		c.transitionState(StateDisconnected)
		return err
	}

	timeout := time.NewTimer(connectTimeout)
	defer timeout.Stop()

	select {
	case <-connCh:
		if c.permanent.Load() {
			return ErrPermanentDisconnect
		}
		return nil
	case <-timeout.C:
		if !c.transitionState(StateDisconnected) && c.permanent.Load() {
			return ErrPermanentDisconnect
		}
		return context.DeadlineExceeded
	case <-c.stopCh:
		if c.permanent.Load() {
			return ErrPermanentDisconnect
		}
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HandleConnected atomically commits all connected effects unless permanent
// loss has already won.
func (c *Connection) HandleConnected() bool {
	return c.markConnected()
}

// HandleDisconnected handles a transient websocket close.
func (c *Connection) HandleDisconnected() {
	if c.transitionState(StateDisconnected) {
		c.scheduleReconnect()
	}
}

// HandleLoggedOut handles terminal logout.
func (c *Connection) HandleLoggedOut() bool {
	return c.HandlePermanentDisconnect("logged out")
}

// HandlePermanentDisconnect transitions once, stops reconnects, and publishes
// one fixed terminal event. It returns true only to the transition winner.
func (c *Connection) HandlePermanentDisconnect(reason string) bool {
	if !c.managed {
		// Foreground keeps the existing detailed record before the state-change
		// record, including for repeated event callbacks.
		c.log.Error("permanent disconnect", "reason", reason)
	}
	if !c.transitionState(StatePermanentlyDisconnected) {
		return false
	}
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.publishPermanentDisconnect()
	return true
}

// HandleStreamError schedules recovery from a transient stream failure.
func (c *Connection) HandleStreamError() {
	c.log.Warn("stream error, scheduling reconnect")
	if c.transitionState(StateDisconnected) {
		c.scheduleReconnect()
	}
}

// scheduleReconnect waits for the appropriate back-off duration, then
// re-invokes reconnect. A generation counter guards against cascades.
func (c *Connection) scheduleReconnect() {
	if c.permanent.Load() {
		return
	}
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

		if c.generation.Load() != gen || c.permanent.Load() {
			return
		}
		if !c.transitionState(StateConnecting) {
			return
		}
		if err := c.reconnect(context.Background()); err != nil {
			if c.permanent.Load() || errors.Is(err, ErrPermanentDisconnect) {
				return
			}
			c.log.Warn("reconnect failed", "err", err)
			if c.transitionState(StateDisconnected) {
				c.scheduleReconnect()
			}
		}
	}()
}

// Stop cancels all pending reconnect goroutines and must be called before
// wa.Disconnect() to avoid a reconnect racing with the disconnect.
func (c *Connection) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}
