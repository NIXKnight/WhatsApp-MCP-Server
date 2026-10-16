// Package connection implements the L2 transport lifecycle: the QR pairing
// flow, automatic reconnection with exponential back-off, connection state
// tracking, and keepalive failure accounting.
package connection

// ConnectionState represents the state of the WhatsApp connection.
type ConnectionState int32

const (
	StateDisconnected            ConnectionState = iota
	StateConnecting                              // attempting to connect
	StateQRWaiting                               // waiting for QR scan
	StateConnected                               // fully connected
	StatePermanentlyDisconnected                 // logged out, no reconnect
)

// String returns a human-readable name for the state.
func (s ConnectionState) String() string {
	switch s {
	case StateDisconnected:
		return "DISCONNECTED"
	case StateConnecting:
		return "CONNECTING"
	case StateQRWaiting:
		return "QR_WAITING"
	case StateConnected:
		return "CONNECTED"
	case StatePermanentlyDisconnected:
		return "PERMANENTLY_DISCONNECTED"
	default:
		return "UNKNOWN"
	}
}

// State returns the current connection state under the transition guard.
func (c *Connection) State() ConnectionState {
	c.transitionMu.Lock()
	defer c.transitionMu.Unlock()
	return ConnectionState(c.state.Load())
}

// transitionStateLocked is the sole connection-state write/log primitive.
// The caller must hold transitionMu.
func (c *Connection) transitionStateLocked(next ConnectionState) bool {
	if next == StatePermanentlyDisconnected {
		if c.permanent.Load() {
			return false
		}
		c.permanent.Store(true)
	} else if c.permanent.Load() {
		return false
	}
	c.state.Store(int32(next))
	c.log.Info("connection state changed", "state", next.String())
	return true
}

// transitionState serializes every ordinary/permanent transition.
func (c *Connection) transitionState(next ConnectionState) bool {
	c.transitionMu.Lock()
	defer c.transitionMu.Unlock()
	return c.transitionStateLocked(next)
}

// markConnected linearizes connected state, retry reset, and channel
// publication against permanent loss. The sole nested order is
// transitionMu -> connectedMu; unlock is the reverse.
func (c *Connection) markConnected() bool {
	c.transitionMu.Lock()
	defer c.transitionMu.Unlock()
	if !c.transitionStateLocked(StateConnected) {
		return false
	}
	c.retryCount.Store(0)
	c.connectedMu.Lock()
	defer c.connectedMu.Unlock()
	connected := c.connectedCh
	if connected != nil {
		select {
		case <-connected:
		default:
			close(connected)
		}
	}
	return true
}
