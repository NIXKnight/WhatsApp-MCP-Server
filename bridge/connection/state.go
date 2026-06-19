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

// State returns the current connection state.
func (c *Connection) State() ConnectionState {
	return ConnectionState(c.state.Load())
}

func (c *Connection) setState(s ConnectionState) {
	c.state.Store(int32(s))
	c.log.Info("connection state changed", "state", s.String())
}
