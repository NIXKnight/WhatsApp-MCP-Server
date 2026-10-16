package api

import (
	"testing"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/client"
)

type fakeLiveWhatsApp struct {
	connected bool
	loggedIn  bool
}

func (f *fakeLiveWhatsApp) IsConnected() bool { return f.connected }
func (f *fakeLiveWhatsApp) IsLoggedIn() bool  { return f.loggedIn }

func TestStatusResponseUsesIndependentLiveWhatsAppTruth(t *testing.T) {
	tests := []struct {
		name      string
		state     client.ConnectionState
		connected bool
		loggedIn  bool
		wantLive  bool
	}{
		{name: "connected state socket down", state: client.StateConnected, loggedIn: true},
		{name: "connected state logged out", state: client.StateConnected, connected: true},
		{name: "connected state both false", state: client.StateConnected},
		{name: "state lags live transport", state: client.StateConnecting, connected: true, loggedIn: true, wantLive: true},
		{name: "exact agreement", state: client.StateConnected, connected: true, loggedIn: true, wantLive: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			live := &fakeLiveWhatsApp{connected: tt.connected, loggedIn: tt.loggedIn}
			got := statusResponse(tt.state, live, 2*time.Second, 3, 4, time.Unix(1, 0))
			if got.State != tt.state.String() {
				t.Fatalf("state = %q, want %q", got.State, tt.state.String())
			}
			if got.IsConnected != tt.wantLive {
				t.Fatalf("is_connected = %v, want %v", got.IsConnected, tt.wantLive)
			}
		})
	}
}

func TestStatusNilClientFailsClosed(t *testing.T) {
	response := statusResponse(client.StateDisconnected, nil, 0, 0, 0, time.Unix(1, 0))
	if response.IsConnected || response.State != client.StateDisconnected.String() {
		t.Fatalf("nil client response = %#v", response)
	}
}
