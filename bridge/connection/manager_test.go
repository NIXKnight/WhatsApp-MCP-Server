package connection

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waStore "go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestQRConnectSyntheticBranches(t *testing.T) {
	acquireErr := errors.New("synthetic acquire failure")
	connectErr := errors.New("synthetic connect failure")
	tests := []struct {
		name       string
		configure  func(*Connection) context.Context
		want       error
		disconnect int32
	}{
		{
			name: "acquire error",
			configure: func(c *Connection) context.Context {
				c.getQRChannel = func(context.Context) (<-chan whatsmeow.QRChannelItem, error) { return nil, acquireErr }
				return context.Background()
			},
			want: acquireErr,
		},
		{
			name: "connect error",
			configure: func(c *Connection) context.Context {
				c.getQRChannel = func(context.Context) (<-chan whatsmeow.QRChannelItem, error) {
					return make(chan whatsmeow.QRChannelItem), nil
				}
				c.connect = func() error { return connectErr }
				return context.Background()
			},
			want: connectErr,
		},
		{
			name: "timeout",
			configure: func(c *Connection) context.Context {
				ch := make(chan whatsmeow.QRChannelItem, 1)
				ch <- whatsmeow.QRChannelTimeout
				c.getQRChannel = func(context.Context) (<-chan whatsmeow.QRChannelItem, error) { return ch, nil }
				return context.Background()
			},
			want: ErrQRTimeout,
		},
		{
			name: "success",
			configure: func(c *Connection) context.Context {
				ch := make(chan whatsmeow.QRChannelItem, 1)
				ch <- whatsmeow.QRChannelSuccess
				c.retryCount.Store(4)
				c.getQRChannel = func(context.Context) (<-chan whatsmeow.QRChannelItem, error) { return ch, nil }
				return context.Background()
			},
		},
		{
			name: "caller cancellation",
			configure: func(c *Connection) context.Context {
				ch := make(chan whatsmeow.QRChannelItem)
				c.getQRChannel = func(context.Context) (<-chan whatsmeow.QRChannelItem, error) { return ch, nil }
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			want:       context.Canceled,
			disconnect: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestConnection(t, false)
			var disconnects atomic.Int32
			c.disconnect = func() { disconnects.Add(1) }
			ctx := tt.configure(c)
			err := c.qrConnect(ctx)
			if !errors.Is(err, tt.want) {
				t.Fatalf("qrConnect() error = %v, want %v", err, tt.want)
			}
			if got := disconnects.Load(); got != tt.disconnect {
				t.Fatalf("disconnect calls = %d, want %d", got, tt.disconnect)
			}
			if tt.name == "success" {
				if c.State() != StateConnected || c.retryCount.Load() != 0 {
					t.Fatal("QR success did not atomically commit connected effects")
				}
			}
		})
	}
}

func TestQRConnectPermanentAndOrdinaryStop(t *testing.T) {
	t.Run("ordinary stop", func(t *testing.T) {
		c := newTestConnection(t, false)
		c.getQRChannel = syntheticQRChannel(make(chan whatsmeow.QRChannelItem))
		c.Stop()
		if err := c.qrConnect(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("ordinary stop = %v", err)
		}
	})

	t.Run("permanent close", func(t *testing.T) {
		c := newTestConnection(t, true)
		ch := make(chan whatsmeow.QRChannelItem)
		close(ch)
		c.getQRChannel = syntheticQRChannel(ch)
		if !c.HandlePermanentDisconnect("") {
			t.Fatal("permanent transition did not win")
		}
		if err := c.qrConnect(context.Background()); !errors.Is(err, ErrPermanentDisconnect) {
			t.Fatalf("permanent QR close = %v", err)
		}
	})
}

func TestPermanentDisconnectExactlyOnceConcurrent(t *testing.T) {
	c := newTestConnection(t, true)
	const count = 32
	results := make(chan bool, count)
	var wg sync.WaitGroup
	wg.Add(count)
	for range count {
		go func() {
			defer wg.Done()
			results <- c.HandlePermanentDisconnect("")
		}()
	}
	wg.Wait()
	close(results)
	winners := 0
	for won := range results {
		if won {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("permanent winners = %d, want 1", winners)
	}
	if c.State() != StatePermanentlyDisconnected || !c.permanent.Load() {
		t.Fatal("permanent state was not committed")
	}
	if got := <-c.PermanentDisconnects(); got != ErrPermanentDisconnect {
		t.Fatalf("terminal event = %v", got)
	}
	select {
	case <-c.PermanentDisconnects():
		t.Fatal("duplicate terminal publication")
	default:
	}
}

func TestPermanentStateIsMonotonic(t *testing.T) {
	c := newTestConnection(t, true)
	c.retryCount.Store(7)
	if !c.HandlePermanentDisconnect("") {
		t.Fatal("permanent transition did not win")
	}
	connected := c.connectedCh
	if c.HandleConnected() {
		t.Fatal("late connected callback was accepted")
	}
	c.HandleDisconnected()
	c.HandleStreamError()
	if c.transitionState(StateConnecting) {
		t.Fatal("late reconnect transition was accepted")
	}
	if c.State() != StatePermanentlyDisconnected || c.retryCount.Load() != 7 {
		t.Fatal("late transition changed permanent state or retry count")
	}
	select {
	case <-connected:
		t.Fatal("late connected callback published success")
	default:
	}
}

func TestConnectedAndPermanentRaceIsLinearizable(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := newTestConnection(t, true)
		c.retryCount.Store(5)
		start := make(chan struct{})
		connectedResult := make(chan bool, 1)
		permanentResult := make(chan bool, 1)
		go func() { <-start; connectedResult <- c.HandleConnected() }()
		go func() { <-start; permanentResult <- c.HandlePermanentDisconnect("") }()
		close(start)
		connectedWon := <-connectedResult
		permanentWon := <-permanentResult
		if !permanentWon {
			t.Fatal("permanent transition must eventually win")
		}
		if c.State() != StatePermanentlyDisconnected {
			t.Fatal("permanent state was replaced")
		}
		if connectedWon && c.retryCount.Load() != 0 {
			t.Fatal("accepted connected transition exposed partial effects")
		}
		if !connectedWon && c.retryCount.Load() != 5 {
			t.Fatal("rejected connected transition changed retry count")
		}
	}
}

func syntheticQRChannel(ch <-chan whatsmeow.QRChannelItem) func(context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return func(context.Context) (<-chan whatsmeow.QRChannelItem, error) { return ch, nil }
}

func newTestConnection(t *testing.T, managed bool) *Connection {
	t.Helper()
	device := &waStore.Device{Log: waLog.Noop}
	wa := whatsmeow.NewClient(device, waLog.Noop)
	if wa == nil {
		t.Fatal("failed to construct inert WhatsMeow client")
	}
	c := NewConnection(wa, slog.New(slog.NewTextHandler(io.Discard, nil)), managed)
	c.connect = func() error { return nil }
	c.disconnect = func() {}
	return c
}

func TestPermanentPublicationDoesNotBlockWithoutConsumer(t *testing.T) {
	c := newTestConnection(t, true)
	done := make(chan struct{})
	go func() {
		c.HandlePermanentDisconnect("")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("permanent publication blocked")
	}
}
