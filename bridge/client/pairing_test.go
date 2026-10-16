package client

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"go.mau.fi/whatsmeow"
	waStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/connection"
)

func TestClientIsPairedNilSafe(t *testing.T) {
	jid := types.NewJID("100", types.DefaultUserServer)
	tests := []struct {
		name string
		c    *Client
		want bool
	}{
		{name: "nil client"},
		{name: "nil whatsapp", c: &Client{}},
		{name: "nil store", c: &Client{WA: &whatsmeow.Client{}}},
		{name: "nil id", c: &Client{WA: &whatsmeow.Client{Store: &waStore.Device{}}}},
		{name: "paired", c: &Client{WA: &whatsmeow.Client{Store: &waStore.Device{ID: &jid}}}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.IsPaired(); got != tt.want {
				t.Fatalf("IsPaired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPermanentDisconnectDelegationNilSafe(t *testing.T) {
	if (*Client)(nil).PermanentDisconnects() != nil || (&Client{}).PermanentDisconnects() != nil {
		t.Fatal("nil client or manager fabricated a permanent event channel")
	}
	c := newSyntheticClient(t, true, io.Discard)
	if c.PermanentDisconnects() == nil {
		t.Fatal("live manager channel was not delegated")
	}
	if !c.conn.HandlePermanentDisconnect("") {
		t.Fatal("first permanent transition did not win")
	}
	if got := <-c.PermanentDisconnects(); got != ErrPermanentDisconnect {
		t.Fatalf("permanent event = %v", got)
	}
	select {
	case <-c.PermanentDisconnects():
		t.Fatal("received a duplicate permanent event")
	default:
	}
}

func TestManagedDependencyLoggersDiscardFromSelection(t *testing.T) {
	const marker = "TEST_PRIVATE_DEPENDENCY_MARKER"
	var managed bytes.Buffer
	managedLogger := slog.New(slog.NewTextHandler(&managed, nil))
	waLogger, dbLogger := dependencyLoggers(managedLogger, true)
	waLogger.Errorf("migration %s", marker)
	dbLogger.Warnf("store %s", marker)
	waLogger.Sub("socket").Infof("whatsmeow %s", marker)
	if managed.Len() != 0 {
		t.Fatalf("managed dependency output was not discarded: %q", managed.String())
	}

	var foreground bytes.Buffer
	foregroundLogger := slog.New(slog.NewTextHandler(&foreground, nil))
	waLogger, dbLogger = dependencyLoggers(foregroundLogger, false)
	waLogger.Errorf("migration %s", marker)
	dbLogger.Warnf("store %s", marker)
	if strings.Count(foreground.String(), marker) != 2 {
		t.Fatalf("foreground dependency records changed: %q", foreground.String())
	}
}

func TestManagedPermanentEventIsFixedAndExactlyOnce(t *testing.T) {
	const marker = "TEST_PRIVATE_PERMANENT_DETAIL"
	var output bytes.Buffer
	c := newSyntheticClient(t, true, &output)
	event := &events.ConnectFailure{Reason: events.ConnectFailureGeneric, Message: marker}

	const callbacks = 16
	var wg sync.WaitGroup
	wg.Add(callbacks)
	for range callbacks {
		go func() {
			defer wg.Done()
			c.handleEvent(event)
		}()
	}
	wg.Wait()

	logs := output.String()
	if strings.Count(logs, `"msg":"permanent_disconnect"`) != 1 {
		t.Fatalf("managed permanent token count changed: %q", logs)
	}
	if strings.Contains(logs, marker) || strings.Contains(logs, event.Reason.String()) {
		t.Fatalf("managed permanent detail leaked: %q", logs)
	}
	if got := <-c.PermanentDisconnects(); got != ErrPermanentDisconnect {
		t.Fatalf("terminal event = %v", got)
	}
	select {
	case <-c.PermanentDisconnects():
		t.Fatal("duplicate terminal event")
	default:
	}
}

func TestForegroundPermanentDetailsPreserveOrdering(t *testing.T) {
	const marker = "TEST_FOREGROUND_PERMANENT_DETAIL"
	var output bytes.Buffer
	c := newSyntheticClient(t, false, &output)
	c.handleEvent(&events.ConnectFailure{Reason: events.ConnectFailureGeneric, Message: marker})
	logs := output.String()
	eventAt := strings.Index(logs, `"msg":"whatsapp: connect failure"`)
	managerAt := strings.Index(logs, `"msg":"permanent disconnect"`)
	stateAt := strings.Index(logs, `"msg":"connection state changed"`)
	if eventAt < 0 || managerAt <= eventAt || stateAt <= managerAt || !strings.Contains(logs, marker) {
		t.Fatalf("foreground permanent records changed order or detail: %q", logs)
	}
	if strings.Contains(logs, `"msg":"permanent_disconnect"`) {
		t.Fatalf("foreground event was replaced by managed token: %q", logs)
	}
}

func TestConnectedEventLogsOnlyAfterAtomicCommit(t *testing.T) {
	var output bytes.Buffer
	c := newSyntheticClient(t, false, &output)
	c.conn.HandlePermanentDisconnect("test terminal")
	output.Reset()
	c.handleEvent(&events.Connected{})
	if output.Len() != 0 || c.State() != StatePermanentlyDisconnected {
		t.Fatalf("late connected callback produced effects: %q", output.String())
	}
}

func newSyntheticClient(t *testing.T, managed bool, output io.Writer) *Client {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(output, nil))
	device := &waStore.Device{Log: waLog.Noop}
	wa := whatsmeow.NewClient(device, waLog.Noop)
	if wa == nil {
		t.Fatal("failed to construct inert WhatsMeow client")
	}
	return &Client{
		WA:        wa,
		conn:      connection.NewConnection(wa, logger, managed),
		log:       logger,
		managed:   managed,
		keepAlive: connection.NewKeepaliveTracker(maxKeepaliveFailures),
	}
}
