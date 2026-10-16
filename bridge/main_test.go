package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/client"
)

func TestCleanupEveryAcquiredResourceSubset(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		http, worker, client bool
		want                 []string
	}{
		{"client startup failure", false, false, false, []string{"store"}},
		{"sender startup failure", true, false, true, []string{"connect", "http", "client", "store"}},
		{"connection failure signal or HTTP exit", true, true, true, []string{"connect", "http", "worker", "client", "store"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var order []string
			var logs bytes.Buffer
			r := bridgeCleanup{
				log: slog.New(slog.NewJSONHandler(&logs, nil)),
				closeStore: func() error {
					order = append(order, "store")
					return errors.New("TEST_PRIVATE_ERROR")
				},
			}
			if tt.client {
				r.disconnect = func() { order = append(order, "client") }
			}
			if tt.http {
				r.cancelConnect = func() { order = append(order, "connect") }
				r.stopHTTP = func(context.Context) error {
					order = append(order, "http")
					return errors.New("TEST_PRIVATE_ERROR")
				}
			}
			if tt.worker {
				r.stopWorkers = func() { order = append(order, "worker") }
			}
			func() { defer r.run() }()
			if !reflect.DeepEqual(order, tt.want) {
				t.Fatalf("cleanup order = %v, want %v", order, tt.want)
			}
			if strings.Contains(logs.String(), "TEST_PRIVATE_ERROR") {
				t.Fatal("cleanup error leaked")
			}
		})
	}
}

func TestManagedPreflight(t *testing.T) {
	for _, tt := range []struct {
		name            string
		managed, paired bool
		wantErr         bool
	}{
		{name: "managed unpaired", managed: true, wantErr: true},
		{name: "managed paired", managed: true, paired: true},
		{name: "foreground unpaired"},
		{name: "foreground paired", paired: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := managedPreflight(tt.managed, tt.paired)
			if errors.Is(err, errManagedUnpaired) != tt.wantErr {
				t.Fatalf("managedPreflight() = %v", err)
			}
		})
	}
}

func TestClassifyBridgeExit(t *testing.T) {
	if BridgeHoldExit != 78 || BridgeHoldExit == 2 {
		t.Fatal("BridgeHoldExit must remain exact 78 and distinct from panic-like status 2")
	}
	tests := []struct {
		name     string
		err      error
		signaled bool
		want     int
	}{
		{name: "success", want: 0},
		{name: "signal overrides error", err: errors.New("ordinary"), signaled: true, want: 0},
		{name: "managed unpaired", err: errManagedUnpaired, want: BridgeHoldExit},
		{name: "cooldown", err: errBridgeCooldown, want: BridgeHoldExit},
		{name: "wrapped qr timeout", err: errors.Join(errors.New("wrapper"), client.ErrQRTimeout), want: BridgeHoldExit},
		{name: "wrapped permanent", err: errors.Join(errors.New("wrapper"), client.ErrPermanentDisconnect), want: BridgeHoldExit},
		{name: "context cancellation remains ordinary", err: context.Canceled, want: 1},
		{name: "ordinary", err: errors.New("ordinary"), want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyBridgeExit(tt.err, tt.signaled); got != tt.want {
				t.Fatalf("classifyBridgeExit() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFinalizeBridgeExitWritesCooldownExactlyOnceForOwnedFailures(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantCode  int
		wantCalls int
	}{
		{name: "QR timeout", err: client.ErrQRTimeout, wantCode: BridgeHoldExit, wantCalls: 1},
		{name: "permanent", err: client.ErrPermanentDisconnect, wantCode: BridgeHoldExit, wantCalls: 1},
		{name: "managed unpaired", err: errManagedUnpaired, wantCode: BridgeHoldExit},
		{name: "active cooldown", err: errBridgeCooldown, wantCode: BridgeHoldExit},
		{name: "server", err: errors.New("server"), wantCode: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			var gotDir string
			code := finalizeBridgeExit(tt.err, false, "TEST_DATA_DIR", func(dir string) {
				calls++
				gotDir = dir
			})
			if code != tt.wantCode || calls != tt.wantCalls {
				t.Fatalf("finalize = code %d calls %d", code, calls)
			}
			if calls == 1 && gotDir != "TEST_DATA_DIR" {
				t.Fatal("finalizer changed marker directory")
			}
		})
	}
}

func TestWaitForBridgeTerminal(t *testing.T) {
	t.Run("server error", func(t *testing.T) {
		server := make(chan error, 1)
		server <- errors.New("server")
		err, signaled := waitForBridgeTerminal(make(chan os.Signal), server, nil)
		if err == nil || signaled {
			t.Fatal("server error was not selected")
		}
	})
	t.Run("permanent", func(t *testing.T) {
		permanent := make(chan error, 1)
		permanent <- client.ErrPermanentDisconnect
		err, signaled := waitForBridgeTerminal(make(chan os.Signal), make(chan error), permanent)
		if !errors.Is(err, client.ErrPermanentDisconnect) || signaled {
			t.Fatal("permanent event was not selected")
		}
	})
	t.Run("signal", func(t *testing.T) {
		signals := make(chan os.Signal, 1)
		signals <- syscall.SIGTERM
		err, signaled := waitForBridgeTerminal(signals, make(chan error), nil)
		if err != nil || !signaled {
			t.Fatal("signal was not selected")
		}
	})
}

func TestManagedUnpairedCleanupSuppressesDiagnosticsButStillRuns(t *testing.T) {
	var logs bytes.Buffer
	suppress := true
	closed := 0
	r := bridgeCleanup{
		log:                 slog.New(slog.NewJSONHandler(&logs, nil)),
		managed:             true,
		suppressDiagnostics: &suppress,
		closeStore: func() error {
			closed++
			return errors.New("TEST_PRIVATE_CLOSE_MARKER")
		},
	}
	r.run()
	if closed != 1 {
		t.Fatalf("store close calls = %d", closed)
	}
	if logs.Len() != 0 {
		t.Fatalf("suppressed cleanup emitted %q", logs.String())
	}
}

func TestManagedCleanupUsesFixedStoreClass(t *testing.T) {
	var logs bytes.Buffer
	r := bridgeCleanup{
		log:     slog.New(slog.NewJSONHandler(&logs, nil)),
		managed: true,
		closeStore: func() error {
			return errors.New("TEST_PRIVATE_CLOSE_MARKER")
		},
	}
	r.run()
	got := logs.String()
	if !strings.Contains(got, `"msg":"bridge_store_close_failed"`) || strings.Contains(got, "TEST_PRIVATE_CLOSE_MARKER") {
		t.Fatalf("managed cleanup log is not fixed: %q", got)
	}
}
