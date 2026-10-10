package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
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
			r := bridgeCleanup{log: slog.New(slog.NewJSONHandler(&logs, nil)), closeStore: func() error { order = append(order, "store"); return errors.New("TEST_PRIVATE_ERROR") }}
			if tt.client {
				r.disconnect = func() { order = append(order, "client") }
			}
			if tt.http {
				r.cancelConnect = func() { order = append(order, "connect") }
				r.stopHTTP = func(context.Context) error { order = append(order, "http"); return errors.New("TEST_PRIVATE_ERROR") }
			}
			if tt.worker {
				r.stopWorkers = func() { order = append(order, "worker") }
			}
			func() { defer r.run() }()
			if !reflect.DeepEqual(order, tt.want) {
				t.Fatal("cleanup order unsafe")
			}
			if strings.Contains(logs.String(), "TEST_PRIVATE_ERROR") {
				t.Fatal("cleanup error leaked")
			}
		})
	}
}
