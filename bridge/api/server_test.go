package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSlogMiddlewareStatusRequestFiltering(t *testing.T) {
	tests := []struct {
		name              string
		logStatusRequests bool
		method            string
		target            string
		status            int
		recoverPanic      bool
		wantLog           bool
		wantPath          string
	}{
		{
			name: "disabled successful status with query", method: http.MethodGet,
			target: "/api/status?probe=TEST_QUERY_MARKER", status: http.StatusNoContent,
			wantPath: "/api/status",
		},
		{
			name: "disabled status redirect", method: http.MethodGet,
			target: "/api/status", status: http.StatusTemporaryRedirect,
			wantPath: "/api/status",
		},
		{
			name: "disabled status error", method: http.MethodGet,
			target: "/api/status?probe=TEST_QUERY_MARKER", status: http.StatusInternalServerError,
			wantLog: true, wantPath: "/api/status",
		},
		{
			name: "disabled recovered status panic", method: http.MethodGet,
			target: "/api/status", recoverPanic: true,
			wantLog: true, wantPath: "/api/status",
		},
		{
			name: "disabled non GET status", method: http.MethodPost,
			target: "/api/status", status: http.StatusNoContent,
			wantLog: true, wantPath: "/api/status",
		},
		{
			name: "disabled other endpoint", method: http.MethodGet,
			target: "/api/messages", status: http.StatusNoContent,
			wantLog: true, wantPath: "/api/messages",
		},
		{
			name: "disabled status lookalike", method: http.MethodGet,
			target: "/api/status/", status: http.StatusNotFound,
			wantLog: true, wantPath: "/api/status/",
		},
		{
			name: "enabled successful status", logStatusRequests: true, method: http.MethodGet,
			target: "/api/status", status: http.StatusNoContent,
			wantLog: true, wantPath: "/api/status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.recoverPanic {
					func() {
						defer func() {
							if recover() != nil {
								http.Error(w, "request failed", http.StatusInternalServerError)
							}
						}()
						panic("TEST_REQUEST_PANIC")
					}()
					return
				}
				w.WriteHeader(tt.status)
			})
			handler := newSlogMiddleware(logger, tt.logStatusRequests)(next)
			request := httptest.NewRequest(tt.method, tt.target, strings.NewReader("TEST_BODY_MARKER"))
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			if !tt.wantLog {
				if output.Len() != 0 {
					t.Fatal("successful status request was logged while disabled")
				}
				return
			}
			if output.Len() == 0 {
				t.Fatal("required HTTP access record was suppressed")
			}
			if strings.Contains(output.String(), "TEST_QUERY_MARKER") || strings.Contains(output.String(), "TEST_BODY_MARKER") {
				t.Fatal("HTTP access record included query or request payload")
			}
			var record struct {
				Message string `json:"msg"`
				Method  string `json:"method"`
				Path    string `json:"path"`
				Status  int    `json:"status"`
			}
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal("HTTP access record was not valid JSON")
			}
			if record.Message != "http" || record.Method != tt.method || record.Path != tt.wantPath || record.Status != recorder.Code {
				t.Fatal("HTTP access record did not preserve request metadata")
			}
		})
	}
}
