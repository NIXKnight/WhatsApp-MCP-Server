package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

const (
	testExactMessageID   = "TEST_SLICE2/context ?#%"
	testPercentMessageID = "TEST_SLICE2%2F_LITERAL"
	testOtherMessageID   = "TEST_SLICE2_OTHER_MESSAGE"
	testExactChatJID     = "910000000001@g.us"
	testOtherChatJID     = "910000000002@g.us"
)

var testExactTimestamp = time.Date(2100, 10, 12, 12, 0, 0, 0, time.UTC)

func testAPILogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testAPIHandler(messageStore *store.Store) http.Handler {
	logger := testAPILogger()
	handler := NewHandler(nil, messageStore, nil, nil, 3, logger)
	return NewServer("127.0.0.1:0", handler, logger).srv.Handler
}

func performRequest(handler http.Handler, target string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
	return recorder
}

func requireErrorResponse(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	status int,
	want ErrorResponse,
) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("unexpected HTTP status: got %d, want %d", recorder.Code, status)
	}
	var got ErrorResponse
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatal("failed to decode error envelope")
	}
	if got != want {
		t.Fatalf("unexpected error envelope: got code %q, want %q", got.Code, want.Code)
	}
}

func TestMessageRoutesRegisteredWithoutCollision(t *testing.T) {
	logger := testAPILogger()
	server := NewServer(
		"127.0.0.1:0",
		NewHandler(nil, nil, nil, nil, 3, logger),
		logger,
	)
	router, ok := server.srv.Handler.(chi.Routes)
	if !ok {
		t.Fatal("server handler does not expose routes")
	}

	registered := make(map[string]bool)
	if err := chi.Walk(
		router,
		func(
			method string,
			route string,
			_ http.Handler,
			_ ...func(http.Handler) http.Handler,
		) error {
			registered[method+" "+route] = true
			return nil
		},
	); err != nil {
		t.Fatal("failed to walk API routes")
	}
	for _, route := range []string{
		"GET /api/messages/{id}",
		"GET /api/messages/{id}/context",
		"GET /api/messages/{id}/similar",
	} {
		if !registered[route] {
			t.Fatalf("required route is not registered: %s", route)
		}
	}
}

func TestMessageRouteValidationWithoutPostgres(t *testing.T) {
	handler := testAPIHandler(nil)
	tests := []struct {
		name   string
		target string
		status int
		want   ErrorResponse
	}{
		{
			name:   "exact message requires chat",
			target: "/api/messages/TEST_MESSAGE",
			status: http.StatusBadRequest,
			want: ErrorResponse{
				Error: "chat_jid query parameter required",
				Code:  "MISSING_PARAM",
			},
		},
		{
			name:   "exact message rejects invalid chat",
			target: "/api/messages/TEST_MESSAGE?chat_jid=not-a-jid",
			status: http.StatusBadRequest,
			want: ErrorResponse{
				Error: "invalid chat_jid format",
				Code:  "INVALID_JID",
			},
		},
		{
			name:   "context route remains distinct",
			target: "/api/messages/TEST_MESSAGE/context",
			status: http.StatusBadRequest,
			want: ErrorResponse{
				Error: "chat_jid query parameter required",
				Code:  "MISSING_PARAM",
			},
		},
		{
			name:   "similar route remains distinct",
			target: "/api/messages/TEST_MESSAGE/similar",
			status: http.StatusBadRequest,
			want: ErrorResponse{
				Error: "chat_jid query parameter required",
				Code:  "MISSING_PARAM",
			},
		},
		{
			name: "check validates jid rather than chat_jid",
			target: "/api/check?jid=not-a-jid&chat_jid=" +
				url.QueryEscape(testExactChatJID),
			status: http.StatusBadRequest,
			want: ErrorResponse{
				Error: "invalid jid format",
				Code:  "INVALID_JID",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(handler, tt.target)
			requireErrorResponse(t, recorder, tt.status, tt.want)
		})
	}
}

func TestGetMessageRejectsInvalidPathEscapeWithFixedError(t *testing.T) {
	handler := NewHandler(nil, nil, nil, nil, 3, testAPILogger())
	request := httptest.NewRequest(
		http.MethodGet,
		"/?chat_jid="+url.QueryEscape(testExactChatJID),
		nil,
	)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("id", "%")
	request.URL.RawPath = "/api/messages/%"
	request = request.WithContext(
		context.WithValue(request.Context(), chi.RouteCtxKey, routeContext),
	)

	recorder := httptest.NewRecorder()
	handler.GetMessage(recorder, request)
	requireErrorResponse(
		t,
		recorder,
		http.StatusBadRequest,
		ErrorResponse{Error: "invalid message ID", Code: "INVALID_MESSAGE_ID"},
	)
}

func openMessageTestDatabase(t *testing.T) (*store.Store, *sql.DB) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal("TEST_DATABASE_URL is invalid")
	}
	if !strings.Contains(strings.ToLower(config.Database), "test") {
		t.Fatal("TEST_DATABASE_URL database name must contain test")
	}

	database := stdlib.OpenDB(*config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		t.Fatal("failed to connect to disposable test database")
	}
	if _, err := database.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS vector"); err != nil {
		_ = database.Close()
		t.Fatal("failed to initialize disposable test database")
	}

	messageStore, err := store.NewStore(databaseURL, testAPILogger())
	if err != nil {
		_ = database.Close()
		t.Fatal("failed to initialize disposable message store")
	}

	// LIFO test cleanup closes the store before the independent fixture handle.
	// Fixture cleanup registered by seedMessageFixtures runs before both closes.
	t.Cleanup(func() { _ = database.Close() })
	t.Cleanup(func() { _ = messageStore.Close() })
	return messageStore, database
}

func execFixtureSQL(t *testing.T, database *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := database.ExecContext(ctx, query, args...); err != nil {
		t.Fatal("failed to prepare synthetic database fixture")
	}
}

func cleanupMessageFixtures(t *testing.T, database *sql.DB) {
	t.Helper()
	execFixtureSQL(
		t,
		database,
		`DELETE FROM messages WHERE
		 (id = $1 AND chat_jid = $2) OR
		 (id = $3 AND chat_jid = $5) OR
		 (id = $4 AND chat_jid = $5)`,
		testExactMessageID,
		testExactChatJID,
		testPercentMessageID,
		testOtherMessageID,
		testOtherChatJID,
	)
	execFixtureSQL(
		t,
		database,
		`DELETE FROM chats WHERE jid = $1 OR jid = $2`,
		testExactChatJID,
		testOtherChatJID,
	)
}

func seedMessageFixtures(t *testing.T, database *sql.DB) MessageResponse {
	t.Helper()
	cleanupMessageFixtures(t, database)
	t.Cleanup(func() { cleanupMessageFixtures(t, database) })

	execFixtureSQL(
		t,
		database,
		`INSERT INTO chats (jid, name, is_group)
		 VALUES ($1, $2, TRUE), ($3, $4, TRUE)`,
		testExactChatJID,
		"TEST_SLICE2_CHAT_ONE",
		testOtherChatJID,
		"TEST_SLICE2_CHAT_TWO",
	)
	execFixtureSQL(
		t,
		database,
		`INSERT INTO messages
		 (id, chat_jid, sender, sender_name, content, timestamp, is_from_me,
		  media_type, filename, url, file_length, push_name,
		  quoted_message_id, quoted_participant)
		 VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7, $8, $9, $10, $11, $12, $13)`,
		testExactMessageID,
		testExactChatJID,
		"910000000003@s.whatsapp.net",
		"TEST_SLICE2_SENDER",
		"TEST_SLICE2_CONTENT",
		testExactTimestamp,
		"audio",
		"TEST_SLICE2_FILE",
		"https://example.test/TEST_SLICE2_MEDIA",
		int64(42),
		"TEST_SLICE2_PUSH",
		"TEST_SLICE2_QUOTED",
		"910000000004@s.whatsapp.net",
	)
	transcribedAt := testExactTimestamp.Add(time.Minute)
	execFixtureSQL(
		t,
		database,
		`INSERT INTO messages_media
		 (message_id, chat_jid, media_type, transcription, transcribed_at)
		 VALUES ($1, $2, 'audio', $3, $4)`,
		testExactMessageID,
		testExactChatJID,
		"TEST_SLICE2_TRANSCRIPTION",
		transcribedAt,
	)
	execFixtureSQL(
		t,
		database,
		`INSERT INTO messages
		 (id, chat_jid, sender, sender_name, content, timestamp, is_from_me)
		 VALUES ($1, $2, $3, $4, $5, $6, FALSE),
		        ($7, $2, $3, $4, $8, $9, FALSE)`,
		testOtherMessageID,
		testOtherChatJID,
		"910000000005@s.whatsapp.net",
		"TEST_SLICE2_OTHER_SENDER",
		"TEST_SLICE2_OTHER_CONTENT",
		testExactTimestamp.Add(time.Second),
		testPercentMessageID,
		"TEST_SLICE2_PERCENT_CONTENT",
		testExactTimestamp.Add(-2*time.Second),
	)

	return MessageResponse{
		ID:                testExactMessageID,
		ChatJID:           testExactChatJID,
		Sender:            "910000000003@s.whatsapp.net",
		SenderName:        "TEST_SLICE2_SENDER",
		Content:           "TEST_SLICE2_CONTENT",
		Timestamp:         testExactTimestamp,
		IsFromMe:          true,
		MediaType:         "audio",
		Filename:          "TEST_SLICE2_FILE",
		URL:               "https://example.test/TEST_SLICE2_MEDIA",
		FileLength:        42,
		PushName:          "TEST_SLICE2_PUSH",
		QuotedMessageID:   "TEST_SLICE2_QUOTED",
		QuotedParticipant: "910000000004@s.whatsapp.net",
		Transcription:     "TEST_SLICE2_TRANSCRIPTION",
		TranscribedAt:     &transcribedAt,
	}
}

func TestGetMessagePostgres(t *testing.T) {
	messageStore, database := openMessageTestDatabase(t)
	want := seedMessageFixtures(t, database)
	handler := testAPIHandler(messageStore)

	t.Run("encoded reserved ID remains on exact route", func(t *testing.T) {
		target := "/api/messages/" + url.PathEscape(testExactMessageID) +
			"?chat_jid=" + url.QueryEscape(testExactChatJID)
		recorder := performRequest(handler, target)
		if recorder.Code != http.StatusOK {
			t.Fatalf(
				"unexpected HTTP status: got %d, want %d",
				recorder.Code,
				http.StatusOK,
			)
		}

		body := recorder.Body.Bytes()
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Fatal("failed to decode exact-message envelope")
		}
		if len(fields) != 1 || fields["message"] == nil {
			t.Fatal("exact-message envelope is not version-stable")
		}

		var got MessageResponseEnvelope
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal("failed to decode exact-message response")
		}
		got.Message.Timestamp = got.Message.Timestamp.UTC()
		if got.Message.TranscribedAt != nil {
			transcribedAt := got.Message.TranscribedAt.UTC()
			got.Message.TranscribedAt = &transcribedAt
		}
		if !reflect.DeepEqual(got.Message, want) {
			t.Fatal("exact-message response did not use the established projection")
		}
	})

	t.Run("canonical percent escape is not decoded twice", func(t *testing.T) {
		target := "/api/messages/" + url.PathEscape(testPercentMessageID) +
			"?chat_jid=" + url.QueryEscape(testOtherChatJID)
		recorder := performRequest(handler, target)
		if recorder.Code != http.StatusOK {
			t.Fatalf(
				"unexpected HTTP status: got %d, want %d",
				recorder.Code,
				http.StatusOK,
			)
		}
		var got MessageResponseEnvelope
		if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
			t.Fatal("failed to decode percent-literal response")
		}
		if got.Message.ID != testPercentMessageID {
			t.Fatal("canonical percent escape was decoded twice")
		}
	})

	t.Run("wrong chat does not fall back", func(t *testing.T) {
		target := "/api/messages/" + url.PathEscape(testExactMessageID) +
			"?chat_jid=" + url.QueryEscape(testOtherChatJID)
		recorder := performRequest(handler, target)
		requireErrorResponse(
			t,
			recorder,
			http.StatusNotFound,
			ErrorResponse{Error: "message not found", Code: "NOT_FOUND"},
		)
	})

	t.Run("unknown message", func(t *testing.T) {
		target := "/api/messages/TEST_SLICE2_MISSING_MESSAGE?chat_jid=" +
			url.QueryEscape(testExactChatJID)
		recorder := performRequest(handler, target)
		requireErrorResponse(
			t,
			recorder,
			http.StatusNotFound,
			ErrorResponse{Error: "message not found", Code: "NOT_FOUND"},
		)
	})
}

func TestGetMessageDatabaseErrorPostgres(t *testing.T) {
	messageStore, _ := openMessageTestDatabase(t)
	if err := messageStore.Close(); err != nil {
		t.Fatal("failed to close synthetic message store")
	}

	target := "/api/messages/TEST_SLICE2_DB_ERROR?chat_jid=" +
		url.QueryEscape(testExactChatJID)
	recorder := performRequest(testAPIHandler(messageStore), target)
	requireErrorResponse(
		t,
		recorder,
		http.StatusInternalServerError,
		ErrorResponse{Error: "failed to get message", Code: "DB_ERROR"},
	)
}

func TestCheckNewMessagesUsesJIDPostgres(t *testing.T) {
	messageStore, database := openMessageTestDatabase(t)
	seedMessageFixtures(t, database)
	handler := testAPIHandler(messageStore)
	since := strconv.FormatInt(testExactTimestamp.Add(-time.Second).UnixMilli(), 10)

	t.Run("jid wins over chat_jid", func(t *testing.T) {
		query := url.Values{
			"since":    {since},
			"jid":      {testExactChatJID},
			"chat_jid": {testOtherChatJID},
		}
		recorder := performRequest(handler, "/api/check?"+query.Encode())
		if recorder.Code != http.StatusOK {
			t.Fatalf(
				"unexpected HTTP status: got %d, want %d",
				recorder.Code,
				http.StatusOK,
			)
		}
		var got CheckResponse
		if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
			t.Fatal("failed to decode check response")
		}
		if got.Count != 1 || len(got.Messages) != 1 ||
			got.Messages[0].ID != testExactMessageID {
			t.Fatal("check endpoint did not filter by jid")
		}
	})

	t.Run("chat_jid is not the check filter", func(t *testing.T) {
		query := url.Values{
			"since":    {since},
			"chat_jid": {testOtherChatJID},
		}
		recorder := performRequest(handler, "/api/check?"+query.Encode())
		if recorder.Code != http.StatusOK {
			t.Fatalf(
				"unexpected HTTP status: got %d, want %d",
				recorder.Code,
				http.StatusOK,
			)
		}
		var got CheckResponse
		if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
			t.Fatal("failed to decode check response")
		}
		if got.Count != 2 || len(got.Messages) != 2 {
			t.Fatal("check endpoint unexpectedly treated chat_jid as its filter")
		}
		if got.Messages[0].ID != testExactMessageID ||
			got.Messages[1].ID != testOtherMessageID {
			t.Fatal("check endpoint returned an unexpected synthetic message set")
		}
	})
}
