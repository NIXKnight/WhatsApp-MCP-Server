package notify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/config"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

// Deterministic, NON-SENSITIVE TEST-ONLY authentication material, not a credential.
const testOnlySecret = "TEST_ONLY_NON_SENSITIVE_HMAC_FIXTURE"

func testNotification() store.Notification {
	now := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	return store.Notification{EventID: strings.Repeat("a", 32), SchemaVersion: 1, EventType: "whatsapp.message.received", ChatJID: "100@g.us", MessageID: "TEST_M1", EventTimestamp: now, ReceivedAt: now, ExpiresAt: now.Add(time.Hour), Attempts: 1, LeaseToken: strings.Repeat("b", 32)}
}
func testConfig() config.Notifications {
	return config.Notifications{Enabled: true, URL: "http://127.0.0.1/webhooks/test", Secret: testOnlySecret, ChatAllowlist: []string{"100@g.us"}, Timeout: time.Second, PollInterval: time.Second, MinInterval: 2 * time.Second, BatchSize: 20, MaxAttempts: 10, MaxAge: time.Hour, Retention: 7 * 24 * time.Hour}
}
func testSender(t *testing.T, server *httptest.Server) *Sender {
	t.Helper()
	cfg := testConfig()
	cfg.URL = server.URL + "/webhooks/test"
	s, err := NewSender(cfg)
	if err != nil {
		t.Fatal("test sender construction failed")
	}
	t.Cleanup(s.Close)
	return s
}

func TestSigningAndStableWhitelist(t *testing.T) {
	n := testNotification()
	type capture struct {
		body                     []byte
		timestamp, signature, id string
	}
	ch := make(chan capture, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ch <- capture{body, r.Header.Get("X-Webhook-Timestamp"), r.Header.Get("X-Webhook-Signature-V2"), r.Header.Get("X-Request-ID")}
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted", "delivery_id": n.EventID})
	}))
	defer server.Close()
	s := testSender(t, server)
	now := n.ReceivedAt
	s.now = func() time.Time { return now }
	for i := 0; i < 2; i++ {
		if s.Deliver(context.Background(), n).State != "accepted" {
			t.Fatal("valid receipt not accepted")
		}
		now = now.Add(time.Second)
	}
	a, b := <-ch, <-ch
	if string(a.body) != string(b.body) || a.id != b.id || a.id != n.EventID || a.timestamp == b.timestamp || a.signature == b.signature {
		t.Fatal("retry identity/body or freshness failed")
	}
	for _, c := range []capture{a, b} {
		mac := hmac.New(sha256.New, []byte(testOnlySecret))
		_, _ = mac.Write([]byte(c.timestamp + "."))
		_, _ = mac.Write(c.body)
		if c.signature != hex.EncodeToString(mac.Sum(nil)) {
			t.Fatal("V2 signature mismatch")
		}
		var body map[string]json.RawMessage
		if json.Unmarshal(c.body, &body) != nil || len(body) != 7 {
			t.Fatal("payload shape invalid")
		}
		for _, key := range []string{"schema_version", "event_type", "event_id", "chat_jid", "message_id", "event_timestamp", "received_at"} {
			if _, ok := body[key]; !ok {
				t.Fatal("metadata field missing")
			}
		}
		if strings.Contains(string(c.body), testOnlySecret) || strings.Contains(string(c.body), n.LeaseToken) {
			t.Fatal("auth/bookkeeping leaked into payload")
		}
	}
}

func TestReceiverResponseSemantics(t *testing.T) {
	n := testNotification()
	for _, tt := range []struct {
		name                     string
		code                     int
		status, id, state, class string
	}{
		{"accepted", 202, "accepted", n.EventID, "accepted", ""}, {"coalesced", 202, "coalesced", n.EventID, "accepted", ""},
		{"duplicate", 200, "duplicate", n.EventID, "accepted", ""}, {"ignored", 200, "ignored", "", "ignored", "receiver_ignored"},
		{"wrong id", 202, "accepted", "TEST_OTHER_ID", "pending", "protocol"}, {"wrong status code", 200, "accepted", n.EventID, "pending", "protocol"},
		{"unknown", 202, "unknown", n.EventID, "pending", "protocol"}, {"empty success", 204, "", "", "pending", "protocol"},
		{"bad request", 400, "", "", "dead", "http_permanent"}, {"missing auth", 401, "", "", "dead", "http_permanent"},
		{"forbidden", 403, "", "", "dead", "http_permanent"}, {"unknown route", 404, "", "", "dead", "http_permanent"},
		{"too large", 413, "", "", "dead", "http_permanent"}, {"redirect", 302, "", "", "dead", "http_permanent"},
		{"timeout", 408, "", "", "pending", "http_retry"}, {"early", 425, "", "", "pending", "http_retry"},
		{"rate limit", 429, "", "", "pending", "rate_limited"}, {"server error", 500, "", "", "pending", "http_retry"},
		{"unavailable", 503, "", "", "pending", "http_retry"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(tt.code)
				if tt.code != 204 {
					_ = json.NewEncoder(w).Encode(map[string]string{"status": tt.status, "delivery_id": tt.id})
				}
			}))
			defer server.Close()
			s := testSender(t, server)
			got := s.Deliver(context.Background(), n)
			if got.State != tt.state || got.Class != tt.class || got.HTTPStatus != tt.code {
				t.Fatal("incorrect response classification")
			}
			if tt.code == 429 && got.RetryAfter != time.Minute {
				t.Fatal("Retry-After ignored")
			}
		})
	}
}

func TestBoundedMalformedAcknowledgement(t *testing.T) {
	for _, body := range []string{"TEST_RESPONSE_MARKER", strings.Repeat("x", 8193)} {
		t.Run(strconv.Itoa(len(body)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202); _, _ = io.WriteString(w, body) }))
			defer server.Close()
			got := testSender(t, server).Deliver(context.Background(), testNotification())
			if got.Class != "protocol" || got.State != "pending" {
				t.Fatal("malformed acknowledgement classification")
			}
		})
	}
}

func TestNoProxyOrRedirect(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://proxy.example.test")
	t.Setenv("HTTPS_PROXY", "http://proxy.example.test")
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer server.Close()
	s := testSender(t, server)
	if s.client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("proxy configured")
	}
	if s.Deliver(context.Background(), testNotification()).State != "dead" || redirected.Load() != 0 {
		t.Fatal("redirect followed")
	}
}

func TestSenderRequiresAuthentication(t *testing.T) {
	for _, tt := range []struct {
		secret, wantError string
	}{
		{"", "notification_auth_required"},
		{" ", "notification_auth_required"},
		{"INSECURE_NO_AUTH", "notification_auth_required"},
		{strings.Repeat("x", config.NotificationSecretMinBytes-1), "notification_auth_too_short"},
	} {
		cfg := testConfig()
		cfg.Secret = tt.secret
		if _, err := NewSender(cfg); err == nil || err.Error() != tt.wantError || (tt.secret != "" && strings.Contains(err.Error(), tt.secret)) {
			t.Fatal("authentication did not fail closed with a fixed sanitized error")
		}
	}
	cfg := testConfig()
	cfg.Enabled = false
	if _, err := NewSender(cfg); err == nil || err.Error() != "notification_auth_required" {
		t.Fatal("disabled sender constructed")
	}

	cfg = testConfig()
	cfg.Secret = strings.Repeat("é", config.NotificationSecretMinBytes/2)
	sender, err := NewSender(cfg)
	if err != nil {
		t.Fatal("32-byte secret was rejected")
	}
	sender.Close()
}

func TestPrivateDialerPinsValidatedAddress(t *testing.T) {
	lookups, connections := 0, 0
	d := privateDialer{lookup: func(context.Context, string, string) ([]netip.Addr, error) {
		lookups++
		if lookups > 1 {
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("::ffff:10.0.0.1")}, nil
	},
		connect: func(ctx context.Context, network, address string) (net.Conn, error) {
			connections++
			if address != "10.0.0.1:443" {
				t.Fatal("dial did not pin normalized numeric IP")
			}
			a, b := net.Pipe()
			_ = b.Close()
			return a, nil
		}}
	conn, err := d.dialContext(context.Background(), "tcp", "receiver.example.test:443")
	if err != nil {
		t.Fatal("safe dial failed")
	}
	_ = conn.Close()
	if lookups != 1 || connections != 1 {
		t.Fatal("DNS was resolved again")
	}
}

func TestPrivateDialerRejectsUnsafeAnswers(t *testing.T) {
	for _, ip := range []string{"8.8.8.8", "::ffff:8.8.8.8", "169.254.169.254", "fe80::1", "::", "0.0.0.0", "ff02::1", "224.0.0.1"} {
		t.Run(ip, func(t *testing.T) {
			d := privateDialer{lookup: func(context.Context, string, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr(ip)}, nil
			}, connect: func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("unsafe answer dialed")
				return nil, nil
			}}
			if _, err := d.dialContext(context.Background(), "tcp", "receiver.example.test:443"); err != errDestination {
				t.Fatal("unsafe address allowed")
			}
		})
	}
	d := privateDialer{httpOnly: true, lookup: func(context.Context, string, string) ([]netip.Addr, error) {
		t.Fatal("HTTP hostname resolved")
		return nil, nil
	}}
	if _, err := d.dialContext(context.Background(), "tcp", "localhost:80"); err != errDestination {
		t.Fatal("HTTP hostname allowed")
	}
}

func TestTLSHostnamePreservedAndCertificateVerified(t *testing.T) {
	n := testNotification()
	sni := make(chan string, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sni <- r.TLS.ServerName
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted", "delivery_id": n.EventID})
	}))
	defer server.Close()
	cfg := testConfig()
	u, _ := url.Parse(server.URL)
	cfg.URL = "https://example.com:" + u.Port() + "/webhooks/test"
	s, err := NewSender(cfg)
	if err != nil {
		t.Fatal("TLS sender setup")
	}
	defer s.Close()
	transport := s.client.Transport.(*http.Transport)
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	transport.TLSClientConfig.RootCAs = pool
	d := privateDialer{lookup: func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}, connect: (&net.Dialer{}).DialContext}
	transport.DialContext = d.dialContext
	if s.Deliver(context.Background(), n).State != "accepted" || <-sni != "example.com" {
		t.Fatal("TLS hostname/SNI lost")
	}
	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("TLS verification bypassed")
	}
	untrusted := testSender(t, server)
	if untrusted.Deliver(context.Background(), n).Class != "tls" {
		t.Fatal("untrusted certificate accepted")
	}
}

func TestCancellationAndTimeout(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(strconv.FormatBool(cancelled), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release }))
			defer server.Close()
			defer close(release)
			s := testSender(t, server)
			s.client.Timeout = 250 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan DeliveryResult, 1)
			go func() { done <- s.Deliver(ctx, testNotification()) }()
			<-entered
			if cancelled {
				cancel()
			}
			got := <-done
			if got.State != "pending" {
				t.Fatal("transport failure not retryable")
			}
			if cancelled && got.Class != "cancelled" {
				t.Fatal("cancellation not classified")
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		raw  string
		want time.Duration
	}{{"60", time.Minute}, {now.Add(time.Minute).Format(http.TimeFormat), time.Minute}, {"invalid", 0}, {"-1", 0}, {now.Add(-time.Minute).Format(http.TimeFormat), 0}, {"9223372036854775807", time.Duration(1<<63 - 1)}} {
		if retryAfter(tt.raw, now) != tt.want {
			t.Fatal("Retry-After parse mismatch")
		}
	}
}

func TestPayloadRejectsMalformedMetadata(t *testing.T) {
	n := testNotification()
	n.EventID = "TEST_RAW_ID_MARKER"
	if _, err := payloadBytes(n); err == nil {
		t.Fatal("non-opaque id accepted")
	}
	n = testNotification()
	n.ChatJID = "TEST_BAD_CHAT"
	if _, err := payloadBytes(n); err == nil {
		t.Fatal("incompatible chat accepted")
	}
}

func BenchmarkPayloadBytes(b *testing.B) {
	n := testNotification()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := payloadBytes(n); err != nil {
			b.Fatal("payload encoding failed")
		}
	}
}

func TestAWSMetadataDNSRejectedBeforeAnyDial(b *testing.T) {
	for _, mixed := range []bool{false, true} {
		b.Run(strconv.FormatBool(mixed), func(t *testing.T) {
			dials := 0
			d := privateDialer{
				lookup: func(context.Context, string, string) ([]netip.Addr, error) {
					ips := []netip.Addr{netip.MustParseAddr("fd00:ec2::254")}
					if mixed {
						ips = append([]netip.Addr{netip.MustParseAddr("10.0.0.1")}, ips...)
					}
					return ips, nil
				},
				connect: func(context.Context, string, string) (net.Conn, error) { dials++; return nil, nil },
			}
			if _, err := d.dialContext(context.Background(), "tcp", "receiver.example.test:443"); err != errDestination || dials != 0 {
				t.Fatal("metadata DNS admission failed")
			}
		})
	}
}
