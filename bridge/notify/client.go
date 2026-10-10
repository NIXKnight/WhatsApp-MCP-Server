// Package notify delivers metadata-only events to an operator-controlled
// private Hermes webhook. It never grants tools or executes WhatsApp actions.
package notify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/config"
	"github.com/NIXKnight/WhatsApp-MCP-Server/bridge/store"
)

var errDestination = errors.New("notification_destination_policy")
var opaqueID = regexp.MustCompile(`^[0-9a-f]{32}$`)
var chatID = regexp.MustCompile(`^\d+@(s\.whatsapp\.net|g\.us|lid)$`)

// Payload is the entire wire contract. Never marshal the message or outbox row.
type Payload struct {
	SchemaVersion  int       `json:"schema_version"`
	EventType      string    `json:"event_type"`
	EventID        string    `json:"event_id"`
	ChatJID        string    `json:"chat_jid"`
	MessageID      string    `json:"message_id"`
	EventTimestamp time.Time `json:"event_timestamp"`
	ReceivedAt     time.Time `json:"received_at"`
}

func payloadBytes(n store.Notification) ([]byte, error) {
	if !opaqueID.MatchString(n.EventID) || n.SchemaVersion != 1 || n.EventType != "whatsapp.message.received" || !chatID.MatchString(n.ChatJID) || n.MessageID == "" || len(n.MessageID) > 512 || n.EventTimestamp.IsZero() || n.ReceivedAt.IsZero() {
		return nil, errors.New("notification_metadata_invalid")
	}
	return json.Marshal(Payload{n.SchemaVersion, n.EventType, n.EventID, n.ChatJID, n.MessageID, n.EventTimestamp.UTC(), n.ReceivedAt.UTC()})
}

type privateDialer struct {
	httpOnly bool
	lookup   func(context.Context, string, string) ([]netip.Addr, error)
	connect  func(context.Context, string, string) (net.Conn, error)
}

func (d privateDialer) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errDestination
	}
	var ips []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		ips = []netip.Addr{ip}
	} else {
		if d.httpOnly {
			return nil, errDestination
		}
		ips, err = d.lookup(ctx, "ip", host)
		if err != nil {
			return nil, errors.New("notification_network_failed")
		}
	}
	if len(ips) == 0 {
		return nil, errDestination
	}
	// Reject mixed public/private answers rather than silently selecting a safe
	// answer. Unmap first; zones, link-local/metadata, multicast and unspecified
	// addresses are not permitted. Do not resolve the hostname again.
	for i, ip := range ips {
		normalized, allowed := config.AllowedNotificationAddress(ip, d.httpOnly)
		if !allowed {
			return nil, errDestination
		}
		ips[i] = normalized
	}
	for _, ip := range ips {
		conn, err := d.connect(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("notification_network_failed")
}

// DeliveryResult contains only fixed classes and bounded retry metadata.
// Accepted means receiver acceptance, not agent execution or completion.
type DeliveryResult struct {
	State      string
	Class      string
	HTTPStatus int
	RetryAfter time.Duration
}

// Sender authenticates delivery without exposing credentials to persistence.
type Sender struct {
	url    string
	secret string
	client *http.Client
	now    func() time.Time
}

// NewSender constructs a private, authenticated client without contacting it.
func NewSender(cfg config.Notifications) (*Sender, error) {
	if !cfg.Enabled || strings.TrimSpace(cfg.Secret) == "" || cfg.Secret == "INSECURE_NO_AUTH" {
		return nil, errors.New("notification_auth_required")
	}
	if len(cfg.Secret) < config.NotificationSecretMinBytes {
		return nil, errors.New("notification_auth_too_short")
	}
	if err := config.ValidateNotificationURL(cfg.URL); err != nil {
		return nil, err
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("notification_timeout_invalid")
	}
	u, _ := url.Parse(cfg.URL)
	dialer := &net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}
	d := privateDialer{httpOnly: u.Scheme == "http", lookup: net.DefaultResolver.LookupNetIP, connect: dialer.DialContext}
	transport := &http.Transport{
		Proxy: nil, DialContext: d.dialContext,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout: cfg.Timeout, ResponseHeaderTimeout: cfg.Timeout,
		MaxConnsPerHost: 1, MaxIdleConns: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: 30 * time.Second,
	}
	return &Sender{url: cfg.URL, secret: cfg.Secret, now: time.Now, client: &http.Client{
		Transport: transport, Timeout: cfg.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// Close releases idle connections; call after delivery has stopped.
func (s *Sender) Close() { s.client.CloseIdleConnections() }

// Deliver makes one bounded attempt. It returns no raw network/receiver errors.
func (s *Sender) Deliver(ctx context.Context, n store.Notification) DeliveryResult {
	body, err := payloadBytes(n)
	if err != nil {
		return DeliveryResult{State: "dead", Class: "metadata_invalid"}
	}
	ts := strconv.FormatInt(s.now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(s.secret))
	_, _ = mac.Write([]byte(ts + "."))
	_, _ = mac.Write(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(string(body)))
	if err != nil {
		return DeliveryResult{State: "dead", Class: "destination_policy"}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", n.EventID)
	req.Header.Set("X-Webhook-Timestamp", ts)
	req.Header.Set("X-Webhook-Signature-V2", hex.EncodeToString(mac.Sum(nil)))
	resp, err := s.client.Do(req)
	if err != nil {
		class, state := "network", "pending"
		var unknown x509.UnknownAuthorityError
		var invalid x509.CertificateInvalidError
		var hostname x509.HostnameError
		if errors.Is(err, errDestination) {
			class, state = "destination_policy", "dead"
		} else if errors.As(err, &unknown) || errors.As(err, &invalid) || errors.As(err, &hostname) {
			class, state = "tls", "dead"
		} else if ctx.Err() != nil {
			class = "cancelled"
		}
		return DeliveryResult{State: state, Class: class}
	}
	defer func() { _ = resp.Body.Close() }()
	result := DeliveryResult{HTTPStatus: resp.StatusCode, State: "dead", Class: "http_permanent"}
	if resp.StatusCode == 408 || resp.StatusCode == 425 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
		result.State, result.Class = "pending", "http_retry"
		if resp.StatusCode == 429 {
			result.Class = "rate_limited"
		}
		result.RetryAfter = retryAfter(resp.Header.Get("Retry-After"), s.now())
		return result
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result
	}
	result.State, result.Class = "pending", "protocol"
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8193))
	if err != nil || len(raw) > 8192 {
		return result
	}
	var ack struct {
		Status     string `json:"status"`
		DeliveryID string `json:"delivery_id"`
	}
	if json.Unmarshal(raw, &ack) != nil {
		return result
	}
	if ack.Status == "ignored" {
		result.State, result.Class = "ignored", "receiver_ignored"
		return result
	}
	if ack.DeliveryID == n.EventID && ((resp.StatusCode == 202 && (ack.Status == "accepted" || ack.Status == "coalesced")) || (resp.StatusCode == 200 && ack.Status == "duplicate")) {
		result.State, result.Class = "accepted", ""
	}
	return result
}

func retryAfter(raw string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil && seconds >= 0 {
		if seconds > int64((1<<63-1)/time.Second) {
			return time.Duration(1<<63 - 1)
		}
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(raw); err == nil && date.After(now) {
		return date.Sub(now)
	}
	return 0
}
