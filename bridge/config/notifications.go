package config

import (
	"errors"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var notificationJID = regexp.MustCompile(`^\d+@(s\.whatsapp\.net|g\.us|lid)$`)

// NotificationSecretMinBytes is the enforced HMAC key length in bytes.
const NotificationSecretMinBytes = 32

// Notifications configures the optional metadata-only incoming-message relay.
// Secret is read only from the operator's environment and must never be logged.
type Notifications struct {
	Enabled       bool
	URL           string
	Secret        string
	ChatAllowlist []string
	Timeout       time.Duration
	PollInterval  time.Duration
	MinInterval   time.Duration
	BatchSize     int
	MaxAttempts   int
	MaxAge        time.Duration
	Retention     time.Duration
}

// ValidateNotificationURL checks syntax without DNS or network access. HTTPS
// destinations are also checked against private/loopback addresses when dialed.
func ValidateNotificationURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || strings.Contains(raw, "#") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return errors.New("notification_url_invalid")
	}
	if !strings.HasPrefix(u.Path, "/webhooks/") || strings.TrimPrefix(u.Path, "/webhooks/") == "" || strings.Contains(strings.TrimPrefix(u.Path, "/webhooks/"), "/") {
		return errors.New("notification_url_route_invalid")
	}
	if p := u.Port(); p != "" {
		n, e := strconv.Atoi(p)
		if e != nil || n < 1 || n > 65535 {
			return errors.New("notification_url_port_invalid")
		}
	}
	ip, ipErr := netip.ParseAddr(u.Hostname())
	switch u.Scheme {
	case "http":
		if _, allowed := AllowedNotificationAddress(ip, true); ipErr != nil || !allowed {
			return errors.New("notification_http_requires_literal_loopback")
		}
	case "https":
		if _, allowed := AllowedNotificationAddress(ip, false); ipErr == nil && !allowed {
			return errors.New("notification_destination_not_private")
		}
	default:
		return errors.New("notification_url_scheme_invalid")
	}
	return nil
}

var awsMetadataIPv6 = netip.MustParseAddr("fd00:ec2::254")

// AllowedNotificationAddress validates and normalizes one resolved/literal
// address. Cloud metadata endpoints must be denied before admitting private
// ranges: AWS IPv6 metadata is ULA, not link-local. The returned numeric address
// is the only permitted dial target; no hostname resolution follows this check.
func AllowedNotificationAddress(ip netip.Addr, loopbackOnly bool) (netip.Addr, bool) {
	if !ip.IsValid() || ip.Zone() != "" {
		return netip.Addr{}, false
	}
	ip = ip.Unmap()
	if ip == awsMetadataIPv6 || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return netip.Addr{}, false
	}
	if ip.IsLoopback() || (!loopbackOnly && ip.IsPrivate()) {
		return ip, true
	}
	return netip.Addr{}, false
}

func loadNotifications(getenv func(string) string) (Notifications, error) {
	n := Notifications{Timeout: 5 * time.Second, PollInterval: time.Second, MinInterval: 2 * time.Second,
		BatchSize: 20, MaxAttempts: 10, MaxAge: 24 * time.Hour, Retention: 7 * 24 * time.Hour}
	if v := getenv("BRIDGE_NOTIFY_ENABLED"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return n, errors.New("BRIDGE_NOTIFY_ENABLED_invalid")
		}
		n.Enabled = b
	}
	// Disabled mode deliberately does not look up the secret or destination.
	if !n.Enabled {
		return n, nil
	}
	n.URL = getenv("BRIDGE_NOTIFY_URL")
	if err := ValidateNotificationURL(n.URL); err != nil {
		return Notifications{}, err
	}
	n.Secret = getenv("BRIDGE_NOTIFY_SECRET")
	if strings.TrimSpace(n.Secret) == "" || n.Secret == "INSECURE_NO_AUTH" {
		return Notifications{}, errors.New("BRIDGE_NOTIFY_SECRET_required")
	}
	if len(n.Secret) < NotificationSecretMinBytes {
		return Notifications{}, errors.New("BRIDGE_NOTIFY_SECRET_too_short")
	}
	seen := make(map[string]bool)
	for _, jid := range strings.Split(getenv("BRIDGE_NOTIFY_CHAT_ALLOWLIST"), ",") {
		jid = strings.TrimSpace(jid)
		if !notificationJID.MatchString(jid) {
			return Notifications{}, errors.New("BRIDGE_NOTIFY_CHAT_ALLOWLIST_invalid")
		}
		if !seen[jid] {
			n.ChatAllowlist = append(n.ChatAllowlist, jid)
			seen[jid] = true
		}
	}
	for _, f := range []struct {
		key      string
		dst      *time.Duration
		min, max time.Duration
	}{
		{"TIMEOUT", &n.Timeout, time.Millisecond, 30 * time.Second},
		{"POLL_INTERVAL", &n.PollInterval, 100 * time.Millisecond, time.Minute},
		{"MIN_INTERVAL", &n.MinInterval, 100 * time.Millisecond, time.Minute},
		{"MAX_AGE", &n.MaxAge, time.Minute, 7 * 24 * time.Hour},
		{"RETENTION", &n.Retention, time.Hour, 30 * 24 * time.Hour},
	} {
		key := "BRIDGE_NOTIFY_" + f.key
		if v := getenv(key); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d < f.min || d > f.max {
				return Notifications{}, errors.New(key + "_invalid")
			}
			*f.dst = d
		}
	}
	for _, f := range []struct {
		key string
		dst *int
	}{{"BATCH_SIZE", &n.BatchSize}, {"MAX_ATTEMPTS", &n.MaxAttempts}} {
		key := "BRIDGE_NOTIFY_" + f.key
		if v := getenv(key); v != "" {
			x, err := strconv.Atoi(v)
			if err != nil || x < 1 || x > 100 {
				return Notifications{}, errors.New(key + "_invalid")
			}
			*f.dst = x
		}
	}
	return n, nil
}
