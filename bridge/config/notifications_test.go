package config

import (
	"strings"
	"testing"
	"time"
)

// This deterministic fixture is non-sensitive, test-only material.
const testOnlyNotificationSecret = "TEST_ONLY_NON_SENSITIVE_HMAC_FIXTURE"

func TestNotificationsDisabledDoesNotReadSecret(t *testing.T) {
	cfg, err := loadNotifications(func(key string) string {
		if key != "BRIDGE_NOTIFY_ENABLED" {
			t.Fatalf("disabled configuration read an unnecessary key")
		}
		return "false"
	})
	if err != nil || cfg.Enabled || cfg.Secret != "" || cfg.URL != "" || cfg.Retention != 7*24*time.Hour {
		t.Fatal("invalid disabled defaults")
	}
}

func notificationEnv() map[string]string {
	return map[string]string{"BRIDGE_NOTIFY_ENABLED": "true", "BRIDGE_NOTIFY_URL": "http://127.0.0.1:8644/webhooks/test",
		"BRIDGE_NOTIFY_SECRET": testOnlyNotificationSecret, "BRIDGE_NOTIFY_CHAT_ALLOWLIST": "100@g.us, 200@lid,100@g.us"}
}

func TestNotificationsConfig(t *testing.T) {
	for _, tt := range []struct {
		name, key, value string
		valid            bool
	}{
		{"defaults", "", "", true}, {"boolean", "BRIDGE_NOTIFY_ENABLED", "invalid-test-value", false},
		{"secret missing", "BRIDGE_NOTIFY_SECRET", "", false}, {"bypass rejected", "BRIDGE_NOTIFY_SECRET", "INSECURE_NO_AUTH", false},
		{"allowlist missing", "BRIDGE_NOTIFY_CHAT_ALLOWLIST", "", false}, {"wildcard", "BRIDGE_NOTIFY_CHAT_ALLOWLIST", "*", false},
		{"bare id", "BRIDGE_NOTIFY_CHAT_ALLOWLIST", "100", false}, {"unsupported namespace", "BRIDGE_NOTIFY_CHAT_ALLOWLIST", "100@broadcast", false},
		{"bad duration", "BRIDGE_NOTIFY_TIMEOUT", "invalid-test-value", false}, {"negative", "BRIDGE_NOTIFY_TIMEOUT", "-1s", false},
		{"timeout bound", "BRIDGE_NOTIFY_TIMEOUT", "31s", false}, {"batch bound", "BRIDGE_NOTIFY_BATCH_SIZE", "101", false},
		{"attempt bound", "BRIDGE_NOTIFY_MAX_ATTEMPTS", "0", false}, {"valid override", "BRIDGE_NOTIFY_MAX_ATTEMPTS", "3", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := notificationEnv()
			if tt.key != "" {
				env[tt.key] = tt.value
			}
			cfg, err := loadNotifications(func(key string) string { return env[key] })
			if (err == nil) != tt.valid {
				t.Fatal("unexpected validation outcome")
			}
			if err != nil && (strings.Contains(err.Error(), testOnlyNotificationSecret) || strings.Contains(err.Error(), "invalid-test-value")) {
				t.Fatal("configuration value leaked")
			}
			if tt.valid && len(cfg.ChatAllowlist) != 2 {
				t.Fatal("allowlist was not deduplicated")
			}
		})
	}
}

func TestNotificationSecretMinimumCountsBytes(t *testing.T) {
	env := notificationEnv()
	env["BRIDGE_NOTIFY_SECRET"] = strings.Repeat("é", 15) + "x" // 31 UTF-8 bytes.
	_, err := loadNotifications(func(key string) string { return env[key] })
	if err == nil || err.Error() != "BRIDGE_NOTIFY_SECRET_too_short" || strings.Contains(err.Error(), env["BRIDGE_NOTIFY_SECRET"]) {
		t.Fatal("short secret did not fail with a fixed sanitized error")
	}

	env["BRIDGE_NOTIFY_SECRET"] = strings.Repeat("é", 16) // 32 UTF-8 bytes.
	if _, err := loadNotifications(func(key string) string { return env[key] }); err != nil {
		t.Fatal("32-byte secret was rejected")
	}
}

func TestNotificationURLPolicy(t *testing.T) {
	for _, tt := range []struct {
		name, url string
		valid     bool
	}{
		{"loopback", "http://127.0.0.1:8644/webhooks/test", true}, {"ipv6", "http://[::1]/webhooks/test", true},
		{"mapped loopback", "http://[::ffff:127.0.0.1]/webhooks/test", true}, {"private TLS", "https://10.0.0.1/webhooks/test", true},
		{"private hostname TLS", "https://receiver.example.test/webhooks/test", true},
		{"hostname HTTP", "http://localhost/webhooks/test", false}, {"private HTTP", "http://10.0.0.1/webhooks/test", false},
		{"public TLS", "https://8.8.8.8/webhooks/test", false}, {"mapped public", "https://[::ffff:8.8.8.8]/webhooks/test", false},
		{"metadata", "https://169.254.169.254/webhooks/test", false},
		{"AWS IPv6 metadata", "https://[fd00:ec2::254]/webhooks/test", false},
		{"ordinary ULA", "https://[fd00:abcd::1]/webhooks/test", true}, {"unspecified", "https://[::]/webhooks/test", false},
		{"multicast", "https://[ff02::1]/webhooks/test", false}, {"userinfo", "https://test-user@10.0.0.1/webhooks/test", false},
		{"query", "https://10.0.0.1/webhooks/test?test=1", false}, {"empty query", "https://10.0.0.1/webhooks/test?", false},
		{"fragment", "https://10.0.0.1/webhooks/test#test", false},
		{"empty fragment", "https://10.0.0.1/webhooks/test#", false}, {"path", "https://10.0.0.1/wrong", false},
		{"scheme", "file:///webhooks/test", false}, {"malformed", "://test-url-marker", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNotificationURL(tt.url)
			if (err == nil) != tt.valid {
				t.Fatal("unexpected URL validation outcome")
			}
			if err != nil && strings.Contains(err.Error(), tt.url) {
				t.Fatal("URL leaked")
			}
		})
	}
}
