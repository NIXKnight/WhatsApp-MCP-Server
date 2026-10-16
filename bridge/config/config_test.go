package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseManagedStrict(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
		err   bool
	}{
		{name: "unset"},
		{name: "empty", value: ""},
		{name: "true", value: "true", want: true},
		{name: "false", value: "false"},
		{name: "upper", value: "TRUE", err: true},
		{name: "mixed", value: "False", err: true},
		{name: "one", value: "1", err: true},
		{name: "zero", value: "0", err: true},
		{name: "leading whitespace", value: " true", err: true},
		{name: "trailing whitespace", value: "false ", err: true},
		{name: "newline", value: "true\n", err: true},
		{name: "marker", value: "TEST_PRIVATE_MANAGED_VALUE", err: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseManaged(func(string) string { return tt.value })
			if tt.err {
				if err == nil || err.Error() != "BRIDGE_MANAGED_invalid" {
					t.Fatalf("unexpected error: %v", err)
				}
				if strings.Contains(err.Error(), tt.value) {
					t.Fatal("managed value leaked")
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("parseManaged() = %v, %v; want %v, nil", got, err, tt.want)
			}
		})
	}
}

func TestOptionalParsersRetainDefaultsAndFixedWarnings(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
		run  func(func(string) string, warningCollector) bool
	}{
		{
			name: "bool", key: "MEDIA_RETRY_ENABLED",
			want: "bridge_config_invalid_bool_MEDIA_RETRY_ENABLED",
			run: func(getenv func(string) string, warnings warningCollector) bool {
				return getEnvBoolWithWarnings(getenv, "MEDIA_RETRY_ENABLED", true, warnings)
			},
		},
		{
			name: "duration", key: "MEDIA_RETRY_INTERVAL",
			want: "bridge_config_invalid_duration_MEDIA_RETRY_INTERVAL",
			run: func(getenv func(string) string, warnings warningCollector) bool {
				return getEnvDurationWithWarnings(getenv, "MEDIA_RETRY_INTERVAL", 3*time.Minute, warnings) == 3*time.Minute
			},
		},
		{
			name: "int", key: "MEDIA_RETRY_BATCH_SIZE",
			want: "bridge_config_invalid_int_MEDIA_RETRY_BATCH_SIZE",
			run: func(getenv func(string) string, warnings warningCollector) bool {
				return getEnvIntWithWarnings(getenv, "MEDIA_RETRY_BATCH_SIZE", 10, warnings) == 10
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warnings := newWarningCollector()
			if !tt.run(func(key string) string {
				if key != tt.key {
					t.Fatalf("unexpected environment read: %s", key)
				}
				return "TEST_PRIVATE_MALFORMED_VALUE"
			}, warnings) {
				t.Fatal("parser did not retain its default")
			}
			if got := warnings.sorted(); !reflect.DeepEqual(got, []string{tt.want}) {
				t.Fatalf("warnings = %v, want %q", got, tt.want)
			}
		})
	}
}

func TestWarningCollectorDeduplicatesAndSorts(t *testing.T) {
	warnings := newWarningCollector()
	warnings.add("int", "Z_SETTING")
	warnings.add("bool", "A_SETTING")
	warnings.add("int", "Z_SETTING")
	want := []string{
		"bridge_config_invalid_bool_A_SETTING",
		"bridge_config_invalid_int_Z_SETTING",
	}
	if got := warnings.sorted(); !reflect.DeepEqual(got, want) {
		t.Fatalf("warnings = %v, want %v", got, want)
	}
}

func TestLoadManagedAndWarningsIsOutputFree(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test:test@127.0.0.1/test")
	t.Setenv("BRIDGE_DATA_DIR", t.TempDir())
	t.Setenv("BRIDGE_MANAGED", "true")
	t.Setenv("BRIDGE_ADDR", "0.0.0.0:8080")
	t.Setenv("BRIDGE_NOTIFY_ENABLED", "false")
	t.Setenv("MEDIA_RETRY_ENABLED", "treu")
	t.Setenv("MEDIA_RETRY_INTERVAL", "")
	t.Setenv("MEDIA_MAX_DOWNLOAD_ATTEMPTS", "")
	t.Setenv("MEDIA_RETRY_BATCH_SIZE", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Managed || !cfg.MediaRetryEnabled {
		t.Fatal("managed mode or existing media retry fallback was not retained")
	}
	want := []string{"bridge_config_invalid_bool_MEDIA_RETRY_ENABLED"}
	if !reflect.DeepEqual(cfg.Warnings, want) {
		t.Fatalf("warnings = %v, want %v", cfg.Warnings, want)
	}
	if got := NonLoopbackWarning(cfg.Addr); got != NonLoopbackWarningToken || strings.Contains(got, cfg.Addr) {
		t.Fatalf("non-loopback warning is not fixed: %q", got)
	}
	if got := NonLoopbackWarning("127.0.0.1:8080"); got != "" {
		t.Fatalf("loopback address produced warning %q", got)
	}
}
