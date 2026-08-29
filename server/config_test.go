package server

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const sampleRelative = "../cmd/redisx/internal/demo/redisx.yaml"

func TestLoadSampleYaml(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	if runtime.GOOS == "windows" {
		t.Setenv("USERPROFILE", homeDir)
	}
	wd, _ := os.Getwd()
	t.Logf("cwd = %s; sample candidate = %s; isolated HOME = %s", wd, filepath.Join(wd, sampleRelative), homeDir)
	_, statErr := os.Stat(sampleRelative)
	if statErr != nil {
		t.Fatalf("sample yaml not found at %s: %v", sampleRelative, statErr)
	}
	cfg, err := LoadConfig(sampleRelative)
	if err != nil {
		t.Fatalf("LoadConfig on sample returned error: %v", err)
	}
	if cfg.App.Port != 7379 {
		t.Fatalf("sample default app.port = %d want 7379", cfg.App.Port)
	}
	if cfg.Ctrl.Port != 7381 {
		t.Fatalf("sample default ctrl.port = %d want 7381", cfg.Ctrl.Port)
	}
	if cfg.Ctrl.Bind != "127.0.0.1" {
		t.Fatalf("sample default ctrl.bind = %q want 127.0.0.1", cfg.Ctrl.Bind)
	}
	if cfg.App.Auth != "123" || cfg.Ctrl.Auth != "234" {
		t.Fatalf("sample pins demo credentials app=123 ctrl=234; got app=%q ctrl=%q", cfg.App.Auth, cfg.Ctrl.Auth)
	}
	if cfg.DataPath == "" {
		t.Fatalf("DataPath must be set (falls back to ~/.redisx/redisx.db when yaml data_path is empty)")
	}
	home, hErr := os.UserHomeDir()
	if hErr == nil {
		want := filepath.Join(home, ".redisx", "redisx.db")
		if cfg.DataPath != want {
			t.Fatalf("yaml omits storage.data_path => expected user-scoped default %q, got %q (sample yaml must not assign empty data_path)", want, cfg.DataPath)
		}
	}
	t.Logf("loaded cfg: app=%s:%d ctrl=%s:%d db=%s",
		cfg.App.Bind, cfg.App.Port, cfg.Ctrl.Bind, cfg.Ctrl.Port, cfg.DataPath)
}

func TestLoadNoConfigFileDefaults(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	if runtime.GOOS == "windows" {
		t.Setenv("USERPROFILE", homeDir)
	}
	dir := t.TempDir()
	absent := filepath.Join(dir, "definitely_not_redisx.yaml")
	cfg, err := LoadConfig(absent)
	if err != nil {
		t.Fatalf("expected defaults when config file absent; got err: %v", err)
	}
	if cfg.App.Port != 7379 {
		t.Fatalf("want app.port default %d, got %d", 7379, cfg.App.Port)
	}
	if cfg.Ctrl.Port != 7381 {
		t.Fatalf("want ctrl.port default %d, got %d", 7381, cfg.Ctrl.Port)
	}
	if cfg.Ctrl.Bind != "127.0.0.1" {
		t.Fatalf("want ctrl.bind default 127.0.0.1, got %q", cfg.Ctrl.Bind)
	}
	if cfg.App.Auth != "" || cfg.Ctrl.Auth != "" {
		t.Fatalf("want no-auth defaults; got app=%q ctrl=%q", cfg.App.Auth, cfg.Ctrl.Auth)
	}
	home, hErr := os.UserHomeDir()
	if hErr == nil {
		want := filepath.Join(home, ".redisx", "redisx.db")
		if cfg.DataPath != want {
			t.Fatalf("no-config DataPath = %q, want %q", cfg.DataPath, want)
		}
	}
	// Actual directory creation + writability check should have passed inside validate.
	dbDir := filepath.Dir(cfg.DataPath)
	info, statErr := os.Stat(dbDir)
	if statErr != nil || !info.IsDir() {
		t.Fatalf("expected default db dir %q to exist after LoadConfig (MkdirAll): stat err=%v", dbDir, statErr)
	}
	t.Logf("no-config case produced DataPath=%q; dir exists=%v", cfg.DataPath, true)
}

func TestConfigAllowsEqualAuth(t *testing.T) {
	dir := t.TempDir()
	y := filepath.Join(dir, "ok.yaml")
	if err := os.WriteFile(y, []byte(`
app:
  bind: ""
  port: 7379
  auth: "same"
ctrl:
  bind: "127.0.0.1"
  port: 7381
  auth: "same"
data_path: "./ok.db"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(y)
	if err != nil {
		t.Fatalf("equal auth is now allowed per simplified rules; got err %v", err)
	}
	if cfg.App.Auth != "same" || cfg.Ctrl.Auth != "same" {
		t.Fatalf("expected auth to round-trip equal values; got app=%q ctrl=%q", cfg.App.Auth, cfg.Ctrl.Auth)
	}
}

func TestConfigFailsNonLoopbackCtrlNoAck(t *testing.T) {
	dir := t.TempDir()
	y := filepath.Join(dir, "bad2.yaml")
	if err := os.WriteFile(y, []byte(`
app:
  bind: ""
  port: 7379
ctrl:
  bind: "0.0.0.0"
  port: 7381
data_path: "./b2.db"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(y)
	if err == nil {
		t.Fatalf("want FATAL ctrl non-loopback no ack; got nil error")
	}
	t.Logf("got expected err: %v", err)
}

func TestConfigAllowsDangerBindAny(t *testing.T) {
	dir := t.TempDir()
	y := filepath.Join(dir, "ok.yaml")
	if err := os.WriteFile(y, []byte(`
app:
  port: 7390
ctrl:
  bind: "0.0.0.0"
  port: 7391
  danger_bind_any: true
data_path: "./ok.db"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(y)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Ctrl.Bind != "0.0.0.0" || cfg.Ctrl.Port != 7391 || !cfg.Ctrl.DangerBindAny {
		t.Fatalf("cfg mismatch: %+v", cfg)
	}
}

// ─── normalizeLogLevel ───

func TestNormalizeLogLevel(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "debug", input: "debug", want: "debug"},
		{name: "info", input: "info", want: "info"},
		{name: "warn", input: "warn", want: "warn"},
		{name: "error", input: "error", want: "error"},
		{name: "case insensitive", input: "DEBUG", want: "debug"},
		{name: "with whitespace", input: "  warn  ", want: "warn"},
		{name: "invalid fallback info", input: "trace", want: "info", wantErr: true},
		{name: "empty invalid", input: "", want: "info", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeLogLevel(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			if got != tt.want {
				t.Fatalf("want %q, got %q", tt.want, got)
			}
		})
	}
}

// ─── normalizeLogFormat ───

func TestNormalizeLogFormat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "text", input: "text", want: "text"},
		{name: "json", input: "json", want: "json"},
		{name: "case insensitive", input: "JSON", want: "json"},
		{name: "with whitespace", input: "  text  ", want: "text"},
		{name: "invalid fallback text", input: "xml", want: "text", wantErr: true},
		{name: "empty invalid", input: "", want: "text", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeLogFormat(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			if got != tt.want {
				t.Fatalf("want %q, got %q", tt.want, got)
			}
		})
	}
}

// ─── validatePort ───

func TestValidatePort(t *testing.T) {
	tests := []struct {
		name    string
		port    int
		field   string
		wantErr bool
	}{
		{name: "min valid", port: 7001, field: "test", wantErr: false},
		{name: "max valid", port: 65535, field: "test", wantErr: false},
		{name: "typical", port: 7379, field: "app.port", wantErr: false},
		{name: "below min", port: 7000, field: "app.port", wantErr: true},
		{name: "reserved", port: 6379, field: "app.port", wantErr: true},
		{name: "zero", port: 0, field: "app.port", wantErr: true},
		{name: "negative", port: -1, field: "app.port", wantErr: true},
		{name: "above max", port: 65536, field: "ctrl.port", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePort(tt.port, tt.field)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

// ─── resolveOne ───

func TestResolveOne(t *testing.T) {
	tests := []struct {
		name    string
		bind    string
		role    string
		want    string
		wantErr bool
	}{
		{name: "empty bind", bind: "", role: "app", want: ""},
		{name: "loopback IP", bind: "127.0.0.1", role: "ctrl", want: "127.0.0.1"},
		{name: "private IP", bind: "10.0.0.1", role: "app", want: "10.0.0.1"},
		{name: "localhost hostname", bind: "localhost", role: "app", want: "localhost"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveOne(tt.bind, tt.role)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got != tt.want {
					t.Fatalf("want %q, got %q", tt.want, got)
				}
			}
		})
	}
}

// ─── validate ───

func TestValidate_SamePort(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	c := &Config{
		App:  AppConfig{Port: 7379},
		Ctrl: CtrlConfig{Port: 7379, Bind: "127.0.0.1"},
		DataPath: filepath.Join(t.TempDir(), "test.db"),
	}
	err := c.validate()
	if err == nil {
		t.Fatal("expected error for same port")
	}
	if !strings.Contains(err.Error(), "must not equal") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_InvalidAppPort(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	c := &Config{
		App:  AppConfig{Port: 80},
		Ctrl: CtrlConfig{Port: 7381, Bind: "127.0.0.1"},
		DataPath: filepath.Join(t.TempDir(), "test.db"),
	}
	err := c.validate()
	if err == nil {
		t.Fatal("expected error for invalid app port")
	}
}

func TestValidate_InvalidCtrlPort(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	c := &Config{
		App:  AppConfig{Port: 7379},
		Ctrl: CtrlConfig{Port: 80, Bind: "127.0.0.1"},
		DataPath: filepath.Join(t.TempDir(), "test.db"),
	}
	err := c.validate()
	if err == nil {
		t.Fatal("expected error for invalid ctrl port")
	}
}

func TestValidate_DataPathIsDir(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	dir := t.TempDir()
	c := &Config{
		App:      AppConfig{Port: 7379},
		Ctrl:     CtrlConfig{Port: 7381, Bind: "127.0.0.1"},
		DataPath: dir, // this is a directory, not a file
	}
	err := c.validate()
	if err == nil {
		t.Fatal("expected error when data_path is a directory")
	}
	if !strings.Contains(err.Error(), "is a directory") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_CtrlBindInvalid(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	// Ctrl port conflict is easier to test than DNS-dependent hostname failures
	c := &Config{
		App:      AppConfig{Port: 7379},
		Ctrl:     CtrlConfig{Port: 7379, Bind: "127.0.0.1"},
		DataPath: filepath.Join(t.TempDir(), "test.db"),
	}
	err := c.validate()
	if err == nil {
		t.Fatal("expected error for same port")
	}
}

// ─── applyDefaults ───

func TestApplyDefaults(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	c := &Config{}
	c.applyDefaults()

	if c.App.Port != 7379 {
		t.Fatalf("want default app.port 7379, got %d", c.App.Port)
	}
	if c.Ctrl.Port != 7381 {
		t.Fatalf("want default ctrl.port 7381, got %d", c.Ctrl.Port)
	}
	if c.Ctrl.Bind != "127.0.0.1" {
		t.Fatalf("want default ctrl.bind 127.0.0.1, got %q", c.Ctrl.Bind)
	}
	if c.App.Bind == "" {
		t.Fatal("app.bind should be auto-detected, got empty")
	}
	if c.DataPath == "" {
		t.Fatal("data_path should have a default")
	}
	if c.Logging.Level != "info" {
		t.Fatalf("want default logging.level info, got %q", c.Logging.Level)
	}
	if c.Logging.Format != "text" {
		t.Fatalf("want default logging.format text, got %q", c.Logging.Format)
	}
}

func TestApplyDefaults_PreservesExplicit(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	c := &Config{
		App:  AppConfig{Port: 9000, Bind: "10.0.0.1"},
		Ctrl: CtrlConfig{Port: 9001, Bind: "0.0.0.0"},
		DataPath: "/tmp/explicit.db",
		Logging: LoggingConfig{Level: "debug", Format: "json"},
	}
	c.applyDefaults()

	if c.App.Port != 9000 {
		t.Fatalf("explicit app.port overwritten: got %d", c.App.Port)
	}
	if c.Ctrl.Port != 9001 {
		t.Fatalf("explicit ctrl.port overwritten: got %d", c.Ctrl.Port)
	}
	if c.Ctrl.Bind != "0.0.0.0" {
		t.Fatalf("explicit ctrl.bind overwritten: got %q", c.Ctrl.Bind)
	}
	if c.App.Bind != "10.0.0.1" {
		t.Fatalf("explicit app.bind overwritten: got %q", c.App.Bind)
	}
	if c.DataPath != "/tmp/explicit.db" {
		t.Fatalf("explicit data_path overwritten: got %q", c.DataPath)
	}
	if c.Logging.Level != "debug" {
		t.Fatalf("explicit logging.level overwritten: got %q", c.Logging.Level)
	}
	if c.Logging.Format != "json" {
		t.Fatalf("explicit logging.format overwritten: got %q", c.Logging.Format)
	}
}

func TestApplyDefaults_InvalidLogLevel_FallsBack(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	c := &Config{Logging: LoggingConfig{Level: "bogus"}}
	c.applyDefaults()
	// invalid level should fallback to "info"
	if c.Logging.Level != "info" {
		t.Fatalf("want fallback info, got %q", c.Logging.Level)
	}
}

func TestApplyDefaults_InvalidLogFormat_FallsBack(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	c := &Config{Logging: LoggingConfig{Format: "xml"}}
	c.applyDefaults()
	// invalid format: normalizeLogFormat returns ("text", err), so code keeps fmtRaw="text"
	if c.Logging.Format != "text" {
		t.Fatalf("want fallback text, got %q", c.Logging.Format)
	}
}
