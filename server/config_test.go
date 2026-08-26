package server

import (
	"os"
	"path/filepath"
	"runtime"
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
	if cfg.App.Auth != "" || cfg.Ctrl.Auth != "" {
		t.Fatalf("sample defaults both auth empty; got app=%q ctrl=%q", cfg.App.Auth, cfg.Ctrl.Auth)
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

func TestConfigFailsEqualAuth(t *testing.T) {
	dir := t.TempDir()
	y := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(y, []byte(`
app:
  bind: ""
  port: 7379
  auth: "same"
ctrl:
  bind: "127.0.0.1"
  port: 7381
  auth: "same"
data_path: "./bad.db"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(y)
	if err == nil {
		t.Fatalf("want FATAL equal auth; got nil error")
	}
	t.Logf("got expected err: %v", err)
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
