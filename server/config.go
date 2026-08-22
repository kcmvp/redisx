package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kcmvp/redisx/internal/privateip"
	"gopkg.in/yaml.v3"
)

const (
	defaultConfigFileName = "redisx.yaml"

	defaultLoggingLevel  = "info"
	defaultLoggingFormat = "text"

	defaultAppPort   = 7379
	defaultAdminPort = 7381
)

type AppConfig struct {
	Bind string `yaml:"bind"`
	Port int    `yaml:"port"`
	Auth string `yaml:"auth"`
}

func (a AppConfig) Addr() string {
	return net.JoinHostPort(a.Bind, strconv.Itoa(a.Port))
}

type AdminConfig struct {
	Bind          string `yaml:"bind"`
	Port          int    `yaml:"port"`
	Auth          string `yaml:"auth"`
	TrustProxy    bool   `yaml:"trust_proxy"`
	DangerBindAny bool   `yaml:"danger_bind_any"`
}

func (a AdminConfig) Addr() string {
	return net.JoinHostPort(a.Bind, strconv.Itoa(a.Port))
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type Config struct {
	App      AppConfig     `yaml:"app"`
	Admin    AdminConfig   `yaml:"admin"`
	DataPath string        `yaml:"data_path"`
	Logging  LoggingConfig `yaml:"logging"`
}

func (c *Config) applyDefaults() {
	if c.App.Port == 0 {
		c.App.Port = defaultAppPort
	}
	if c.Admin.Port == 0 {
		c.Admin.Port = defaultAdminPort
	}
	if c.Admin.Bind == "" {
		c.Admin.Bind = "127.0.0.1"
	}
	if c.App.Bind == "" {
		detect := privateip.DetectAppBind(nil)
		if !detect.UsedFallbackLoopback && detect.BindIP != "" {
			slog.Info("auto-selected private IP for app bind (cloud-safe)",
				"ip", detect.BindIP,
				"rank_candidates", detect.PrivateCandidates)
			c.App.Bind = detect.BindIP
		} else {
			if detect.Warning != "" {
				slog.Warn("app bind auto-detect fallback", "reason", detect.Warning)
			}
			c.App.Bind = "127.0.0.1"
		}
	}
	if c.DataPath == "" {
		home, err := os.UserHomeDir()
		if err == nil && home != "" {
			c.DataPath = filepath.Join(home, ".redisx", "redisx.db")
		} else {
			c.DataPath = filepath.Join(".", ".redisx", "redisx.db")
		}
	}
	if c.Logging.Level == "" {
		c.Logging.Level = defaultLoggingLevel
	} else {
		if lvl, lvlErr := normalizeLogLevel(c.Logging.Level); lvlErr != nil {
			slog.Warn(lvlErr.Error())
			c.Logging.Level = lvl
		} else {
			c.Logging.Level = lvl
		}
	}
	if c.Logging.Format == "" {
		c.Logging.Format = defaultLoggingFormat
	} else {
		if fmtRaw, fmtErr := normalizeLogFormat(c.Logging.Format); fmtErr != nil {
			slog.Warn(fmtErr.Error())
			c.Logging.Format = fmtRaw
		} else {
			c.Logging.Format = fmtRaw
		}
	}
}

func normalizeLogLevel(raw string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch v {
	case "debug", "info", "warn", "error":
		return v, nil
	}
	return "info", fmt.Errorf("unknown logging.level %q, fallback info", raw)
}

func normalizeLogFormat(raw string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch v {
	case "text", "json":
		return v, nil
	}
	return "text", fmt.Errorf("unknown logging.format %q, fallback text", raw)
}

const (
	minAllowedPort = 7001
	maxAllowedPort = 65535
)

func validatePort(port int, field string) error {
	if port < minAllowedPort || port > maxAllowedPort {
		return fmt.Errorf("invalid %s=%d: must be %d..%d (ports below %d are reserved; avoid collisions with stock Redis 6379 / default admin 6381)",
			field, port, minAllowedPort, maxAllowedPort, minAllowedPort)
	}
	return nil
}

func resolveOne(bind string, role string) (explicit string, err error) {
	if bind == "" {
		return "", nil
	}
	parsed := net.ParseIP(bind)
	if parsed != nil {
		return bind, nil
	}
	if _, perr := net.LookupIP(bind); perr != nil {
		return bind, fmt.Errorf("%s.bind=%q lookup failed: %w", role, bind, perr)
	}
	return bind, nil
}

func (c *Config) validate(configDir string) error {
	c.applyDefaults()

	if err := validatePort(c.App.Port, "app.port"); err != nil {
		return errors.New("STARTUP FATAL: " + err.Error())
	}
	if err := validatePort(c.Admin.Port, "admin.port"); err != nil {
		return errors.New("STARTUP FATAL: " + err.Error())
	}
	if c.App.Port == c.Admin.Port {
		return fmt.Errorf(
			"STARTUP FATAL: app.port=%d must not equal admin.port=%d; assign distinct TCP ports",
			c.App.Port, c.Admin.Port,
		)
	}
	adminBind, err := resolveOne(c.Admin.Bind, "admin")
	if err != nil {
		return errors.New("STARTUP FATAL: " + err.Error())
	}
	if _, aerr := resolveOne(c.App.Bind, "app"); aerr != nil {
		return errors.New("STARTUP FATAL: " + aerr.Error())
	}
	if adminBind != "" && adminBind != "127.0.0.1" {
		parsed := net.ParseIP(adminBind)
		isLoopback := parsed != nil && parsed.IsLoopback()
		if !isLoopback && !c.Admin.TrustProxy && !c.Admin.DangerBindAny {
			return fmt.Errorf(
				"STARTUP FATAL: admin.bind=%q (resolved) is not loopback. Refusing. "+
					"Either bind admin to 127.0.0.1 (recommended; expose via Caddy+mTLS reverse-proxy), "+
					"or set admin.trust_proxy (running behind trusted L7 PROXY v2 + mTLS verifier), "+
					"or set admin.danger_bind_any (ACKNOWLEDGED unsafe; isolated air-gapped nets only).",
				adminBind,
			)
		}
	}
	if c.App.Auth != "" && c.Admin.Auth != "" && c.App.Auth == c.Admin.Auth {
		return errors.New(
			"STARTUP FATAL: AppAuth equals AdminAuth; dual-port model requires distinct passwords. Refusing.",
		)
	}

	dbPath := c.DataPath
	if !filepath.IsAbs(dbPath) && configDir != "" {
		dbPath = filepath.Join(configDir, dbPath)
	}
	dbDir := filepath.Dir(dbPath)
	if info, err := os.Stat(dbDir); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("STARTUP FATAL: storage.data_path dir %q stat: %w", dbDir, err)
		}
		if mkErr := os.MkdirAll(dbDir, 0o755); mkErr != nil {
			return fmt.Errorf("STARTUP FATAL: storage.data_path dir %q mkdir: %w", dbDir, mkErr)
		}
	} else if !info.IsDir() {
		return fmt.Errorf("STARTUP FATAL: storage.data_path dir %q is not a directory", dbDir)
	}
	if info, stErr := os.Stat(dbPath); stErr == nil {
		if info.IsDir() {
			return fmt.Errorf("STARTUP FATAL: storage.data_path %q is a directory, must be a regular file", dbPath)
		}
		if info.Mode()&os.ModeType != 0 {
			return fmt.Errorf("STARTUP FATAL: storage.data_path %q is not a regular file (mode=%s)", dbPath, info.Mode())
		}
	}
	f, fErr := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0o644)
	if fErr != nil {
		return fmt.Errorf("STARTUP FATAL: storage.data_path %q not openable for write: %w", dbPath, fErr)
	}
	_ = f.Close()
	c.DataPath = dbPath

	if slog.Default() != nil {
		slog.Debug("config validated",
			"config_dir", configDir,
			"app", map[string]string{
				"bind": c.App.Bind, "port": strconv.Itoa(c.App.Port), "auth_set": strconv.FormatBool(c.App.Auth != ""),
			},
			"admin", map[string]string{
				"bind": c.Admin.Bind, "port": strconv.Itoa(c.Admin.Port), "auth_set": strconv.FormatBool(c.Admin.Auth != ""),
			},
			"storage_data_path", c.DataPath,
			"logging_level", c.Logging.Level,
			"logging_format", c.Logging.Format,
		)
	}
	return nil
}

func LoadConfig(yamlPath string) (*Config, error) {
	if yamlPath == "" {
		yamlPath = defaultConfigFileName
	}
	abs, absErr := filepath.Abs(yamlPath)
	if absErr == nil {
		yamlPath = abs
	}
	configDir := filepath.Dir(yamlPath)
	cfg := &Config{}

	raw, readErr := os.ReadFile(yamlPath)
	if readErr == nil {
		if decErr := yaml.Unmarshal(raw, cfg); decErr != nil {
			return nil, fmt.Errorf("parse config %s: %w", yamlPath, decErr)
		}
	} else {
		slog.Warn("config file unavailable, applying system defaults",
			"path", yamlPath, "err", readErr)
	}
	if err := cfg.validate(configDir); err != nil {
		return nil, err
	}
	return cfg, nil
}
