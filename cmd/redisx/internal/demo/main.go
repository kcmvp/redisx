package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/kcmvp/redisx/server"
)

const defaultConfigName = "redisx.yaml"

func main() {
	cfg, err := server.LoadConfig(defaultConfigName)
	if err != nil {
		slog.Error("load config failed (put a valid redisx.yaml in CWD)",
			"config_file", defaultConfigName, "error", err)
		os.Exit(1)
	}

	slog.Info("demo booting redisx via yaml config",
		"config", defaultConfigName, "db", cfg.DataPath,
		"app", cfg.App.Addr(),
		"ctrl", cfg.Admin.Addr(),
	)
	db := server.StartWithConfig(cfg)
	if db == nil {
		slog.Error("StartWithConfig returned nil (Validate fatal above, refusing to continue)")
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("demo: redisx connect ->")
	if cfg.Admin.Auth == "" {
		fmt.Printf("  redisx -H 127.0.0.1 -p %d\n", cfg.Admin.Port)
	} else {
		fmt.Printf("  redisx -H 127.0.0.1 -p %d -a %s\n", cfg.Admin.Port, cfg.Admin.Auth)
	}
	fmt.Println("  (Ctrl-C / SIGTERM / SIGINT to shutdown cleanly)")
	fmt.Println()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("signal received, stopping server", "signal", sig)
	if stopErr := server.Stop(); stopErr != nil {
		slog.Warn("server.Stop returned error", "error", stopErr)
	}
	slog.Info("demo stopped")
}
