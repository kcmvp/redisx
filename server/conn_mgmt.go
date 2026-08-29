package server

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/tidwall/buntdb"
)

// ——— Effective port management ———

var (
	effPortMu   sync.RWMutex
	effPorts    = map[portRole]int{}
	effPortsSet = map[portRole]bool{}
)

func recordEffectivePort(role portRole, port int) {
	effPortMu.Lock()
	defer effPortMu.Unlock()
	effPorts[role] = port
	effPortsSet[role] = true
}

func getEffectivePort(role portRole) (int, bool, int) {
	effPortMu.RLock()
	defer effPortMu.RUnlock()
	v, ok := effPorts[role]
	defaultV := defaultAppPort
	if role == portRoleCtrl {
		defaultV = defaultCtrlPort
	}
	return defaultV, ok, v
}

// ——— Auth connection tracking ———

const (
	unlimitedAuthConns = -1
)

var (
	authStateMu       sync.Mutex
	authKeyMaxConns   = map[string]int{}
	authKeyConnCounts = map[string]int{}
)

func authLimitStoreKey(key string) string {
	return naming.AuthStorageKey(key)
}

func loadAuthKeyLimits(db *DB) error {
	limits := map[string]int{}

	keysRes := db.Keys(naming.AuthStorageGlob())
	if keysRes.IsError() {
		return keysRes.Error()
	}

	for _, storeKey := range keysRes.MustGet() {
		key, _ := naming.StripAuthPrefixIfAuth(storeKey)
		valRes := db.Get(storeKey)
		if valRes.IsError() {
			if errors.Is(valRes.Error(), buntdb.ErrNotFound) {
				slog.Info("skip expired auth key limit while loading", "auth_key", key)
				continue
			}
			return valRes.Error()
		}

		limit, err := strconv.Atoi(valRes.MustGet())
		if err != nil {
			slog.Info("skip unavailable auth key limit while loading", "auth_key", key)
			continue
		}
		limits[key] = limit
	}

	authStateMu.Lock()
	authKeyMaxConns = limits
	authKeyConnCounts = map[string]int{}
	authStateMu.Unlock()
	return nil
}

func refreshAuthLimit(db *DB, key string) (int, bool, error) {
	if key == internalAuthKey {
		return unlimitedAuthConns, true, nil
	}

	res := db.Get(authLimitStoreKey(key))
	if res.IsError() {
		if errors.Is(res.Error(), buntdb.ErrNotFound) {
			authStateMu.Lock()
			delete(authKeyMaxConns, key)
			authStateMu.Unlock()
			return 0, false, nil
		}
		return 0, false, res.Error()
	}

	limit, err := strconv.Atoi(res.MustGet())
	if err != nil {
		authStateMu.Lock()
		delete(authKeyMaxConns, key)
		authStateMu.Unlock()
		slog.Info("treat unavailable auth key limit as expired", "auth_key", key)
		return 0, false, nil
	}

	authStateMu.Lock()
	authKeyMaxConns[key] = limit
	authStateMu.Unlock()
	return limit, true, nil
}

func releaseAuthConn(key string, db *DB) {
	if key == "" || key == internalAuthKey {
		return
	}
	appValues, ctrlValues, _, _, err := loadAllAuthPortKeys(db)
	if err == nil {
		if len(appValues) > 0 || len(ctrlValues) > 0 {
			if _, isApp := appValues[key]; isApp {
				return
			}
			if _, isCtrl := ctrlValues[key]; isCtrl {
				return
			}
		}
	}

	authStateMu.Lock()
	defer authStateMu.Unlock()

	if current := authKeyConnCounts[key]; current > 1 {
		authKeyConnCounts[key] = current - 1
	} else {
		delete(authKeyConnCounts, key)
	}
}

func acquireAuthConn(db *DB, key string) error {
	if key == internalAuthKey {
		return nil
	}
	appValues, ctrlValues, _, _, err := loadAllAuthPortKeys(db)
	if err != nil {
		return err
	}
	if len(appValues) > 0 || len(ctrlValues) > 0 {
		if _, isApp := appValues[key]; isApp {
			return nil
		}
		if _, isCtrl := ctrlValues[key]; isCtrl {
			return nil
		}
	}
	limit, ok, err := refreshAuthLimit(db, key)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("auth key not found")
	}
	if limit != unlimitedAuthConns && limit < 0 {
		return fmt.Errorf("invalid auth limit for key %s", key)
	}

	authStateMu.Lock()
	defer authStateMu.Unlock()

	if limit != unlimitedAuthConns && authKeyConnCounts[key] >= limit {
		return fmt.Errorf("auth key connection limit exceeded")
	}

	authKeyConnCounts[key]++
	return nil
}

// ——— Signal handling ———

var (
	shutdownSignalCh chan os.Signal
	shutdownDoneCh   chan struct{}

	signalNotifyFn = signal.Notify
	signalStopFn   = signal.Stop
)

func handleShutdownSignals(sigCh <-chan os.Signal, doneCh <-chan struct{}, stopFn func() error) {
	select {
	case sig := <-sigCh:
		if sig != nil {
			slog.Info("redisx server caught shutdown signal", "signal", sig.String())
		} else {
			slog.Info("redisx server caught shutdown signal")
		}
		if err := stopFn(); err != nil {
			slog.Warn("graceful shutdown failed", "error", err)
		}
	case <-doneCh:
	}
}

func watchShutdownSignals() {
	shutdownOnce.Do(func() {
		sigCh := make(chan os.Signal, 1)
		doneCh := make(chan struct{})
		shutdownMu.Lock()
		shutdownSignalCh = sigCh
		shutdownDoneCh = doneCh
		shutdownMu.Unlock()

		signalNotifyFn(sigCh, os.Interrupt, syscall.SIGTERM)
		go handleShutdownSignals(sigCh, doneCh, Stop)
	})
}
