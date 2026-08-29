package internal

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"
)

var (
	authKeyOnce sync.Once
	authKey     string

	addrMu   sync.Mutex
	addrOnce sync.Once
	ctrlAddr atomic.Value // string
	appAddr  atomic.Value // string
)

func AuthKey() string {
	authKeyOnce.Do(func() {
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		authKey = hex.EncodeToString(b)
	})

	return authKey
}

func SetAddr(ctrl, app string) {
	if ctrl == "" || app == "" {
		slog.Warn("internal.SetAddr called with empty value; ignored", "ctrl", ctrl, "app", app)
		return
	}
	addrMu.Lock()
	defer addrMu.Unlock()
	ctrlPrev, _ := ctrlAddr.Load().(string)
	appPrev, _ := appAddr.Load().(string)
	alreadySet := ctrlPrev != "" || appPrev != ""
	addrOnce.Do(func() {
		slog.Info("internal.SetAddr embedded addresses registered",
			"ctrl", ctrl, "app", app,
		)
		ctrlAddr.Store(ctrl)
		appAddr.Store(app)
	})
	if alreadySet {
		slog.Warn("internal.SetAddr called after addresses already set; ignored. embedded mode supports one in-process server per process",
			"stored_ctrl", ctrlPrev, "stored_app", appPrev,
			"ignored_ctrl", ctrl, "ignored_app", app,
		)
	}
}

func ResetAddrs() {
	addrMu.Lock()
	defer addrMu.Unlock()
	ctrlPrev, _ := ctrlAddr.Load().(string)
	appPrev, _ := appAddr.Load().(string)
	ctrlAddr = atomic.Value{}
	appAddr = atomic.Value{}
	addrOnce = sync.Once{}
	if ctrlPrev != "" || appPrev != "" {
		slog.Info("internal.ResetAddrs cleared stored embedded addresses",
			"cleared_ctrl", ctrlPrev, "cleared_app", appPrev,
		)
	}
}

func CtrlAddr() string {
	if v, ok := ctrlAddr.Load().(string); ok {
		return v
	}
	return ""
}

func AppAddr() string {
	if v, ok := appAddr.Load().(string); ok {
		return v
	}
	return ""
}
