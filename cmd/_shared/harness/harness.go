//go:build testonly
// +build testonly

package harness

import (
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/redisx/server"
)

var portCursor atomic.Int32

func init() { portCursor.Store(7000) }

func FindFreePortIn7k8k(t *testing.T) int {
	t.Helper()
	const attempts = 2000
	for i := 0; i < attempts; i++ {
		raw := portCursor.Add(1)
		p := int(((raw - 7000) % 1000) + 7000)
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err != nil {
			continue
		}
		_ = ln.Close()
		time.Sleep(time.Millisecond)
		return p
	}
	t.Fatalf("FindFreePortIn7k8k: no free 127.0.0.1 port in [7000,8000) after %d attempts", attempts)
	return 0
}

type Harness struct {
	T          *testing.T
	AppPort    int
	CtrlPort   int
	DBPath     string
	DataDir    string
	AppBindIP  string
	CtrlBindIP string
	AppAuth    string
	CtrlAuth   string
}

type HarnessOpts struct {
	AppAuth           string
	CtrlAuth          string
	AppBindIP         string
	CtrlBindIP        string
	CtrlTrustProxy    bool
	CtrlDangerBindAny bool
}

func NewHarness(t *testing.T, opt HarnessOpts) *Harness {
	t.Helper()
	if opt.AppBindIP == "" {
		opt.AppBindIP = "127.0.0.1"
	}
	if opt.CtrlBindIP == "" {
		opt.CtrlBindIP = "127.0.0.1"
	}
	appPort := FindFreePortIn7k8k(t)
	ctrlPort := FindFreePortIn7k8k(t)
	for ctrlPort == appPort {
		ctrlPort = FindFreePortIn7k8k(t)
	}

	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, "integ.db")

	cfg := &server.Config{
		App: server.AppConfig{
			Bind: opt.AppBindIP,
			Port: appPort,
			Auth: opt.AppAuth,
		},
		Ctrl: server.CtrlConfig{
			Bind:          opt.CtrlBindIP,
			Port:          ctrlPort,
			Auth:          opt.CtrlAuth,
			TrustProxy:    opt.CtrlTrustProxy,
			DangerBindAny: opt.CtrlDangerBindAny,
		},
		DataPath: dbPath,
	}

	t.Logf("harness: boot app=%s:%d ctrl=%s:%d db=%s app_auth=%q ctrl_auth=%q trust_proxy=%v danger=%v",
		opt.AppBindIP, appPort, opt.CtrlBindIP, ctrlPort, dbPath,
		opt.AppAuth, opt.CtrlAuth, opt.CtrlTrustProxy, opt.CtrlDangerBindAny)

	db := server.StartWith(cfg)
	if db == nil {
		t.Fatalf("harness: StartWith returned nil (Validate fatal; check slog output above)")
	}
	t.Cleanup(func() { _ = server.Stop() })

	finalApp, finalCtrl := db.EffectiveAuthKeys()
	if opt.AppAuth == "" && finalApp != "" {
		opt.AppAuth = finalApp
	}
	if opt.CtrlAuth == "" && finalCtrl != "" {
		opt.CtrlAuth = finalCtrl
	}

	waitListenReady(t, opt.AppBindIP, appPort)
	waitListenReady(t, opt.CtrlBindIP, ctrlPort)
	return &Harness{
		T: t, AppPort: appPort, CtrlPort: ctrlPort, DBPath: dbPath, DataDir: dataDir,
		AppBindIP: opt.AppBindIP, CtrlBindIP: opt.CtrlBindIP,
		AppAuth: opt.AppAuth, CtrlAuth: opt.CtrlAuth,
	}
}

func (h *Harness) CtrlBind() string { return h.CtrlBindIP }
func (h *Harness) AppBind() string  { return h.AppBindIP }

func waitListenReady(t *testing.T, ip string, port int) {
	t.Helper()
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 25*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("harness: listener %s not ready within 3s: last err=%v", addr, lastErr)
}
