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
	T           *testing.T
	AppPort     int
	AdminPort   int
	DBPath      string
	DataDir     string
	AppBindIP   string
	AdminBindIP string
	AppAuth     string
	AdminAuth   string
}

type HarnessOpts struct {
	AppAuth            string
	AdminAuth          string
	AppBindIP          string
	AdminBindIP        string
	AdminTrustProxy    bool
	AdminDangerBindAny bool
}

func NewHarness(t *testing.T, opt HarnessOpts) *Harness {
	t.Helper()
	if opt.AppBindIP == "" {
		opt.AppBindIP = "127.0.0.1"
	}
	if opt.AdminBindIP == "" {
		opt.AdminBindIP = "127.0.0.1"
	}
	appPort := FindFreePortIn7k8k(t)
	adminPort := FindFreePortIn7k8k(t)
	for adminPort == appPort {
		adminPort = FindFreePortIn7k8k(t)
	}

	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, "integ.db")

	cfg := &server.Config{
		App: server.AppConfig{
			Bind: opt.AppBindIP,
			Port: appPort,
			Auth: opt.AppAuth,
		},
		Admin: server.AdminConfig{
			Bind:          opt.AdminBindIP,
			Port:          adminPort,
			Auth:          opt.AdminAuth,
			TrustProxy:    opt.AdminTrustProxy,
			DangerBindAny: opt.AdminDangerBindAny,
		},
		DataPath: dbPath,
	}

	t.Logf("harness: boot app=%s:%d admin=%s:%d db=%s app_auth=%q admin_auth=%q trust_proxy=%v danger=%v",
		opt.AppBindIP, appPort, opt.AdminBindIP, adminPort, dbPath,
		opt.AppAuth, opt.AdminAuth, opt.AdminTrustProxy, opt.AdminDangerBindAny)

	db := server.StartWithConfig(cfg)
	if db == nil {
		t.Fatalf("harness: StartWithConfig returned nil (Validate fatal; check slog output above)")
	}
	t.Cleanup(func() { _ = server.Stop() })

	waitListenReady(t, opt.AppBindIP, appPort)
	waitListenReady(t, opt.AdminBindIP, adminPort)
	return &Harness{
		T: t, AppPort: appPort, AdminPort: adminPort, DBPath: dbPath, DataDir: dataDir,
		AppBindIP: opt.AppBindIP, AdminBindIP: opt.AdminBindIP,
		AppAuth: opt.AppAuth, AdminAuth: opt.AdminAuth,
	}
}

func (h *Harness) AdminBind() string { return h.AdminBindIP }
func (h *Harness) AppBind() string   { return h.AppBindIP }

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
