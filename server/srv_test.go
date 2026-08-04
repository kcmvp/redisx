package server

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/redcon"
)

const testExternalAuthKey = "external-test-key"

func getFreePort() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "127.0.0.1:6380"
	}
	defer func() {
		_ = l.Close()
	}()
	return l.Addr().String()
}

type ServerTestSuite struct {
	suite.Suite
	origInternalAuthKey  string
	origListenAndServeFn func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error
	origOsExitFn         func(code int)
}

func (s *ServerTestSuite) SetupSuite() {
	globalMu.Lock()
	s.origInternalAuthKey = internalAuthKey
	s.origListenAndServeFn = listenAndServeFn
	s.origOsExitFn = osExitFn
	globalMu.Unlock()
}

func (s *ServerTestSuite) SetupTest() {
	_ = stop()
	time.Sleep(5 * time.Millisecond)

	authStateMu.Lock()
	authKeyMaxConns = map[string]int{}
	authKeyConnCounts = map[string]int{}
	authStateMu.Unlock()

	globalMu.Lock()
	internalAuthKey = "internal-test-key"
	srvOnce = sync.Once{}
	listenAndServeFn = redcon.ListenAndServe
	globalMu.Unlock()
}

func (s *ServerTestSuite) TearDownTest() {
	_ = stop()
	time.Sleep(5 * time.Millisecond)

	authStateMu.Lock()
	authKeyMaxConns = map[string]int{}
	authKeyConnCounts = map[string]int{}
	authStateMu.Unlock()

	globalMu.Lock()
	internalAuthKey = s.origInternalAuthKey
	listenAndServeFn = s.origListenAndServeFn
	osExitFn = s.origOsExitFn
	srvOnce = sync.Once{}
	globalMu.Unlock()
}

func TestServerSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}

func TestCollectPrivateIPs(t *testing.T) {
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		&net.IPNet{IP: net.ParseIP("192.168.1.23"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("10.0.0.8"), Mask: net.CIDRMask(24, 32)},
		&net.IPAddr{IP: net.ParseIP("10.0.0.8")},
		&net.IPNet{IP: net.ParseIP("172.16.0.9"), Mask: net.CIDRMask(16, 32)},
		&net.IPNet{IP: net.ParseIP("8.8.8.8"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("fd00::1"), Mask: net.CIDRMask(64, 128)},
		&net.IPNet{IP: net.ParseIP("fe80::1"), Mask: net.CIDRMask(64, 128)},
	}

	got := collectPrivateIPs(addrs)
	want := []string{"192.168.1.23", "10.0.0.8", "172.16.0.9", "fd00::1"}

	if len(got) != len(want) {
		t.Fatalf("collectPrivateIPs() len = %d, want %d; got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("collectPrivateIPs()[%d] = %q, want %q; got=%v", i, got[i], want[i], got)
		}
	}
}

func TestCollectPrivateIPsPrefersLANOrder(t *testing.T) {
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("172.19.0.1"), Mask: net.CIDRMask(16, 32)},
		&net.IPNet{IP: net.ParseIP("192.168.9.162"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("10.0.0.8"), Mask: net.CIDRMask(24, 32)},
		&net.IPNet{IP: net.ParseIP("172.18.0.1"), Mask: net.CIDRMask(16, 32)},
		&net.IPNet{IP: net.ParseIP("fd00::1"), Mask: net.CIDRMask(64, 128)},
	}

	got := collectPrivateIPs(addrs)
	want := []string{
		"192.168.9.162",
		"10.0.0.8",
		"172.18.0.1",
		"172.19.0.1",
		"fd00::1",
	}

	if len(got) != len(want) {
		t.Fatalf("collectPrivateIPs() len = %d, want %d; got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("collectPrivateIPs()[%d] = %q, want %q; got=%v", i, got[i], want[i], got)
		}
	}
}

func replaceID(arr [][]string, id string) [][]string {
	if arr == nil {
		return nil
	}
	res := make([][]string, len(arr))
	for i, a := range arr {
		res[i] = make([]string, len(a))
		for j, str := range a {
			res[i][j] = strings.ReplaceAll(str, "{id}", id)
		}
	}
	return res
}

func replaceID1D(arr []string, id string) []string {
	if arr == nil {
		return nil
	}
	res := make([]string, len(arr))
	for i, str := range arr {
		res[i] = strings.ReplaceAll(str, "{id}", id)
	}
	return res
}

func authOverConn(conn net.Conn, key string) (string, error) {
	if _, err := fmt.Fprintf(conn, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(key), key); err != nil {
		return "", err
	}

	buf := make([]byte, 128)
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

func seedAuthKeyLimit(t *testing.T, db *DB, key string, limit int) {
	t.Helper()
	if err := db.Set(authLimitStoreKey(key), fmt.Sprintf("%d", limit)); err != nil {
		t.Fatalf("failed to seed auth key limit: %v", err)
	}
	if err := loadAuthKeyLimits(db); err != nil {
		t.Fatalf("failed to reload auth key limits: %v", err)
	}
}

func (s *ServerTestSuite) TestConnectionLimits() {
	t := s.T()
	addr := getFreePort()

	// Start server with default external auth key limit = 1
	db := Start(addr, testutil.DBPath(t))
	seedAuthKeyLimit(t, db, testExternalAuthKey, 1)
	defer func() { _ = stop() }()

	time.Sleep(10 * time.Millisecond)

	// Connection 1 should authenticate successfully.
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}
	defer func() { _ = conn1.Close() }()
	resp, err := authOverConn(conn1, testExternalAuthKey)
	if err != nil {
		t.Fatalf("failed to auth conn1: %v", err)
	}
	if !strings.Contains(resp, "+OK") {
		t.Fatalf("conn1 auth response = %q, want +OK", resp)
	}

	authStateMu.Lock()
	count := authKeyConnCounts[testExternalAuthKey]
	authStateMu.Unlock()
	if count != 1 {
		t.Fatalf("authKeyConnCounts[%q] = %d, want 1", testExternalAuthKey, count)
	}

	// Connection 2 should be rejected during AUTH.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	resp, err = authOverConn(conn2, testExternalAuthKey)
	if err == nil && !strings.Contains(resp, "ERR auth key connection limit exceeded") {
		t.Fatalf("conn2 auth response = %q, want limit exceeded error", resp)
	}

	// Count should still be 1
	authStateMu.Lock()
	count = authKeyConnCounts[testExternalAuthKey]
	authStateMu.Unlock()
	if count != 1 {
		t.Fatalf("authKeyConnCounts[%q] after rejected auth = %d, want 1", testExternalAuthKey, count)
	}

	// Close connection 1
	_ = conn1.Close()

	// Wait for server to process close
	time.Sleep(50 * time.Millisecond)

	// Count should be 0
	authStateMu.Lock()
	count = authKeyConnCounts[testExternalAuthKey]
	authStateMu.Unlock()
	if count != 0 {
		t.Fatalf("authKeyConnCounts[%q] after close = %d, want 0", testExternalAuthKey, count)
	}

	// Now connection 3 should succeed since slot is freed.
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn3: %v", err)
	}
	defer func() { _ = conn3.Close() }()

	resp, err = authOverConn(conn3, testExternalAuthKey)
	if err != nil {
		t.Fatalf("failed to auth conn3: %v", err)
	}
	if !strings.Contains(resp, "+OK") {
		t.Fatalf("conn3 auth response = %q, want +OK", resp)
	}

	authStateMu.Lock()
	count = authKeyConnCounts[testExternalAuthKey]
	authStateMu.Unlock()
	if count != 1 {
		t.Fatalf("authKeyConnCounts[%q] = %d, want 1", testExternalAuthKey, count)
	}
}

func (s *ServerTestSuite) TestDynamicAuthLimitRefresh() {
	t := s.T()
	addr := getFreePort()
	db := Start(addr, testutil.DBPath(t))
	seedAuthKeyLimit(t, db, testExternalAuthKey, 1)
	defer func() { _ = stop() }()

	time.Sleep(10 * time.Millisecond)

	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}
	defer func() { _ = conn1.Close() }()

	resp, err := authOverConn(conn1, testExternalAuthKey)
	if err != nil || !strings.Contains(resp, "+OK") {
		t.Fatalf("conn1 auth response = %q, err = %v", resp, err)
	}

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	defer func() { _ = conn2.Close() }()

	resp, err = authOverConn(conn2, testExternalAuthKey)
	if err == nil && !strings.Contains(resp, "ERR auth key connection limit exceeded") {
		t.Fatalf("conn2 auth response = %q, want limit exceeded error", resp)
	}

	if setErr := db.Set(authLimitStoreKey(testExternalAuthKey), "2"); setErr != nil {
		t.Fatalf("failed to update auth limit in storage: %v", setErr)
	}

	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn3: %v", err)
	}
	defer func() { _ = conn3.Close() }()

	resp, err = authOverConn(conn3, testExternalAuthKey)
	if err != nil || !strings.Contains(resp, "+OK") {
		t.Fatalf("conn3 auth response = %q, err = %v", resp, err)
	}

	if setErr := db.Set(authLimitStoreKey(testExternalAuthKey), "1"); setErr != nil {
		t.Fatalf("failed to shrink auth limit in storage: %v", setErr)
	}

	conn4, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn4: %v", err)
	}
	defer func() { _ = conn4.Close() }()

	resp, err = authOverConn(conn4, testExternalAuthKey)
	if err == nil && !strings.Contains(resp, "ERR auth key connection limit exceeded") {
		t.Fatalf("conn4 auth response = %q, want limit exceeded error after shrink", resp)
	}
}

func (s *ServerTestSuite) TestAuthSameConnectionDoesNotDoubleCount() {
	t := s.T()
	addr := getFreePort()
	db := Start(addr, testutil.DBPath(t))
	seedAuthKeyLimit(t, db, testExternalAuthKey, 2)
	defer func() { _ = stop() }()

	time.Sleep(10 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := authOverConn(conn, testExternalAuthKey)
	if err != nil || !strings.Contains(resp, "+OK") {
		t.Fatalf("first auth response = %q, err = %v", resp, err)
	}

	authStateMu.Lock()
	count := authKeyConnCounts[testExternalAuthKey]
	authStateMu.Unlock()
	if count != 1 {
		t.Fatalf("authKeyConnCounts[%q] after first auth = %d, want 1", testExternalAuthKey, count)
	}

	resp, err = authOverConn(conn, testExternalAuthKey)
	if err != nil || !strings.Contains(resp, "+OK") {
		t.Fatalf("second auth response = %q, err = %v", resp, err)
	}

	authStateMu.Lock()
	count = authKeyConnCounts[testExternalAuthKey]
	authStateMu.Unlock()
	if count != 1 {
		t.Fatalf("authKeyConnCounts[%q] after second auth on same connection = %d, want 1", testExternalAuthKey, count)
	}
}

func (s *ServerTestSuite) TestLoadAuthKeyLimitsSkipsUnavailableKeys() {
	db := openDB(testutil.DBPath(s.T()))
	s.Require().NotNil(db)
	defer func() { _ = db.Close() }()

	s.Require().NoError(db.Set(authLimitStoreKey("valid"), "2"))
	s.Require().NoError(db.Set(authLimitStoreKey("expired"), ""))

	err := loadAuthKeyLimits(db)
	s.NoError(err)

	authStateMu.Lock()
	defer authStateMu.Unlock()

	s.Equal(2, authKeyMaxConns["valid"])
	_, exists := authKeyMaxConns["expired"]
	s.False(exists)
}

func (s *ServerTestSuite) TestRefreshAuthLimitTreatsBadValueAsExpired() {
	db := openDB(testutil.DBPath(s.T()))
	s.Require().NotNil(db)
	defer func() { _ = db.Close() }()

	s.Require().NoError(db.Set(authLimitStoreKey("bad"), ""))

	limit, ok, err := refreshAuthLimit(db, "bad")
	s.NoError(err)
	s.False(ok)
	s.Zero(limit)
}

func (s *ServerTestSuite) TestListenAndServeWithStop() {
	addr := getFreePort()

	// Test invalid address
	err := listenAndServeWithStop("invalid-addr:999999", nil, nil, nil)
	s.Error(err)

	// Test valid address
	errCh := make(chan error, 1)
	go func() {
		errCh <- listenAndServeWithStop(addr,
			func(conn redcon.Conn, cmd redcon.Command) { conn.WriteString("OK") },
			func(conn redcon.Conn) bool { return true },
			func(conn redcon.Conn, err error) {},
		)
	}()

	time.Sleep(50 * time.Millisecond)

	// Stop it by closing the listener
	serverMu.Lock()
	ln := serverListener
	serverMu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}

	select {
	case err := <-errCh:
		s.True(isServerShutdownErr(err))
	case <-time.After(1 * time.Second):
		s.Fail("listenAndServeWithStop did not return after closing listener")
	}
}

func (s *ServerTestSuite) TestStartStorageFailure() {
	tempDir := s.T().TempDir()
	parentFile := filepath.Join(tempDir, "not-a-dir")
	f, err := os.Create(parentFile)
	s.Require().NoError(err)
	s.Require().NoError(f.Close())

	exitCalled := false
	globalMu.Lock()
	osExitFn = func(code int) {
		exitCalled = true
	}
	globalMu.Unlock()

	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(prevLogger)

	db := Start("127.0.0.1:0", filepath.Join(parentFile, "kv.db"))
	s.Nil(db)
	s.True(exitCalled)

	// Reset srvOnce for subsequent tests
	globalMu.Lock()
	srvOnce = sync.Once{}
	globalMu.Unlock()
}

func (s *ServerTestSuite) TestStartListenAndServeFailure() {
	exitCh := make(chan struct{})
	globalMu.Lock()
	osExitFn = func(code int) {
		close(exitCh)
	}
	// Force listenAndServeFn to return an unexpected error
	listenAndServeFn = func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
		return errors.New("mock listen error")
	}
	globalMu.Unlock()

	db := Start("", testutil.DBPath(s.T()))
	s.NotNil(db) // DB opens successfully, but background listen fails

	select {
	case <-exitCh:
		// success
	case <-time.After(1 * time.Second):
		s.Fail("osExitFn was not called")
	}

	// Reset srvOnce for subsequent tests
	globalMu.Lock()
	srvOnce = sync.Once{}
	globalMu.Unlock()
}

type localMockConn struct {
	redcon.Conn
	lastErr string
}

func (m *localMockConn) WriteError(msg string) { m.lastErr = msg }

func (s *ServerTestSuite) TestHandleCommandDBNil() {
	var ps redcon.PubSub
	conn := &localMockConn{}
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdPing)}}
	handleCommand(conn, cmd, nil, &ps)
	s.Equal("ERR storage not initialized", conn.lastErr)
}

func (s *ServerTestSuite) TestHandleShutdownSignalsWarnsOnError() {
	_ = s.T()
	sigCh := make(chan os.Signal, 1)
	doneCh := make(chan struct{})

	stopFn := func() error {
		return errors.New("simulated stop error")
	}

	go handleShutdownSignals(sigCh, doneCh, stopFn)

	sigCh <- os.Interrupt

	// Give the goroutine time to process the signal and log the warning
	time.Sleep(50 * time.Millisecond)
}

func (s *ServerTestSuite) TestStop() {
	t := s.T()

	// Ensure stop works when nothing is initialized
	err := stop()
	if err != nil {
		t.Fatalf("stop() with nil everything should return nil, got %v", err)
	}

	// Initialize partial state to test cleanup paths
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	serverMu.Lock()
	serverListener = ln
	serverMu.Unlock()

	sigCh := make(chan os.Signal, 1)
	doneCh := make(chan struct{})
	shutdownMu.Lock()
	shutdownSignalCh = sigCh
	shutdownDoneCh = doneCh
	shutdownMu.Unlock()

	err = stop()
	if err != nil {
		t.Fatalf("stop() returned error: %v", err)
	}

	// Verify cleanup
	serverMu.Lock()
	if serverListener != nil {
		t.Errorf("expected serverListener to be nil")
	}
	serverMu.Unlock()

	shutdownMu.Lock()
	if shutdownSignalCh != nil {
		t.Errorf("expected shutdownSignalCh to be nil")
	}
	if shutdownDoneCh != nil {
		t.Errorf("expected shutdownDoneCh to be nil")
	}
	shutdownMu.Unlock()
}

func (s *ServerTestSuite) TestStartListenError() {
	t := s.T()

	// We use an atomic flag to track if exit was called
	var exitCalled atomic.Bool

	globalMu.Lock()
	osExitFn = func(code int) {
		exitCalled.Store(true)
	}

	listenCalledCh := make(chan string, 1)
	listenAndServeFn = func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
		select {
		case listenCalledCh <- addr:
		default:
		}
		return errors.New("simulated listen error")
	}
	globalMu.Unlock()

	// Make sure the Start completes execution before the test finishes
	doneCh := make(chan struct{})
	go func() {
		_ = Start(getFreePort(), testutil.DBPath(t))
		close(doneCh)
	}()

	select {
	case <-listenCalledCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Start() should invoke listenAndServeFn")
	}

	// Wait for goroutine to fully finish and call osExitFn
	select {
	case <-doneCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Start() goroutine did not complete")
	}

	// Small sleep to ensure the osExitFn callback finishes execution
	time.Sleep(10 * time.Millisecond)

	if !exitCalled.Load() {
		t.Error("expected osExitFn to be called on listen error")
	}
}

func (s *ServerTestSuite) TestStartInvokesListener() {
	t := s.T()

	listenCalledCh := make(chan string, 1)
	listenAndServeFn = func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
		select {
		case listenCalledCh <- addr:
		default:
		}
		return nil
	}

	startAddr := "127.0.0.1:16380"
	_ = Start(startAddr, testutil.DBPath(t))

	select {
	case addr := <-listenCalledCh:
		if addr != startAddr {
			t.Fatalf("Start() listen addr = %q, want %q", addr, startAddr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Start() should invoke listenAndServeFn")
	}

}

func (s *ServerTestSuite) TestStartUsesPrivateAddr() {
	t := s.T()

	listenCalledCh := make(chan string, 1)
	listenAndServeFn = func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
		select {
		case listenCalledCh <- addr:
		default:
		}
		return nil
	}

	_ = Start("", testutil.DBPath(t))

	select {
	case addr := <-listenCalledCh:
		if addr != privateAddr {
			t.Fatalf("Start() listen addr = %q, want %q", addr, privateAddr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Start() should invoke listenAndServeFn")
	}
}
