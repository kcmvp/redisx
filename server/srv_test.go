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

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/privateip"
	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/buntdb"
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
	origListenAndServeFn func(role portRole, addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error
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
	_ = Stop()
	time.Sleep(5 * time.Millisecond)

	authStateMu.Lock()
	authKeyMaxConns = map[string]int{}
	authKeyConnCounts = map[string]int{}
	authStateMu.Unlock()

	globalMu.Lock()
	internalAuthKey = "internal-test-key"
	srvOnce = sync.Once{}
	listenAndServeFn = func(_ portRole, addr string, h func(redcon.Conn, redcon.Command), a func(redcon.Conn) bool, c func(redcon.Conn, error)) error {
		return redcon.ListenAndServe(addr, h, a, c)
	}
	globalMu.Unlock()
}

func (s *ServerTestSuite) TearDownTest() {
	_ = Stop()
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

	got := privateip.Filter(addrs)
	want := []string{"192.168.1.23", "10.0.0.8", "172.16.0.9", "fd00::1"}

	if len(got) != len(want) {
		t.Fatalf("Filter() len = %d, want %d; got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Filter()[%d] = %q, want %q; got=%v", i, got[i], want[i], got)
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

	got := privateip.Filter(addrs)
	want := []string{
		"192.168.9.162",
		"10.0.0.8",
		"172.18.0.1",
		"172.19.0.1",
		"fd00::1",
	}

	if len(got) != len(want) {
		t.Fatalf("Filter() len = %d, want %d; got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Filter()[%d] = %q, want %q; got=%v", i, got[i], want[i], got)
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

	// Start server with default external auth key limit = 1
	authLimitDbPath := testutil.DBPath(t)
	alp, ctlP := testutil.AllocateTwoFreePorts(t)
	acfg := &Config{
		DataPath: authLimitDbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: alp},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctlP},
	}
	db := StartWithConfig(acfg)
	if db == nil {
		t.Fatalf("StartWithConfig returned nil; appPort=%d ctrlPort=%d", alp, ctlP)
	}
	addr := acfg.Ctrl.Addr()
	seedAuthKeyLimit(t, db, testExternalAuthKey, 1)
	defer func() { _ = Stop() }()

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
	dynDbPath := testutil.DBPath(t)
	dynApp, dynCtl := testutil.AllocateTwoFreePorts(t)
	dynCfg := &Config{
		DataPath: dynDbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: dynApp},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: dynCtl},
	}
	db := StartWithConfig(dynCfg)
	if db == nil {
		t.Fatalf("StartWithConfig returned nil; appPort=%d ctrlPort=%d", dynApp, dynCtl)
	}
	addr := dynCfg.Ctrl.Addr()
	seedAuthKeyLimit(t, db, testExternalAuthKey, 1)
	defer func() { _ = Stop() }()

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
	sameDbPath := testutil.DBPath(t)
	sameApp, sameCtl := testutil.AllocateTwoFreePorts(t)
	sameCfg := &Config{
		DataPath: sameDbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: sameApp},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: sameCtl},
	}
	db := StartWithConfig(sameCfg)
	if db == nil {
		t.Fatalf("StartWithConfig returned nil; appPort=%d ctrlPort=%d", sameApp, sameCtl)
	}
	addr := sameCfg.Ctrl.Addr()
	seedAuthKeyLimit(t, db, testExternalAuthKey, 2)
	defer func() { _ = Stop() }()

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
	err := listenAndServeWithStop(portRoleUnknown, "invalid-addr:999999", nil, nil, nil)
	s.Error(err)

	// Test valid address
	errCh := make(chan error, 1)
	go func() {
		errCh <- listenAndServeWithStop(portRoleUnknown, addr,
			func(conn redcon.Conn, cmd redcon.Command) { conn.WriteString("OK") },
			func(conn redcon.Conn) bool { return true },
			func(conn redcon.Conn, err error) {},
		)
	}()

	time.Sleep(50 * time.Millisecond)

	// Stop it by closing the listener
	serverMu.Lock()
	var ln net.Listener
	for _, v := range serverListeners {
		ln = v
		break
	}
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

	badDataPath := filepath.Join(parentFile, "kv.db")
	appPort, pErr := testutil.AllocateFreePort()
	s.Require().NoError(pErr)
	ctrlPort, pErr := testutil.AllocateFreePort()
	s.Require().NoError(pErr)
	cfg := &Config{
		DataPath: badDataPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort},
	}
	db := StartWithConfig(cfg)
	s.Nil(db)
	s.True(exitCalled)

	// Reset srvOnce for subsequent tests
	globalMu.Lock()
	srvOnce = sync.Once{}
	globalMu.Unlock()
}

func (s *ServerTestSuite) TestStartListenAndServeFailure() {
	exitCh := make(chan struct{})
	var exitOnce sync.Once
	globalMu.Lock()
	osExitFn = func(code int) {
		exitOnce.Do(func() {
			close(exitCh)
		})
	}
	// Force listenAndServeFn to return an unexpected error
	listenAndServeFn = func(_ portRole, addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
		return errors.New("mock listen error")
	}
	globalMu.Unlock()

	appPort, pErr := testutil.AllocateFreePort()
	s.Require().NoError(pErr)
	ctrlPort, pErr := testutil.AllocateFreePort()
	s.Require().NoError(pErr)
	cfg := &Config{
		DataPath: testutil.DBPath(s.T()),
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort},
	}
	db := StartWithConfig(cfg)
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
	err := Stop()
	if err != nil {
		t.Fatalf("Stop() with nil everything should return nil, got %v", err)
	}

	// Initialize partial state to test cleanup paths
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	serverMu.Lock()
	serverListeners[portRoleUnknown] = ln
	serverMu.Unlock()

	sigCh := make(chan os.Signal, 1)
	doneCh := make(chan struct{})
	shutdownMu.Lock()
	shutdownSignalCh = sigCh
	shutdownDoneCh = doneCh
	shutdownMu.Unlock()

	err = Stop()
	if err != nil {
		t.Fatalf("Stop() returned error: %v", err)
	}

	// Verify cleanup
	serverMu.Lock()
	if len(serverListeners) != 0 {
		t.Errorf("expected serverListeners to be empty, got %d entries", len(serverListeners))
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

	listenCalledCh := make(chan string, 2)
	listenAndServeFn = func(_ portRole, addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
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
		lp1, lp2 := testutil.AllocateTwoFreePorts(t)
		lcfg := &Config{
			DataPath: testutil.DBPath(t),
			App:      AppConfig{Bind: "127.0.0.1", Port: lp1},
			Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: lp2},
		}
		_ = StartWithConfig(lcfg)
		close(doneCh)
	}()

	select {
	case <-listenCalledCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StartWithConfig should invoke listenAndServeFn for either App or Ctrl listener")
	}

	// Wait for goroutine to fully finish and call osExitFn
	select {
	case <-doneCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StartWithConfig goroutine did not complete")
	}

	// Small sleep to ensure the osExitFn callback finishes execution
	time.Sleep(10 * time.Millisecond)

	if !exitCalled.Load() {
		t.Error("expected osExitFn to be called on listen error")
	}
}

func (s *ServerTestSuite) TestStartInvokesListener() {
	t := s.T()

	listenCalledCh := make(chan string, 4)
	listenAndServeFn = func(_ portRole, addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
		select {
		case listenCalledCh <- addr:
		default:
		}
		return nil
	}

	appPort, pErr := testutil.AllocateFreePort()
	s.Require().NoError(pErr)
	ctrlPort := 16380

	cfg := &Config{
		DataPath: testutil.DBPath(t),
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort},
	}
	startCtrlAddr := cfg.Ctrl.Addr()
	_ = StartWithConfig(cfg)

	gotAddrs := map[string]struct{}{}
	timeout := time.After(500 * time.Millisecond)
loop:
	for i := 0; i < 2; i++ {
		select {
		case addr := <-listenCalledCh:
			gotAddrs[addr] = struct{}{}
		case <-timeout:
			break loop
		}
	}
	if _, ok := gotAddrs[startCtrlAddr]; !ok {
		keys := make([]string, 0, len(gotAddrs))
		for k := range gotAddrs {
			keys = append(keys, k)
		}
		t.Fatalf("StartWithConfig() listener called with %v; want ctrl addr %q present among them", keys, startCtrlAddr)
	}
}

func (s *ServerTestSuite) TestStartUsesPrivateAddr() {
	t := s.T()
	_ = t
	// Legacy behavior "StartForTest("") → default to privateAddr" is obsolete
	// because StartForTest was removed. The equivalent modern path is
	// StartWithConfig with Ctrl.Port explicitly set to 16380 — exercised by
	// TestStartInvokesListener above.
}

func rawCmd(t *testing.T, conn net.Conn, timeout time.Duration, args ...string) string {
	t.Helper()
	parts := make([]string, 0, len(args)*3+2)
	parts = append(parts, fmt.Sprintf("*%d\r\n", len(args)))
	for _, a := range args {
		parts = append(parts, fmt.Sprintf("$%d\r\n%s\r\n", len(a), a))
	}
	raw := strings.Join(parts, "")
	if _, err := conn.Write([]byte(raw)); err != nil {
		t.Fatalf("raw write failed: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("raw read failed: %v", err)
	}
	return string(buf[:n])
}

// App port must never allow any read/write against _auth_:* SSoT namespace.
// This is gate1_NoAuthNsAccessOnAppPort (#1 filter, before gate2/gate3/gate4),
// so even when AUTH is satisfied and the command passes every other gate we
// still reply with literal "No Privilege".
func (s *ServerTestSuite) TestAppPortHasNoAuthNsPrivilege() {
	t := s.T()

	dbPath := testutil.DBPath(t)
	appP, ctrlP := testutil.AllocateTwoFreePorts(t)

	// Seed explicit auth values so gate2_AuthKeyMatch + gate3_CommandByPortRole
	// pass cleanly — the test is about privilege, not AUTH failures.
	const (
		appAuth  = "s.app-auth-778"
		ctrlAuth = "s.ctrl-auth-778"
	)
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appP, Auth: appAuth},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlP, Auth: ctrlAuth},
	}
	db := StartWithConfig(cfg)
	if db == nil {
		t.Fatalf("StartWithConfig nil; appP=%d ctrlP=%d", appP, ctrlP)
	}
	defer func() { _ = Stop() }()
	time.Sleep(15 * time.Millisecond)

	// ————— App-port client, AUTHed with app AUTH key —————
	appConn, err := net.Dial("tcp", cfg.App.Addr())
	if err != nil {
		t.Fatalf("dial app: %v", err)
	}
	defer func() { _ = appConn.Close() }()
	if resp := rawCmd(t, appConn, 250*time.Millisecond, "AUTH", appAuth); !strings.Contains(resp, "+OK") {
		t.Fatalf("AUTH app port failed: %q", resp)
	}

	for _, tc := range [][]string{
		{"GET", naming.AuthStorageKey("app_0")},
		{"GET", naming.AuthStorageKey("ctrl_0")},
		{"SET", naming.AuthStorageKey("app_0"), "newval"},
		{"SET", naming.AuthStorageKey("ctrl_0"), "newval"},
		{"DEL", naming.AuthStorageKey("app_0")},
		{"DEL", naming.AuthStorageKey("ctrl_0")},
		{"EXISTS", naming.AuthStorageKey("app_0")},
		{"TTL", naming.AuthStorageKey("app_0")},
		{"PERSIST", naming.AuthStorageKey("app_0")},
		{"TYPE", naming.AuthStorageKey("app_0")},
		{"KEYS", naming.AuthNsPrefix() + naming.StorageKeySeparator() + "*"},
		{"KEYS", "*"},
		{"SCAN", "0", "MATCH", "*"},
		{"SCAN", "0", "MATCH", naming.AuthNsPrefix() + naming.StorageKeySeparator() + "*"},
	} {
		resp := rawCmd(t, appConn, 250*time.Millisecond, tc...)
		if !strings.Contains(resp, "No Privilege") {
			t.Fatalf("app-port cmd=%v resp=%q want substring \"No Privilege\"", tc, resp)
		}
	}

	// Spot check: non-_auth_ key still works normally on app port (sanity
	// guard: we didn't accidentally block every GET/SET). Keys must contain
	// a namespace colon separator per KV key policy.
	_ = rawCmd(t, appConn, 250*time.Millisecond, "SET", "appns:user-k", "hello")
	getResp := rawCmd(t, appConn, 250*time.Millisecond, "GET", "appns:user-k")
	if !strings.Contains(getResp, "hello") {
		t.Fatalf("expected app-port plain GET/SET to work normally, got %q", getResp)
	}

	// ————— Ctrl-port client, AUTHed with ctrl AUTH key —————
	ctrlConn, err := net.Dial("tcp", cfg.Ctrl.Addr())
	if err != nil {
		t.Fatalf("dial ctrl: %v", err)
	}
	defer func() { _ = ctrlConn.Close() }()
	if resp := rawCmd(t, ctrlConn, 250*time.Millisecond, "AUTH", ctrlAuth); !strings.Contains(resp, "+OK") {
		t.Fatalf("AUTH ctrl port failed: %q", resp)
	}

	// 1. Ctrl-port GET of _auth_:app_0 / _auth_:ctrl_0 must work (no privilege
	//    error; the values themselves are the SSoT and ctrl is admin-facing).
	appVal := readKeyOrEmptyCtrl(t, db, "app_0")
	ctrlVal := readKeyOrEmptyCtrl(t, db, "ctrl_0")
	if appVal != appAuth || ctrlVal != ctrlAuth {
		t.Fatalf("SSoT mismatch after start: want app=%q ctrl=%q stored app_0=%q ctrl_0=%q",
			appAuth, ctrlAuth, appVal, ctrlVal)
	}
	resp := rawCmd(t, ctrlConn, 250*time.Millisecond, "GET", naming.AuthStorageKey("app_0"))
	if !strings.Contains(resp, appAuth) {
		t.Fatalf("ctrl GET %q want app AUTH in response; got %q", naming.AuthStorageKey("app_0"), resp)
	}
	if strings.Contains(resp, "No Privilege") {
		t.Fatalf("ctrl GET %q must not hit No Privilege gate; got %q", naming.AuthStorageKey("app_0"), resp)
	}

	// 2. Ctrl-port WRITE against _auth_ must still be blocked by the EXISTING
	//    internal-ns WriteGuard (IsInternalKey/ERR internal namespace). It
	//    should NOT be "No Privilege" because the ctrl port passed privilege
	//    gate — the second-stage guard catches it.
	respSet := rawCmd(t, ctrlConn, 250*time.Millisecond, "SET", naming.AuthStorageKey("app_0"), "stolen")
	if strings.Contains(respSet, "+OK") {
		t.Fatalf("ctrl SET %q must not succeed (existing internal WriteGuard); got %q", naming.AuthStorageKey("app_0"), respSet)
	}
	if strings.Contains(respSet, "No Privilege") {
		t.Fatalf("ctrl SET %q rejected by WriteGuard not privilege gate; got %q", naming.AuthStorageKey("app_0"), respSet)
	}
	if !strings.Contains(strings.ToUpper(respSet), "ERR") {
		t.Fatalf("ctrl SET %q want some ERR (internal ns); got %q", naming.AuthStorageKey("app_0"), respSet)
	}
}

func seedBootstrapAuthKV(t *testing.T, db *DB, slot, value string) {
	t.Helper()
	if db == nil || db.disk == nil {
		t.Fatal("seedBootstrapAuthKV: db not open")
	}
	if werr := db.disk.Update(func(tx *buntdb.Tx) error {
		_, _, serr := tx.Set(naming.AuthStorageKey(slot), value, nil)
		return serr
	}); werr != nil {
		t.Fatalf("seedBootstrapAuthKV(slot=%s,value=%s): %v", slot, value, werr)
	}
}

// TestAuthPortRoleKeyBinding verifies:
//
//  1. AUTH on app port with the ctrl_0 key → AUTH succeeds (the key is a
//     legitimate auth key, so AUTH returns +OK), but the NEXT non-handshake
//     command returns WRONGPASS because the key is wrong for the port role.
//  2. AUTH on ctrl port with the app_0 key → symmetric: AUTH succeeds, but
//     subsequent commands return WRONGPASS.
//  3. AUTH on app port with the correct app_0 key → everything passes.
//  4. AUTH on ctrl port with the correct ctrl_0 key → everything passes.
//  5. Extra account slots (app_1, ctrl_1) seeded manually via Raw().Update
//     behave symmetrically to the _0 defaults — their VALUES are accepted
//     strictly on the matching port role.
func (s *ServerTestSuite) TestAuthPortRoleKeyBinding() {
	t := s.T()

	dbPath := testutil.DBPath(t)
	appP, ctrlP := testutil.AllocateTwoFreePorts(t)

	// Use explicit seeds so the banner does not fire (banner is tested via
	// the db unit tests; this test is about wire-protocol WRONGPASS).
	const (
		app0Auth  = "s.app0-auth-3a1f"
		ctrl0Auth = "s.ctrl0-auth-7c2b"
	)

	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appP, Auth: app0Auth},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlP, Auth: ctrl0Auth},
	}
	db := StartWithConfig(cfg)
	if db == nil {
		t.Fatalf("StartWithConfig returned nil")
	}
	defer func() { _ = Stop() }()

	// —— Seed extra account-1 slots directly via Raw disk layer.
	//    (Ctrl-only operational operation; the app port would hit No
	//    Privilege gate instead.)
	const (
		app1Slot  = "app_1"
		ctrl1Slot = "ctrl_1"
		app1Auth  = "s.app1-auth-bb55"
		ctrl1Auth = "s.ctrl1-auth-ee99"
	)
	seedBootstrapAuthKV(t, db, app1Slot, app1Auth)
	seedBootstrapAuthKV(t, db, ctrl1Slot, ctrl1Auth)

	// Helper: dials TCP, issues AUTH then, if AUTH succeeded (reply contained
	// +OK), issues a single plain PING. If AUTH failed (server replied with
	// an error and closed the connection) we skip the PING step to avoid
	// reading EOF on a closed socket.
	authThenPing := func(dialAddr, auth string) (authResp, pingResp string) {
		t.Helper()
		n, derr := net.Dial("tcp", dialAddr)
		s.Require().NoError(derr)
		defer func() { _ = n.Close() }()
		ar := rawCmd(t, n, 300*time.Millisecond, "AUTH", auth)
		authResp = ar
		if strings.Contains(ar, "+OK") {
			pingResp = rawCmd(t, n, 300*time.Millisecond, "PING")
		}
		return authResp, pingResp
	}

	// Case 1a — app port, AUTH with ctrl0 → AUTH itself returns WRONGPASS
	// (fail-fast: AUTH command checks role binding, not delayed to first cmd).
	{
		authR, _ := authThenPing(cfg.App.Addr(), ctrl0Auth)
		if !strings.Contains(authR, "WRONGPASS") {
			t.Fatalf("[app/ctrl0-auth] AUTH response = %q want substring WRONGPASS", authR)
		}
	}
	// Case 1b — ctrl port, AUTH with app0 → AUTH itself returns WRONGPASS
	{
		authR, _ := authThenPing(cfg.Ctrl.Addr(), app0Auth)
		if !strings.Contains(authR, "WRONGPASS") {
			t.Fatalf("[ctrl/app0-auth] AUTH response = %q want substring WRONGPASS", authR)
		}
	}

	// Case 2a — app port with correct app0 AUTH → PING returns PONG
	{
		authR, pingR := authThenPing(cfg.App.Addr(), app0Auth)
		if !strings.Contains(authR, "+OK") {
			t.Fatalf("[app/app0-auth] AUTH = %q want +OK", authR)
		}
		if !strings.Contains(pingR, "+PONG") {
			t.Fatalf("[app/app0-auth] PING = %q want +PONG", pingR)
		}
	}
	// Case 2b — ctrl port with correct ctrl0 AUTH → PING returns PONG
	{
		authR, pingR := authThenPing(cfg.Ctrl.Addr(), ctrl0Auth)
		if !strings.Contains(authR, "+OK") {
			t.Fatalf("[ctrl/ctrl0-auth] AUTH = %q want +OK", authR)
		}
		if !strings.Contains(pingR, "+PONG") {
			t.Fatalf("[ctrl/ctrl0-auth] PING = %q want +PONG", pingR)
		}
	}

	// Case 3a — account 1 app key on app port → AUTH +OK then PING +PONG;
	// same key on ctrl port → AUTH itself returns WRONGPASS (fail-fast role
	// match inside AUTH command, same as Case 1).
	{
		authR, pingR := authThenPing(cfg.App.Addr(), app1Auth)
		if !strings.Contains(authR, "+OK") {
			t.Fatalf("[app/app1-auth] AUTH = %q want +OK", authR)
		}
		if !strings.Contains(pingR, "+PONG") {
			t.Fatalf("[app/app1-auth] PING = %q want +PONG (app_1 slot must be app-port accept list)", pingR)
		}
	}
	{
		authR, _ := authThenPing(cfg.Ctrl.Addr(), app1Auth)
		if !strings.Contains(authR, "WRONGPASS") {
			t.Fatalf("[ctrl/app1-auth] AUTH = %q want substring WRONGPASS — app1 slot only valid on app port", authR)
		}
	}
	// Case 3b — account 1 ctrl key on ctrl port → AUTH +OK then PING +PONG;
	// same key on app port → AUTH itself returns WRONGPASS (fail-fast).
	{
		authR, pingR := authThenPing(cfg.Ctrl.Addr(), ctrl1Auth)
		if !strings.Contains(authR, "+OK") {
			t.Fatalf("[ctrl/ctrl1-auth] AUTH = %q want +OK", authR)
		}
		if !strings.Contains(pingR, "+PONG") {
			t.Fatalf("[ctrl/ctrl1-auth] PING = %q want +PONG (ctrl_1 slot must be ctrl-port accept list)", pingR)
		}
	}
	{
		authR, _ := authThenPing(cfg.App.Addr(), ctrl1Auth)
		if !strings.Contains(authR, "WRONGPASS") {
			t.Fatalf("[app/ctrl1-auth] AUTH = %q want substring WRONGPASS — ctrl1 slot only valid on ctrl port", authR)
		}
	}
}

// thin wrapper that avoids test name collision with server/auth_test.go helpers.
func readKeyOrEmptyCtrl(t *testing.T, db *DB, slot string) string {
	t.Helper()
	return readKeyOrEmptyInternal(t, db, slot)
}

func readKeyOrEmptyInternal(t *testing.T, db *DB, slot string) string {
	t.Helper()
	var v string
	if db == nil || db.disk == nil {
		return ""
	}
	_ = db.disk.View(func(tx *buntdb.Tx) error {
		key := naming.AuthStorageKey(slot)
		gv, gerr := tx.Get(key)
		if gerr != nil && gerr != buntdb.ErrNotFound {
			return gerr
		}
		v = gv
		return nil
	})
	return v
}
