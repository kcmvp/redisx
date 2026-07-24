package server

import (
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/redisx/storage"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/redcon"
)

// resetStorage clears the singleton instance in storage package. Used only for testing.
// We access the unexported function via go:linkname, or we can just not reset it here and test it differently.
// Since we can't easily call the private `reset` in storage, we just restart the DB with memory flag.
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
	origAuthKey          string
	origExternalMaxConns int
	origListenAndServeFn func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error
	origOsExitFn         func(code int)
}

func (s *ServerTestSuite) SetupSuite() {
	globalMu.Lock()
	s.origInternalAuthKey = internalAuthKey
	s.origAuthKey = authKey
	s.origExternalMaxConns = externalMaxConns
	s.origListenAndServeFn = listenAndServeFn
	s.origOsExitFn = osExitFn
	globalMu.Unlock()
}

func (s *ServerTestSuite) SetupTest() {
	_ = stop()
	time.Sleep(5 * time.Millisecond)
	storage.Reset()

	connCountMu.Lock()
	activeExternalConns = 0
	connCountMu.Unlock()

	globalMu.Lock()
	internalAuthKey = "internal-test-key"
	authKey = "external-test-key"
	externalMaxConns = 1
	srvOnce = sync.Once{}
	listenAndServeFn = redcon.ListenAndServe
	globalMu.Unlock()
}

func (s *ServerTestSuite) TearDownTest() {
	_ = stop()
	time.Sleep(5 * time.Millisecond)
	storage.Reset()

	connCountMu.Lock()
	activeExternalConns = 0
	connCountMu.Unlock()

	globalMu.Lock()
	internalAuthKey = s.origInternalAuthKey
	authKey = s.origAuthKey
	externalMaxConns = s.origExternalMaxConns
	listenAndServeFn = s.origListenAndServeFn
	osExitFn = s.origOsExitFn
	srvOnce = sync.Once{}
	globalMu.Unlock()
}

func TestServerSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
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

func (s *ServerTestSuite) TestConnectionLimits() {
	t := s.T()
	addr := getFreePort()

	// Start server with externalMaxConns = 1
	_ = Start(addr, 1, ":memory:")
	defer func() { _ = stop() }()

	time.Sleep(10 * time.Millisecond)

	// Connection 1 should succeed
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // wait for acceptCon to execute

	// Verify it's active
	connCountMu.Lock()
	count := activeExternalConns
	connCountMu.Unlock()
	if count != 1 {
		t.Fatalf("activeExternalConns = %d, want 1", count)
	}

	// Connection 2 should be rejected/closed immediately
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}

	// We wait a bit to ensure server closed it
	time.Sleep(10 * time.Millisecond)

	// Read from conn2 should fail
	_ = conn2.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	_, err = conn2.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected conn2 to be closed due to limits, but read succeeded")
	}

	// Count should still be 1
	connCountMu.Lock()
	count = activeExternalConns
	connCountMu.Unlock()
	if count != 1 {
		t.Fatalf("activeExternalConns after rejected accept = %d, want 1", count)
	}

	// Close connection 1
	_ = conn1.Close()

	// Wait for server to process close
	time.Sleep(50 * time.Millisecond)

	// Count should be 0
	connCountMu.Lock()
	count = activeExternalConns
	connCountMu.Unlock()
	if count != 0 {
		t.Fatalf("activeExternalConns after close = %d, want 0", count)
	}

	// Now connection 3 should succeed since slot is freed
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial conn3: %v", err)
	}
	defer func() { _ = conn3.Close() }()

	time.Sleep(50 * time.Millisecond) // wait for acceptCon to execute

	connCountMu.Lock()
	count = activeExternalConns
	connCountMu.Unlock()
	if count != 1 {
		t.Fatalf("activeExternalConns = %d, want 1", count)
	}
}

func (s *ServerTestSuite) TestAuthKeyHelpers() {
	// Test resolveExternalAuthKey
	v1 := resolveExternalAuthKey(func(k string) string { return "env-key" })
	s.Equal("env-key", v1)

	v2 := resolveExternalAuthKey(func(k string) string { return "" })
	s.NotEmpty(v2)
	s.NotEqual("env-key", v2)

	// Test ensureExternalAuthKey
	globalMu.Lock()
	authKey = ""
	globalMu.Unlock()

	origEnv := os.Getenv("RESPX_AUTH_KEY")
	_ = os.Setenv("RESPX_AUTH_KEY", "env-key-2")
	ensureExternalAuthKey()
	s.Equal("env-key-2", authKey)

	// Second call should not overwrite
	_ = os.Setenv("RESPX_AUTH_KEY", "env-key-3")
	ensureExternalAuthKey()
	s.Equal("env-key-2", authKey)

	// Test ensureExternalAuthKey with empty env var (generates random)
	globalMu.Lock()
	authKey = ""
	globalMu.Unlock()
	_ = os.Unsetenv("RESPX_AUTH_KEY")
	ensureExternalAuthKey()
	s.NotEmpty(authKey)
	s.NotEqual("env-key-2", authKey)
	s.NotEqual("env-key-3", authKey)

	if origEnv != "" {
		_ = os.Setenv("RESPX_AUTH_KEY", origEnv)
	} else {
		_ = os.Unsetenv("RESPX_AUTH_KEY")
	}
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
	// Setup environment to make storage.Open fail
	tempDir := s.T().TempDir()
	fileHome := tempDir + "/fakehome"
	f, _ := os.Create(fileHome)
	_ = f.Close()
	s.T().Setenv("HOME", fileHome)

	exitCalled := false
	globalMu.Lock()
	osExitFn = func(code int) {
		exitCalled = true
	}
	globalMu.Unlock()

	db := Start("127.0.0.1:0", 0, "/dev/null/kv.db")
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

	db := Start("", 0, ":memory:")
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
		_ = Start(getFreePort(), 3, ":memory:")
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

func (s *ServerTestSuite) TestStartInvokesListenerAndSetsMaxConnections() {
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
	startMaxCon := 3
	_ = Start(startAddr, startMaxCon, ":memory:")

	select {
	case addr := <-listenCalledCh:
		if addr != startAddr {
			t.Fatalf("Start() listen addr = %q, want %q", addr, startAddr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Start() should invoke listenAndServeFn")
	}

	if externalMaxConns != startMaxCon {
		t.Fatalf("externalMaxConns = %d, want %d", externalMaxConns, startMaxCon)
	}
}

func (s *ServerTestSuite) TestStartUsesPrivateAddrAndMinimumMaxConn() {
	t := s.T()

	listenCalledCh := make(chan string, 1)
	listenAndServeFn = func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
		select {
		case listenCalledCh <- addr:
		default:
		}
		return nil
	}

	_ = Start("", 0, ":memory:")

	select {
	case addr := <-listenCalledCh:
		if addr != privateAddr {
			t.Fatalf("Start() listen addr = %q, want %q", addr, privateAddr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Start() should invoke listenAndServeFn")
	}

	if externalMaxConns != 1 {
		t.Fatalf("externalMaxConns = %d, want 1", externalMaxConns)
	}
}
