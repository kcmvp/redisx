package server

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/respx/internal"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

type mockConn struct {
	remoteAddr string
	ctx        interface{}
	closed     bool
	errors     []string
	strings    []string
	bulks      [][]byte
	ints       []int
	nulls      int
}

func (m *mockConn) RemoteAddr() string             { return m.remoteAddr }
func (m *mockConn) Close() error                   { m.closed = true; return nil }
func (m *mockConn) WriteError(msg string)          { m.errors = append(m.errors, msg) }
func (m *mockConn) WriteString(str string)         { m.strings = append(m.strings, str) }
func (m *mockConn) WriteBulk(b []byte)             { m.bulks = append(m.bulks, append([]byte(nil), b...)) }
func (m *mockConn) WriteBulkString(string)         {}
func (m *mockConn) WriteInt(num int)               { m.ints = append(m.ints, num) }
func (m *mockConn) WriteInt64(int64)               {}
func (m *mockConn) WriteUint64(uint64)             {}
func (m *mockConn) WriteArray(int)                 {}
func (m *mockConn) WriteNull()                     { m.nulls++ }
func (m *mockConn) WriteRaw([]byte)                {}
func (m *mockConn) WriteAny(any)                   {}
func (m *mockConn) Context() interface{}           { return m.ctx }
func (m *mockConn) SetContext(v interface{})       { m.ctx = v }
func (m *mockConn) SetReadBuffer(int)              {}
func (m *mockConn) Detach() redcon.DetachedConn    { return &mockDetachedConn{mockConn: m} }
func (m *mockConn) ReadPipeline() []redcon.Command { return nil }
func (m *mockConn) PeekPipeline() []redcon.Command { return nil }
func (m *mockConn) NetConn() net.Conn              { return nil }

type mockDetachedConn struct {
	*mockConn
}

func (m *mockDetachedConn) ReadCommand() (redcon.Command, error) { return redcon.Command{}, nil }
func (m *mockDetachedConn) Flush() error                         { return nil }

func resetTestState(t *testing.T) {
	t.Helper()
	originalState := connAuthState
	originalInternalAuthKey := internalAuthKey
	originalAuthKey := authKey
	originalExternalMaxConns := externalMaxConns
	originalListenAndServeFn := listenAndServeFn
	originalOsExitFn := osExitFn
	originalUserHomeDirFn := userHomeDirFn
	originalSrvOnce := srvOnce

	globalMu.Lock()
	connAuthState = make(map[string]string)
	internalAuthKey = "internal-test-key"
	authKey = "external-test-key"
	externalMaxConns = 1
	srvOnce = sync.Once{}
	listenAndServeFn = redcon.ListenAndServe
	globalMu.Unlock()

	t.Cleanup(func() {
		globalMu.Lock()
		connAuthState = originalState
		internalAuthKey = originalInternalAuthKey
		authKey = originalAuthKey
		externalMaxConns = originalExternalMaxConns
		listenAndServeFn = originalListenAndServeFn
		osExitFn = originalOsExitFn
		userHomeDirFn = originalUserHomeDirFn
		srvOnce = originalSrvOnce
		globalMu.Unlock()
	})
}

func newCommand(args ...string) redcon.Command {
	cmd := redcon.Command{Args: make([][]byte, len(args))}
	for i, arg := range args {
		cmd.Args[i] = []byte(arg)
	}
	return cmd
}

func TestGetConnKeyReturnsRemoteAddr(t *testing.T) {
	conn := &mockConn{remoteAddr: "127.0.0.1:12345"}

	if got := getConnKey(conn); got != conn.remoteAddr {
		t.Fatalf("getConnKey() = %q, want %q", got, conn.remoteAddr)
	}
}

func TestGetConnStateReturnsMissingForUnknownConnection(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:12345"}
	state, ok := getConnState(conn)
	if ok {
		t.Fatalf("getConnState() ok = true, want false, state=%q", state)
	}
}

func TestIsServerShutdownErr(t *testing.T) {
	if !isServerShutdownErr(nil) {
		t.Fatal("isServerShutdownErr(nil) = false, want true")
	}

	if !isServerShutdownErr(net.ErrClosed) {
		t.Fatal("isServerShutdownErr(net.ErrClosed) = false, want true")
	}

	errUseOfClosed := errors.New("use of closed network connection")
	if !isServerShutdownErr(errUseOfClosed) {
		t.Fatal("isServerShutdownErr(use of closed...) = false, want true")
	}

	if isServerShutdownErr(errors.New("other error")) {
		t.Fatal("isServerShutdownErr(other error) = true, want false")
	}
}

func TestListenAndServeWithStopError(t *testing.T) {
	// Provide a bad address to cause net.Listen to fail
	err := listenAndServeWithStop("invalid-address",
		func(conn redcon.Conn, cmd redcon.Command) {},
		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)

	if err == nil {
		t.Fatal("expected error from net.Listen")
	}
}

func TestListenAndServeWithStop(t *testing.T) {
	addr := "127.0.0.1:0" // let OS pick a free port
	errCh := make(chan error, 1)

	go func() {
		errCh <- listenAndServeWithStop(addr,
			func(conn redcon.Conn, cmd redcon.Command) {},
			func(conn redcon.Conn) bool { return true },
			func(conn redcon.Conn, err error) {},
		)
	}()

	// wait for listener to be set
	time.Sleep(100 * time.Millisecond)

	serverMu.Lock()
	ln := serverListener
	serverMu.Unlock()

	if ln == nil {
		t.Fatal("serverListener is nil, expected active listener")
	}

	// Close listener to stop server
	_ = ln.Close()

	select {
	case err := <-errCh:
		if !isServerShutdownErr(err) {
			t.Fatalf("expected shutdown error, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for server to stop")
	}
}

func TestResolveExternalAuthKeyWithEnv(t *testing.T) {
	envKey := "env-test-key"
	key := resolveExternalAuthKey(func(s string) string {
		if s == internal.RespxAuthKeyEnv {
			return envKey
		}
		return ""
	})
	if key != envKey {
		t.Fatalf("resolveExternalAuthKey = %q, want %q", key, envKey)
	}
}

func TestEnsureExternalAuthKey(t *testing.T) {
	resetTestState(t)

	// Test when authKey is already set
	authKey = "pre-set-key"
	ensureExternalAuthKey()
	if authKey != "pre-set-key" {
		t.Fatalf("authKey = %q, want %q", authKey, "pre-set-key")
	}

	// Test when authKey is empty and env is missing
	authKey = ""
	ensureExternalAuthKey()
	if authKey == "" {
		t.Fatal("authKey should be generated if empty and env is missing")
	}

	// Test when authKey is empty and env is present
	authKey = ""
	_ = os.Setenv(internal.RespxAuthKeyEnv, "env-auth-key")
	t.Cleanup(func() { _ = os.Unsetenv(internal.RespxAuthKeyEnv) })
	ensureExternalAuthKey()
	if authKey != "env-auth-key" {
		t.Fatalf("authKey = %q, want %q", authKey, "env-auth-key")
	}
}

func TestAuthConnectionStoresState(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:12345"}
	setConnectionAuthState(conn, internalAuthKey)

	state, ok := getConnState(conn)
	if !ok {
		t.Fatal("getConnState() ok = false, want true")
	}
	if state != internalAuthKey {
		t.Fatalf("getConnState() = %q, want %q", state, internalAuthKey)
	}
}

func TestAuthConnectionOverwritesPreviousState(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:12345"}
	setConnectionAuthState(conn, "")
	setConnectionAuthState(conn, internalAuthKey)

	state, ok := getConnState(conn)
	if !ok || state != internalAuthKey {
		t.Fatalf("getConnState() = (%q, %v), want (%q, true)", state, ok, internalAuthKey)
	}
}

func TestDeauthConnectionRemovesState(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:12345"}
	setConnectionAuthState(conn, internalAuthKey)
	clearConnectionAuthState(conn)

	state, ok := getConnState(conn)
	if ok {
		t.Fatalf("getConnState() after deauth = (%q, true), want missing", state)
	}
}

func TestExternalConnectionCountsOnlyNonInternalConnections(t *testing.T) {
	resetTestState(t)

	internalConn := &mockConn{remoteAddr: "127.0.0.1:10001"}
	externalConn := &mockConn{remoteAddr: "127.0.0.1:10002"}
	pendingConn := &mockConn{remoteAddr: "127.0.0.1:10003"}

	setConnectionAuthState(internalConn, internalAuthKey)
	setConnectionAuthState(externalConn, "external-key")
	setConnectionAuthState(pendingConn, "")

	if got := externalConnection(); got != 2 {
		t.Fatalf("externalConnection() = %d, want 2", got)
	}
}

func TestAcceptConnectionStoresPendingConnection(t *testing.T) {
	resetTestState(t)
	externalMaxConns = 2

	conn := &mockConn{remoteAddr: "127.0.0.1:20001"}
	if !acceptCon(conn) {
		t.Fatal("acceptConnection() = false, want true")
	}

	state, ok := getConnState(conn)
	if !ok || state != "" {
		t.Fatalf("getConnState() = (%q, %v), want (\"\", true)", state, ok)
	}
}

func TestAcceptConnectionRejectsWhenAtExternalLimit(t *testing.T) {
	resetTestState(t)
	externalMaxConns = 1

	first := &mockConn{remoteAddr: "127.0.0.1:20001"}
	second := &mockConn{remoteAddr: "127.0.0.1:20002"}

	if !acceptCon(first) {
		t.Fatal("first acceptConnection() = false, want true")
	}
	if acceptCon(second) {
		t.Fatal("second acceptConnection() = true, want false")
	}

	if _, ok := getConnState(second); ok {
		t.Fatal("rejected connection should not be stored in connAuthState")
	}
}

func TestAcceptConnectionRejectDoesNotIncreaseExternalCount(t *testing.T) {
	resetTestState(t)
	externalMaxConns = 1

	first := &mockConn{remoteAddr: "127.0.0.1:20001"}
	second := &mockConn{remoteAddr: "127.0.0.1:20002"}

	if !acceptCon(first) {
		t.Fatal("first acceptConnection() = false, want true")
	}
	if got := externalConnection(); got != 1 {
		t.Fatalf("externalConnection() after first accept = %d, want 1", got)
	}

	if acceptCon(second) {
		t.Fatal("second acceptConnection() = true, want false")
	}
	if got := externalConnection(); got != 1 {
		t.Fatalf("externalConnection() after rejected accept = %d, want 1", got)
	}
}

func TestAcceptConnectionAllowsNewConnectionAfterCloseFreesSlot(t *testing.T) {
	resetTestState(t)
	externalMaxConns = 1

	first := &mockConn{remoteAddr: "127.0.0.1:20011"}
	second := &mockConn{remoteAddr: "127.0.0.1:20012"}

	if !acceptCon(first) {
		t.Fatal("first acceptConnection() = false, want true")
	}
	if acceptCon(second) {
		t.Fatal("second acceptConnection() = true, want false")
	}

	closeCon(first, nil)

	if got := externalConnection(); got != 0 {
		t.Fatalf("externalConnection() after close = %d, want 0", got)
	}

	if !acceptCon(second) {
		t.Fatal("second acceptConnection() after close = false, want true")
	}
	if got := externalConnection(); got != 1 {
		t.Fatalf("externalConnection() after reaccept = %d, want 1", got)
	}
}

func TestAcceptConnectionIgnoresInternalConnectionForExternalLimit(t *testing.T) {
	resetTestState(t)
	externalMaxConns = 1

	internalConn := &mockConn{remoteAddr: "127.0.0.1:20010"}
	setConnectionAuthState(internalConn, internalAuthKey)

	externalConn := &mockConn{remoteAddr: "127.0.0.1:20011"}
	if !acceptCon(externalConn) {
		t.Fatal("acceptConnection() should allow one external connection when only internal connection exists")
	}
}

func TestCloseConnectionRemovesStateAndKeepsCountUpdated(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:20001"}
	setConnectionAuthState(conn, "")
	if got := externalConnection(); got != 1 {
		t.Fatalf("externalConnection() before close = %d, want 1", got)
	}

	closeCon(conn, nil)

	if _, ok := getConnState(conn); ok {
		t.Fatal("connection state should be removed after closeConnection")
	}
	if got := externalConnection(); got != 0 {
		t.Fatalf("externalConnection() after close = %d, want 0", got)
	}
}

func TestCloseConnectionWithErrorAlsoRemovesState(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:20012"}
	setConnectionAuthState(conn, "")

	closeCon(conn, errors.New("network error"))

	if _, ok := getConnState(conn); ok {
		t.Fatal("connection state should be removed after closeConnection(err)")
	}
}

func TestHandleCommand(t *testing.T) {
	resetTestState(t)

	tests := []struct {
		name        string
		auth        bool
		commands    [][]string
		wantStrings []string
		wantBulks   []string
		wantInts    []int
		wantNulls   int
		wantErrors  []string
		wantClosed  bool
	}{
		{
			name:       "empty command",
			auth:       true,
			commands:   [][]string{{}},
			wantErrors: []string{"ERR empty command"},
		},
		{
			name:       "unauthenticated write command",
			auth:       false,
			commands:   [][]string{{"SET", "k", "v"}},
			wantErrors: []string{"NOAUTH authentication required"},
			wantClosed: true,
		},
		{
			name:        "auth success stores internal key",
			auth:        false,
			commands:    [][]string{{"AUTH", internalAuthKey}},
			wantStrings: []string{"OK"},
		},
		{
			name:       "auth failure closes connection",
			auth:       false,
			commands:   [][]string{{"AUTH", "wrong-key"}},
			wantErrors: []string{"ERR authentication failed"},
			wantClosed: true,
		},
		{
			name:       "auth format failure closes connection",
			auth:       false,
			commands:   [][]string{{"AUTH"}},
			wantErrors: []string{"ERR authentication failed"},
			wantClosed: true,
		},
		{
			name:        "hello command",
			auth:        false,
			commands:    [][]string{{"HELLO"}},
			wantStrings: []string{}, // WriteAny doesn't populate strings/bulks in mockConn currently
		},
		{
			name:        "client command",
			auth:        false,
			commands:    [][]string{{"CLIENT"}},
			wantStrings: []string{"OK"},
		},
		{
			name:       "ping requires authentication",
			auth:       false,
			commands:   [][]string{{"PING"}},
			wantErrors: []string{"NOAUTH authentication required"},
			wantClosed: true,
		},
		{
			name:        "ping authenticated",
			auth:        true,
			commands:    [][]string{{"PING"}},
			wantStrings: []string{"PONG"},
		},
		{
			name:       "quit requires authentication",
			auth:       false,
			commands:   [][]string{{"QUIT"}},
			wantErrors: []string{"NOAUTH authentication required"},
			wantClosed: true,
		},
		{
			name:        "quit authenticated",
			auth:        true,
			commands:    [][]string{{"QUIT"}},
			wantStrings: []string{"OK"},
			wantClosed:  true,
		},
		{
			name: "set get setnx and del lifecycle",
			auth: true,
			commands: [][]string{
				{"SET", "name", "alice"},
				{"GET", "name"},
				{"SETNX", "name", "bob"},
				{"DEL", "name"},
				{"GET", "name"},
			},
			wantStrings: []string{"OK"},
			wantBulks:   []string{"alice"},
			wantInts:    []int{0, 1},
			wantNulls:   1,
		},
		{
			name:      "get non-existent",
			auth:      true,
			commands:  [][]string{{"GET", "nonexistent"}},
			wantNulls: 1,
		},
		{
			name:     "del non-existent",
			auth:     true,
			commands: [][]string{{"DEL", "nonexistent"}},
			wantInts: []int{0},
		},
		{
			name:        "setnx exists",
			auth:        true,
			commands:    [][]string{{"SET", "k", "v"}, {"SETNX", "k", "v2"}},
			wantStrings: []string{"OK"},
			wantInts:    []int{0},
		},
		{
			name:       "unknown command writes error",
			auth:       true,
			commands:   [][]string{{"WHATEVER"}},
			wantErrors: []string{"ERR unknown command 'WHATEVER'"},
		},
		{
			name:       "wrong number of args set",
			auth:       true,
			commands:   [][]string{{"SET", "k"}},
			wantErrors: []string{"ERR wrong number of arguments for 'SET' command"},
		},
		{
			name:       "wrong number of args get",
			auth:       true,
			commands:   [][]string{{"GET"}},
			wantErrors: []string{"ERR wrong number of arguments for 'GET' command"},
		},
		{
			name:       "wrong number of args setnx",
			auth:       true,
			commands:   [][]string{{"SETNX", "k"}},
			wantErrors: []string{"ERR wrong number of arguments for 'SETNX' command"},
		},
		{
			name:       "wrong number of args del",
			auth:       true,
			commands:   [][]string{{"DEL"}},
			wantErrors: []string{"ERR wrong number of arguments for 'DEL' command"},
		},
		{
			name:       "wrong number of args publish",
			auth:       true,
			commands:   [][]string{{"PUBLISH", "topic"}},
			wantErrors: []string{"ERR wrong number of arguments for 'PUBLISH' command"},
		},
		{
			name:       "wrong number of args subscribe",
			auth:       true,
			commands:   [][]string{{"SUBSCRIBE"}},
			wantErrors: []string{"ERR wrong number of arguments for 'SUBSCRIBE' command"},
		},
		{
			name:       "wrong number of args psubscribe",
			auth:       true,
			commands:   [][]string{{"PSUBSCRIBE"}},
			wantErrors: []string{"ERR wrong number of arguments for 'PSUBSCRIBE' command"},
		},
		{
			name:     "publish returns subscriber count",
			auth:     true,
			commands: [][]string{{"PUBLISH", "topic", "payload"}},
			wantInts: []int{0},
		},
		{
			name:     "subscribe and psubscribe",
			auth:     true,
			commands: [][]string{{"SUBSCRIBE", "topic-a", "topic-b"}, {"PSUBSCRIBE", "topic-*"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := buntdb.Open(":memory:")
			if err != nil {
				t.Fatalf("failed to open buntdb: %v", err)
			}
			defer func() { _ = db.Close() }()

			conn := &mockConn{remoteAddr: "127.0.0.1:30000"}
			if tc.auth {
				setConnectionAuthState(conn, internalAuthKey)
			} else {
				clearConnectionAuthState(conn)
			}

			var ps redcon.PubSub

			for _, args := range tc.commands {
				var cmd redcon.Command
				if len(args) > 0 {
					cmd = newCommand(args...)
				}
				handleCommand(conn, cmd, db, &ps)
			}

			// Validate Errors
			if len(tc.wantErrors) > 0 {
				if len(conn.errors) != len(tc.wantErrors) {
					t.Errorf("errors = %v, want %v", conn.errors, tc.wantErrors)
				} else {
					for i, e := range tc.wantErrors {
						if conn.errors[i] != e {
							t.Errorf("error[%d] = %q, want %q", i, conn.errors[i], e)
						}
					}
				}
			} else if len(conn.errors) > 0 {
				t.Errorf("unexpected errors: %v", conn.errors)
			}

			// Validate Strings
			if len(tc.wantStrings) > 0 {
				if len(conn.strings) != len(tc.wantStrings) {
					t.Errorf("strings = %v, want %v", conn.strings, tc.wantStrings)
				} else {
					for i, s := range tc.wantStrings {
						if conn.strings[i] != s {
							t.Errorf("string[%d] = %q, want %q", i, conn.strings[i], s)
						}
					}
				}
			}

			// Validate Bulks
			if len(tc.wantBulks) > 0 {
				if len(conn.bulks) != len(tc.wantBulks) {
					t.Errorf("bulks length = %d, want %d", len(conn.bulks), len(tc.wantBulks))
				} else {
					for i, b := range tc.wantBulks {
						if string(conn.bulks[i]) != b {
							t.Errorf("bulk[%d] = %q, want %q", i, string(conn.bulks[i]), b)
						}
					}
				}
			}

			// Validate Ints
			if len(tc.wantInts) > 0 {
				if len(conn.ints) != len(tc.wantInts) {
					t.Errorf("ints = %v, want %v", conn.ints, tc.wantInts)
				} else {
					for i, val := range tc.wantInts {
						if conn.ints[i] != val {
							t.Errorf("int[%d] = %d, want %d", i, conn.ints[i], val)
						}
					}
				}
			}

			// Validate Nulls
			if conn.nulls != tc.wantNulls {
				t.Errorf("nulls = %d, want %d", conn.nulls, tc.wantNulls)
			}

			// Validate Closed
			if conn.closed != tc.wantClosed {
				t.Errorf("closed = %v, want %v", conn.closed, tc.wantClosed)
			}
		})
	}
}

func TestHandleCommandSubscribeThenPublishReturnsOneSubscriber(t *testing.T) {
	resetTestState(t)

	db, _ := buntdb.Open(":memory:")
	defer func() { _ = db.Close() }()

	subConn := &mockConn{remoteAddr: "127.0.0.1:30030"}
	pubConn := &mockConn{remoteAddr: "127.0.0.1:30031"}
	setConnectionAuthState(subConn, internalAuthKey)
	setConnectionAuthState(pubConn, internalAuthKey)

	var ps redcon.PubSub

	handleCommand(subConn, newCommand("SUBSCRIBE", "topic-1"), db, &ps)
	handleCommand(pubConn, newCommand("PUBLISH", "topic-1", "payload"), db, &ps)

	if len(pubConn.ints) != 1 || pubConn.ints[0] != 1 {
		t.Fatalf("publish subscriber count = %#v, want [1]", pubConn.ints)
	}
}

func TestHandleCommandCaseInsensitiveSubscribeAndPublish(t *testing.T) {
	resetTestState(t)

	db, _ := buntdb.Open(":memory:")
	defer func() { _ = db.Close() }()

	subConn := &mockConn{remoteAddr: "127.0.0.1:30032"}
	pubConn := &mockConn{remoteAddr: "127.0.0.1:30033"}
	setConnectionAuthState(subConn, internalAuthKey)
	setConnectionAuthState(pubConn, internalAuthKey)

	var ps redcon.PubSub

	handleCommand(subConn, newCommand("sUbScRiBe", "topic-2"), db, &ps)
	handleCommand(pubConn, newCommand("pUbLiSh", "topic-2", "payload"), db, &ps)

	if len(pubConn.ints) != 1 || pubConn.ints[0] != 1 {
		t.Fatalf("case-insensitive publish subscriber count = %#v, want [1]", pubConn.ints)
	}
}

func TestHandleCommandDbNotInitializedWritesError(t *testing.T) {
	conn := &mockConn{remoteAddr: "127.0.0.1:30007"}
	var ps redcon.PubSub

	handleCommand(conn, newCommand("GET", "k"), nil, &ps)

	if len(conn.errors) != 1 || conn.errors[0] != "ERR storage not initialized" {
		t.Fatalf("errors = %#v, want [\"ERR storage not initialized\"]", conn.errors)
	}
}

func TestHandleShutdownSignalsWarnsOnError(t *testing.T) {
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

func TestStopWithStorageCloseError(t *testing.T) {
	resetTestState(t)

	db, err := buntdb.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open buntdb: %v", err)
	}

	// We can cause Close to return an error by closing it first
	_ = db.Close()
	storage = db

	// Set srvOnce so stop() will actually do cleanup
	srvOnce.Do(func() {})

	err = stop()
	if err == nil {
		t.Error("expected stop() to return an error from storage.Close()")
	}

	if storage != nil {
		t.Error("expected storage to be nil even if Close() failed")
	}
}

func TestStop(t *testing.T) {
	resetTestState(t)

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

	db, err := buntdb.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open buntdb: %v", err)
	}
	storage = db

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

	if storage != nil {
		t.Errorf("expected storage to be nil")
	}
}

func TestStartDB(t *testing.T) {
	resetTestState(t)

	tmpDir := t.TempDir()
	userHomeDirFn = func() (string, error) {
		return tmpDir, nil
	}

	startDB()

	if storage == nil {
		t.Fatal("expected storage to be initialized")
	}
	defer func() { _ = storage.Close() }()

	// Verify directory was created
	_, err := os.Stat(filepath.Join(tmpDir, ".respx"))
	if os.IsNotExist(err) {
		t.Fatal("expected .respx directory to be created")
	}
}

func TestStartDBHomeDirError(t *testing.T) {
	resetTestState(t)

	globalMu.Lock()
	userHomeDirFn = func() (string, error) {
		return "", errors.New("simulated home dir error")
	}

	exitCalled := false
	osExitFn = func(code int) {
		exitCalled = true
		if code != 1 {
			t.Errorf("osExitFn called with code %d, want 1", code)
		}
	}
	globalMu.Unlock()

	startDB()

	if !exitCalled {
		t.Error("expected osExitFn to be called on home dir error")
	}
}

func TestStartDBMkdirError(t *testing.T) {
	resetTestState(t)

	tmpDir := t.TempDir()
	// Create a file where the directory should be, to cause MkdirAll to fail
	badPath := filepath.Join(tmpDir, ".respx")
	err := os.WriteFile(badPath, []byte("file instead of dir"), 0o644)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	globalMu.Lock()
	userHomeDirFn = func() (string, error) {
		return tmpDir, nil
	}

	exitCalled := false
	osExitFn = func(code int) {
		exitCalled = true
		if code != 1 {
			t.Errorf("osExitFn called with code %d, want 1", code)
		}
	}
	globalMu.Unlock()

	startDB()

	if !exitCalled {
		t.Error("expected osExitFn to be called on mkdir error")
	}
}

func TestStartDBOpenError(t *testing.T) {
	resetTestState(t)

	tmpDir := t.TempDir()

	globalMu.Lock()
	userHomeDirFn = func() (string, error) {
		return tmpDir, nil
	}
	globalMu.Unlock()

	// Create a directory where the db file should be, to cause Open to fail
	respxDir := filepath.Join(tmpDir, ".respx")
	err := os.MkdirAll(respxDir, 0o755)
	if err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	dbPath := filepath.Join(respxDir, "data.db")
	err = os.MkdirAll(dbPath, 0o755)
	if err != nil {
		t.Fatalf("failed to create dir for db path: %v", err)
	}

	globalMu.Lock()
	exitCalled := false
	osExitFn = func(code int) {
		exitCalled = true
		if code != 1 {
			t.Errorf("osExitFn called with code %d, want 1", code)
		}
	}
	globalMu.Unlock()

	startDB()

	if !exitCalled {
		t.Error("expected osExitFn to be called on buntdb.Open error")
	}
}

func TestStartListenError(t *testing.T) {
	resetTestState(t)

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
		Start("127.0.0.1:16380", 3)
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

func TestStartInvokesListenerAndSetsMaxConnections(t *testing.T) {
	resetTestState(t)

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
	Start(startAddr, startMaxCon)

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

func TestStartUsesPrivateAddrAndMinimumMaxConn(t *testing.T) {
	resetTestState(t)

	listenCalledCh := make(chan string, 1)
	listenAndServeFn = func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
		select {
		case listenCalledCh <- addr:
		default:
		}
		return nil
	}

	Start("", 0)

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
