package server

import (
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

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
	originalSrvOnce := srvOnce
	originalListenAndServeFn := listenAndServeFn

	connAuthState = make(map[string]string)
	internalAuthKey = "internal-test-key"
	authKey = "external-test-key"
	externalMaxConns = 1
	srvOnce = sync.Once{}
	listenAndServeFn = redcon.ListenAndServe

	t.Cleanup(func() {
		connAuthState = originalState
		internalAuthKey = originalInternalAuthKey
		authKey = originalAuthKey
		externalMaxConns = originalExternalMaxConns
		srvOnce = originalSrvOnce
		listenAndServeFn = originalListenAndServeFn
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

func TestFallbackExternalAuthKeyShouldDifferFromInternalKey(t *testing.T) {
	resetTestState(t)

	fallbackKey := resolveExternalAuthKey(func(string) string { return "" })
	if fallbackKey == "" {
		t.Fatal("resolveExternalAuthKey() should generate a non-empty key when env is missing")
	}
	if fallbackKey == internalAuthKey {
		t.Fatal("external fallback auth key should differ from internal auth key")
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

func TestHandleCommandEmptyCommandWritesError(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30001"}
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, redcon.Command{}, items, &mu, &ps)

	if len(conn.errors) != 1 || conn.errors[0] != "ERR empty command" {
		t.Fatalf("errors = %#v, want [\"ERR empty command\"]", conn.errors)
	}
}

func TestHandleCommandRequiresAuthenticationForWriteCommands(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30002"}
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, newCommand("SET", "k", "v"), items, &mu, &ps)

	if len(conn.errors) != 1 || conn.errors[0] != "NOAUTH authentication required" {
		t.Fatalf("errors = %#v, want [\"NOAUTH authentication required\"]", conn.errors)
	}
	if !conn.closed {
		t.Fatal("connection should be closed after unauthenticated command")
	}
}

func TestHandleCommandAuthSuccessStoresInternalKey(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30003"}
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, newCommand("AUTH", authKey), items, &mu, &ps)

	if len(conn.strings) != 1 || conn.strings[0] != "OK" {
		t.Fatalf("strings = %#v, want [\"OK\"]", conn.strings)
	}
	state, ok := getConnState(conn)
	if !ok || state != authKey {
		t.Fatalf("getConnState() = (%q, %v), want (%q, true)", state, ok, authKey)
	}
}

func TestHandleCommandAuthFailureClosesConnection(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30004"}
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, newCommand("AUTH", "wrong-key"), items, &mu, &ps)

	if len(conn.errors) != 1 || conn.errors[0] != "ERR authentication failed" {
		t.Fatalf("errors = %#v, want [\"ERR authentication failed\"]", conn.errors)
	}
	if !conn.closed {
		t.Fatal("connection should be closed after invalid auth")
	}
}

func TestHandleCommandAuthFormatFailureClosesConnection(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30009"}
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, newCommand("AUTH"), items, &mu, &ps)

	if len(conn.errors) != 1 || conn.errors[0] != "ERR authentication failed" {
		t.Fatalf("errors = %#v, want [\"ERR authentication failed\"]", conn.errors)
	}
	if !conn.closed {
		t.Fatal("connection should be closed after malformed auth")
	}
}

func TestHandleCommandPingAndQuitRequireAuthentication(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30005"}
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, newCommand("PING"), items, &mu, &ps)
	if len(conn.errors) != 1 || conn.errors[0] != "NOAUTH authentication required" {
		t.Fatalf("PING errors = %#v, want [\"NOAUTH authentication required\"]", conn.errors)
	}
	if !conn.closed {
		t.Fatal("connection should be closed after unauthenticated PING")
	}

	conn2 := &mockConn{remoteAddr: "127.0.0.1:30006"}
	handleCommand(conn2, newCommand("QUIT"), items, &mu, &ps)
	if len(conn2.errors) != 1 || conn2.errors[0] != "NOAUTH authentication required" {
		t.Fatalf("QUIT errors = %#v, want [\"NOAUTH authentication required\"]", conn2.errors)
	}
	if !conn2.closed {
		t.Fatal("connection should be closed after unauthenticated QUIT")
	}
}

func TestHandleCommandSetGetSetNxAndDelLifecycle(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30006"}
	setConnectionAuthState(conn, internalAuthKey)
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, newCommand("SET", "name", "alice"), items, &mu, &ps)
	handleCommand(conn, newCommand("GET", "name"), items, &mu, &ps)
	handleCommand(conn, newCommand("SETNX", "name", "bob"), items, &mu, &ps)
	handleCommand(conn, newCommand("DEL", "name"), items, &mu, &ps)
	handleCommand(conn, newCommand("GET", "name"), items, &mu, &ps)

	if len(conn.strings) == 0 || conn.strings[0] != "OK" {
		t.Fatalf("SET response strings = %#v, want first item \"OK\"", conn.strings)
	}
	if len(conn.bulks) != 1 || string(conn.bulks[0]) != "alice" {
		t.Fatalf("GET bulk = %#v, want [[97 108 105 99 101]]", conn.bulks)
	}
	if len(conn.ints) != 2 || conn.ints[0] != 0 || conn.ints[1] != 1 {
		t.Fatalf("ints = %#v, want [0 1]", conn.ints)
	}
	if conn.nulls != 1 {
		t.Fatalf("nulls = %d, want 1", conn.nulls)
	}
}

func TestHandleCommandUnknownCommandWritesError(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30007"}
	setConnectionAuthState(conn, internalAuthKey)
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, newCommand("WHATEVER"), items, &mu, &ps)

	if len(conn.errors) != 1 || conn.errors[0] != "ERR unknown command 'WHATEVER'" {
		t.Fatalf("errors = %#v, want [\"ERR unknown command 'WHATEVER'\"]", conn.errors)
	}
}

func TestHandleCommandWrongNumberOfArgsErrors(t *testing.T) {
	resetTestState(t)

	tests := []struct {
		name string
		cmd  redcon.Command
	}{
		{name: "set", cmd: newCommand("SET", "k")},
		{name: "get", cmd: newCommand("GET")},
		{name: "setnx", cmd: newCommand("SETNX", "k")},
		{name: "del", cmd: newCommand("DEL")},
		{name: "publish", cmd: newCommand("PUBLISH", "topic")},
		{name: "subscribe", cmd: newCommand("SUBSCRIBE")},
		{name: "psubscribe", cmd: newCommand("PSUBSCRIBE")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := &mockConn{remoteAddr: "127.0.0.1:30100"}
			setConnectionAuthState(conn, internalAuthKey)
			items := make(map[string][]byte)
			var mu sync.RWMutex
			var ps redcon.PubSub

			handleCommand(conn, tc.cmd, items, &mu, &ps)

			if len(conn.errors) == 0 {
				t.Fatal("expected error for wrong number of args")
			}
			if !strings.Contains(conn.errors[0], "wrong number of arguments") {
				t.Fatalf("error = %q, want contains %q", conn.errors[0], "wrong number of arguments")
			}
		})
	}
}

func TestHandleCommandPublishReturnsSubscriberCount(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30008"}
	setConnectionAuthState(conn, internalAuthKey)
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, newCommand("PUBLISH", "topic", "payload"), items, &mu, &ps)

	if len(conn.ints) != 1 || conn.ints[0] != 0 {
		t.Fatalf("ints = %#v, want [0]", conn.ints)
	}
}

func TestHandleCommandSubscribeThenPublishReturnsOneSubscriber(t *testing.T) {
	resetTestState(t)

	subConn := &mockConn{remoteAddr: "127.0.0.1:30030"}
	pubConn := &mockConn{remoteAddr: "127.0.0.1:30031"}
	setConnectionAuthState(subConn, internalAuthKey)
	setConnectionAuthState(pubConn, internalAuthKey)

	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(subConn, newCommand("SUBSCRIBE", "topic-1"), items, &mu, &ps)
	handleCommand(pubConn, newCommand("PUBLISH", "topic-1", "payload"), items, &mu, &ps)

	if len(pubConn.ints) != 1 || pubConn.ints[0] != 1 {
		t.Fatalf("publish subscriber count = %#v, want [1]", pubConn.ints)
	}
}

func TestHandleCommandCaseInsensitiveSubscribeAndPublish(t *testing.T) {
	resetTestState(t)

	subConn := &mockConn{remoteAddr: "127.0.0.1:30032"}
	pubConn := &mockConn{remoteAddr: "127.0.0.1:30033"}
	setConnectionAuthState(subConn, internalAuthKey)
	setConnectionAuthState(pubConn, internalAuthKey)

	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(subConn, newCommand("sUbScRiBe", "topic-2"), items, &mu, &ps)
	handleCommand(pubConn, newCommand("pUbLiSh", "topic-2", "payload"), items, &mu, &ps)

	if len(pubConn.ints) != 1 || pubConn.ints[0] != 1 {
		t.Fatalf("case-insensitive publish subscriber count = %#v, want [1]", pubConn.ints)
	}
}

func TestHandleCommandSubscribeAndPSubscribe(t *testing.T) {
	resetTestState(t)

	conn := &mockConn{remoteAddr: "127.0.0.1:30020"}
	setConnectionAuthState(conn, internalAuthKey)
	items := make(map[string][]byte)
	var mu sync.RWMutex
	var ps redcon.PubSub

	handleCommand(conn, newCommand("SUBSCRIBE", "topic-a", "topic-b"), items, &mu, &ps)
	handleCommand(conn, newCommand("PSUBSCRIBE", "topic-*"), items, &mu, &ps)

	if len(conn.errors) != 0 {
		t.Fatalf("subscribe flow should not write errors, got %#v", conn.errors)
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
