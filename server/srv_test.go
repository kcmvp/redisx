package server

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

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
			name:       "ping requires authentication",
			auth:       false,
			commands:   [][]string{{"PING"}},
			wantErrors: []string{"NOAUTH authentication required"},
			wantClosed: true,
		},
		{
			name:       "quit requires authentication",
			auth:       false,
			commands:   [][]string{{"QUIT"}},
			wantErrors: []string{"NOAUTH authentication required"},
			wantClosed: true,
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
			defer db.Close()

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
	defer db.Close()

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
	defer db.Close()

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

func TestHandleCommandSubscribeAndPSubscribe(t *testing.T) {
	resetTestState(t)

	db, _ := buntdb.Open(":memory:")
	defer db.Close()

	conn := &mockConn{remoteAddr: "127.0.0.1:30020"}
	setConnectionAuthState(conn, internalAuthKey)

	var ps redcon.PubSub

	handleCommand(conn, newCommand("SUBSCRIBE", "topic-a", "topic-b"), db, &ps)
	handleCommand(conn, newCommand("PSUBSCRIBE", "topic-*"), db, &ps)

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
