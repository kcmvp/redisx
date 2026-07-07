package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/kcmvp/respx/storage"
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
	addr    string
	db      storage.DB
	schemas []storage.Schema

	origInternalAuthKey  string
	origAuthKey          string
	origExternalMaxConns int
	origListenAndServeFn func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error
	origOsExitFn         func(code int)
}

func (s *ServerTestSuite) SetupSuite() {
	s.schemas = []storage.Schema{
		storage.JsonSchema("user_idx", 0).PrefixAttr("id").Index("age"),
		storage.JsonSchema("user_key", 0).PrefixAttr("id"),
		storage.JsonSchema("user_no_idx", 0).PrefixAttr("id"),
	}

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
	_ = Start(addr, 1, false)
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

	db := Start("127.0.0.1:0", 0, true, s.schemas...)
	s.Nil(db)
	s.True(exitCalled)

	// Reset srvOnce for subsequent tests
	globalMu.Lock()
	srvOnce = sync.Once{}
	globalMu.Unlock()
}

func (s *ServerTestSuite) TestStartListenAndServeFailure() {
	exitCalled := false
	globalMu.Lock()
	osExitFn = func(code int) {
		exitCalled = true
	}
	// Force listenAndServeFn to return an unexpected error
	listenAndServeFn = func(addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error {
		return errors.New("mock listen error")
	}
	globalMu.Unlock()

	db := Start("", 0, false, s.schemas...)
	s.NotNil(db) // DB opens successfully, but background listen fails

	// Give the goroutine time to run and fail
	time.Sleep(50 * time.Millisecond)

	s.True(exitCalled)

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
	cmd := redcon.Command{Args: [][]byte{[]byte("PING")}}
	handleCommand(conn, cmd, nil, &ps)
	s.Equal("ERR storage not initialized", conn.lastErr)
}

func (s *ServerTestSuite) TestCommandTable() {
	t := s.T()
	s.addr = getFreePort()
	s.db = Start(s.addr, 100, false, s.schemas...)

	tests := []struct {
		name        string
		auth        bool
		commands    [][]string
		wantStrings []string
		wantBulks   []string
		wantInts    []int
		wantNulls   int
		wantErrors  []string
		wantArrays  [][]string
		wantClosed  bool
		dbInit      bool
		schemas     []storage.Schema
		setupDB     func(uid string)
	}{
		{
			name:       "unauthenticated write command",
			auth:       false,
			commands:   [][]string{{"SET", "k_{id}", "v"}},
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
				{"SET", "name_{id}", "alice"},
				{"GET", "name_{id}"},
				{"SETNX", "name_{id}", "bob"},
				{"DEL", "name_{id}"},
				{"GET", "name_{id}"},
			},
			wantStrings: []string{"OK"},
			wantBulks:   []string{"alice"},
			wantInts:    []int{0, 1},
			wantNulls:   1,
		},
		{
			name:      "get non-existent",
			auth:      true,
			commands:  [][]string{{"GET", "nonexistent_{id}"}},
			wantNulls: 1,
		},
		{
			name:        "keys",
			auth:        true,
			commands:    [][]string{{"SET", "{id}_key1", "val1"}, {"SET", "{id}_key2", "val2"}, {"KEYS", "{id}_key*"}},
			wantStrings: []string{"OK", "OK"},
			wantArrays:  [][]string{{"{id}_key1", "{id}_key2"}},
		},
		{
			name:       "keys_not_found",
			auth:       true,
			commands:   [][]string{{"KEYS", "nonexistent*"}},
			wantArrays: [][]string{{}},
		},
		{
			name:     "del non-existent",
			auth:     true,
			commands: [][]string{{"DEL", "nonexistent_{id}"}},
			wantInts: []int{0},
		},
		{
			name:        "setnx exists",
			auth:        true,
			commands:    [][]string{{"SET", "k_{id}", "v"}, {"SETNX", "k_{id}", "v2"}},
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
			name:        "set with EX",
			auth:        true,
			commands:    [][]string{{"set", "k2_{id}", "v2", "EX", "1"}},
			wantStrings: []string{"OK"},
		},
		{
			name:        "set with PX",
			auth:        true,
			commands:    [][]string{{"set", "k3_{id}", "v3", "PX", "500"}},
			wantStrings: []string{"OK"},
		},
		{
			name:        "setex command",
			auth:        true,
			commands:    [][]string{{"setex", "k4_{id}", "1", "v4"}},
			wantStrings: []string{"OK"},
		},
		{
			name:       "wrong number of args setex",
			auth:       true,
			commands:   [][]string{{"setex", "k_{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'setex' command"},
		},
		{
			name:       "setex invalid ttl",
			auth:       true,
			commands:   [][]string{{"setex", "k_{id}", "abc", "v"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "set wrong number of args",
			auth:       true,
			commands:   [][]string{{"set", "k_{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'set' command"},
		},
		{
			name:       "set EX invalid integer",
			auth:       true,
			commands:   [][]string{{"set", "k_{id}", "v", "EX", "abc"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "set PX invalid integer",
			auth:       true,
			commands:   [][]string{{"set", "k_{id}", "v", "PX", "abc"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "get wrong number of args",
			auth:       true,
			commands:   [][]string{{"get"}},
			wantErrors: []string{"ERR wrong number of arguments for 'get' command"},
		},
		{
			name:       "setnx wrong number of args",
			auth:       true,
			commands:   [][]string{{"setnx", "k_{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'setnx' command"},
		},
		{
			name:       "del wrong number of args",
			auth:       true,
			commands:   [][]string{{"del"}},
			wantErrors: []string{"ERR wrong number of arguments for 'del' command"},
		},
		{
			name:       "keys wrong number of args",
			auth:       true,
			commands:   [][]string{{"keys"}},
			wantErrors: []string{"ERR wrong number of arguments for 'keys' command"},
		},
		{
			name:       "publish wrong number of args",
			auth:       true,
			commands:   [][]string{{"publish", "topic"}},
			wantErrors: []string{"ERR wrong number of arguments for 'publish' command"},
		},
		{
			name:       "subscribe wrong number of args",
			auth:       true,
			commands:   [][]string{{"subscribe"}},
			wantErrors: []string{"ERR wrong number of arguments for 'subscribe' command"},
		},
		{
			name:       "byindex wrong number of args",
			auth:       true,
			commands:   [][]string{{"byindex", "user", "age"}},
			wantErrors: []string{"ERR wrong number of arguments for 'byindex' command"},
		},
		{
			name:       "byindex invalid order",
			auth:       true,
			commands:   [][]string{{"byindex", "user", "age", "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "byindex invalid json",
			auth:       true,
			commands:   [][]string{{"byindex", "user", "age", "{invalid"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "bykey wrong number of args",
			auth:       true,
			commands:   [][]string{{"bykey", "user", "*"}},
			wantErrors: []string{"ERR wrong number of arguments for 'bykey' command"},
		},
		{
			name:       "bykey invalid order",
			auth:       true,
			commands:   [][]string{{"bykey", "user", "*", "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "bykey invalid json",
			auth:       true,
			commands:   [][]string{{"bykey", "user", "*", "{invalid"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "wrong number of args set",
			auth:       true,
			commands:   [][]string{{"SET", "k_{id}"}},
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
			commands:   [][]string{{"SETNX", "k_{id}"}},
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
			name:       "byindex wrong number of args",
			auth:       true,
			commands:   [][]string{{"byindex", "schema", "attr"}},
			wantErrors: []string{"ERR wrong number of arguments for 'byindex' command"},
		},
		{
			name:       "byindex invalid order",
			auth:       true,
			commands:   [][]string{{"byindex", "schema", "attr", "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "byindex invalid json",
			auth:       true,
			commands:   [][]string{{"byindex", "schema", "attr", "{invalid}", "ASC"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "bykey wrong number of args",
			auth:       true,
			commands:   [][]string{{"bykey", "schema", "pattern"}},
			wantErrors: []string{"ERR wrong number of arguments for 'bykey' command"},
		},
		{
			name:       "bykey invalid order",
			auth:       true,
			commands:   [][]string{{"bykey", "schema", "pattern", "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "bykey invalid json",
			auth:       true,
			commands:   [][]string{{"bykey", "schema", "pattern", "{invalid}", "ASC"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
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
		{
			name:       "set with EX invalid value",
			auth:       true,
			commands:   [][]string{{"set", "k_{id}", "v", "EX", "notanumber"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "set with PX invalid value",
			auth:       true,
			commands:   [][]string{{"set", "k_{id}", "v", "PX", "notanumber"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "setex command invalid time",
			auth:       true,
			commands:   [][]string{{"setex", "k_{id}", "notanumber", "v"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "byindex success",
			auth:       true,
			commands:   [][]string{{"byindex", "user_idx", "age", "{}", "ASC"}},
			wantArrays: [][]string{{`{"id":"1_{id}", "age":20}`, `{"id":"2_{id}", "age":30}`}},
			setupDB: func(uid string) {

				_ = s.db.Save(s.schemas[0], fmt.Sprintf(`{"id":"1_%s", "age":20}`, uid))
				_ = s.db.Save(s.schemas[0], fmt.Sprintf(`{"id":"2_%s", "age":30}`, uid))
			},
		},
		{
			name:       "byindex not found",
			auth:       true,
			commands:   [][]string{{"byindex", "user_no_idx", "unknown", "{}", "ASC"}},
			wantErrors: []string{"ERR index unknown not found for schema user_no_idx"},
			setupDB: func(uid string) {
			},
		},
		{
			name:       "bykey success",
			auth:       true,
			commands:   [][]string{{"bykey", "user_key", "*_{id}", "{}", "DESC"}},
			wantArrays: [][]string{{`{"id":"2_{id}"}`, `{"id":"1_{id}"}`}},
			setupDB: func(uid string) {

				_ = s.db.Save(s.schemas[1], fmt.Sprintf(`{"id":"1_%s"}`, uid))
				_ = s.db.Save(s.schemas[1], fmt.Sprintf(`{"id":"2_%s"}`, uid))
			},
		},
		{
			name:       "bykey not found",
			auth:       true,
			commands:   [][]string{{"bykey", "user_key", "unknown_{id}:*", "{}"}},
			wantArrays: [][]string{{}},
			setupDB: func(uid string) {
			},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uid := fmt.Sprintf("%d", i)
			if tc.setupDB != nil {
				tc.setupDB(uid)
			}

			conn, err := net.Dial("tcp", s.addr)
			if err != nil {
				t.Fatalf("failed to connect to server: %v", err)
			}
			defer func() { _ = conn.Close() }()

			if tc.auth {
				// We use internalAuthKey which is populated by Start
				b := []byte(fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(internalAuthKey), internalAuthKey))
				_, _ = conn.Write(b)
				buf := make([]byte, 1024)
				_, _ = conn.Read(buf)
			}

			var finalResp string
			var closed bool

			for _, args := range replaceID(tc.commands, uid) {
				var b []byte
				if len(args) == 0 {
					b = []byte("*0\r\n")
				} else {
					b = append(b, []byte(fmt.Sprintf("*%d\r\n", len(args)))...)
					for _, arg := range args {
						b = append(b, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))...)
					}
				}
				_, err := conn.Write(b)
				if err != nil {
					closed = true
					break
				}

				_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
				buf := make([]byte, 4096)
				n, err := conn.Read(buf)
				if err != nil {
					if n > 0 {
						finalResp += string(buf[:n])
					}
					closed = true
					break
				}
				finalResp += string(buf[:n])
			}

			// Try one more read to see if it's closed
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			_, err = conn.Read(make([]byte, 1))
			if err != nil {
				if err == io.EOF || strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "closed") || strings.Contains(err.Error(), "reset by peer") {
					closed = true
				}
			}

			// Validate Errors
			for _, e := range replaceID1D(tc.wantErrors, uid) {
				if !strings.Contains(finalResp, e) {
					t.Errorf("expected error %q, got resp: %q", e, finalResp)
				}
			}

			// Validate Strings
			for _, s := range replaceID1D(tc.wantStrings, uid) {
				if !strings.Contains(finalResp, "+"+s+"\r\n") {
					t.Errorf("expected string %q, got resp: %q", s, finalResp)
				}
			}

			// Validate Bulks
			for _, b := range replaceID1D(tc.wantBulks, uid) {
				if !strings.Contains(finalResp, fmt.Sprintf("$%d\r\n%s\r\n", len(b), b)) {
					t.Errorf("expected bulk %q, got resp: %q", b, finalResp)
				}
			}

			// Validate Ints
			for _, val := range tc.wantInts {
				if !strings.Contains(finalResp, fmt.Sprintf(":%d\r\n", val)) {
					t.Errorf("expected int %d, got resp: %q", val, finalResp)
				}
			}

			// Validate Nulls
			if tc.wantNulls > 0 {
				if strings.Count(finalResp, "$-1\r\n") != tc.wantNulls {
					t.Errorf("expected %d nulls, got resp: %q", tc.wantNulls, finalResp)
				}
			}

			if replaceID(tc.wantArrays, uid) != nil {
				for _, wantArr := range replaceID(tc.wantArrays, uid) {
					// Build the expected array RESP
					var expected string
					if len(wantArr) == 0 {
						expected = "*0\r\n"
					} else {
						expected = fmt.Sprintf("*%d\r\n", len(wantArr))
						for _, item := range wantArr {
							expected += fmt.Sprintf("$%d\r\n%s\r\n", len(item), item)
						}
					}
					if !strings.Contains(finalResp, expected) {
						t.Errorf("expected array %q, got resp: %q", expected, finalResp)
					}
				}
			}

			// Validate Closed
			if closed != tc.wantClosed {
				t.Errorf("closed = %v, want %v", closed, tc.wantClosed)
			}
		})
	}
}

func (s *ServerTestSuite) TestPubSub() {
	t := s.T()
	addr := getFreePort()
	_ = Start(addr, 10, false)
	defer func() { _ = stop() }()

	// Need a small sleep to ensure server is ready
	time.Sleep(10 * time.Millisecond)

	// Dial Sub Connection
	subConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect sub: %v", err)
	}
	defer func() { _ = subConn.Close() }()

	// Auth Sub
	_, _ = fmt.Fprintf(subConn, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(internalAuthKey), internalAuthKey)
	buf := make([]byte, 1024)
	_, _ = subConn.Read(buf)

	// Subscribe
	_, _ = subConn.Write([]byte("*2\r\n$9\r\nSUBSCRIBE\r\n$7\r\ntopic-1\r\n"))
	_ = subConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	n, _ := subConn.Read(buf)
	subResp := string(buf[:n])
	if !strings.Contains(subResp, "subscribe") || !strings.Contains(subResp, "topic-1") {
		t.Fatalf("expected subscribe response, got: %s", subResp)
	}

	// Dial Pub Connection
	pubConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect pub: %v", err)
	}
	defer func() { _ = pubConn.Close() }()

	// Auth Pub
	_, _ = fmt.Fprintf(pubConn, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(internalAuthKey), internalAuthKey)
	_, _ = pubConn.Read(buf)

	// Publish (Case insensitive check as well)
	_, _ = pubConn.Write([]byte("*3\r\n$7\r\npUbLiSh\r\n$7\r\ntopic-1\r\n$7\r\npayload\r\n"))
	_ = pubConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	n, _ = pubConn.Read(buf)
	pubResp := string(buf[:n])

	// Expected response for publish is integer 1 (1 subscriber)
	if !strings.Contains(pubResp, ":1\r\n") {
		t.Fatalf("publish expected :1, got: %s", pubResp)
	}

	// Check if Sub connection received the message
	_ = subConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	n, _ = subConn.Read(buf)
	msgResp := string(buf[:n])
	if !strings.Contains(msgResp, "message") || !strings.Contains(msgResp, "payload") {
		t.Fatalf("expected message on sub, got: %s", msgResp)
	}
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
		_ = Start(getFreePort(), 3, false)
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
	_ = Start(startAddr, startMaxCon, false)

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

	_ = Start("", 0, false)

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

func (s *ServerTestSuite) TestParseFilter() {
	t := s.T()
	jsonRecord := `{"name": "ken", "age": 30, "status": "active", "score": 95.5}`

	tests := []struct {
		name       string
		jsonFilter string
		expectErr  bool
		expected   bool // expected result when evaluating jsonRecord
	}{
		// Empty filters
		{"Empty string", ``, false, true},
		{"Empty object", `{}`, false, true},
		{"Invalid JSON", `{invalid`, true, false},

		// Basic equality
		{"Implicit Eq string", `{"name": "ken"}`, false, true},
		{"Implicit Eq false", `{"name": "john"}`, false, false},
		{"Explicit Eq string", `{"name": {"$eq": "ken"}}`, false, true},
		{"Explicit Eq number", `{"age": {"$eq": 30}}`, false, true},

		// Other comparators
		{"Neq true", `{"name": {"$neq": "john"}}`, false, true},
		{"Neq false", `{"name": {"$neq": "ken"}}`, false, false},

		{"Gt true", `{"age": {"$gt": 20}}`, false, true},
		{"Gt false", `{"age": {"$gt": 40}}`, false, false},

		{"Gte true", `{"age": {"$gte": 30}}`, false, true},

		{"Lt true", `{"age": {"$lt": 40}}`, false, true},
		{"Lt false", `{"age": {"$lt": 20}}`, false, false},

		{"Lte true", `{"age": {"$lte": 30}}`, false, true},

		{"Contains true", `{"status": {"$contains": "act"}}`, false, true},
		{"Contains false", `{"status": {"$contains": "pen"}}`, false, false},

		{"In true", `{"status": {"$in": ["pending", "active"]}}`, false, true},
		{"In false", `{"status": {"$in": ["pending", "banned"]}}`, false, false},
		{"In not array", `{"status": {"$in": "active"}}`, true, false},

		// Logical Combinators
		{
			name:       "Implicit AND (multiple keys)",
			jsonFilter: `{"age": {"$gt": 20}, "status": "active"}`,
			expectErr:  false,
			expected:   true,
		},
		{
			name:       "Implicit AND false",
			jsonFilter: `{"age": {"$gt": 40}, "status": "active"}`,
			expectErr:  false,
			expected:   false,
		},
		{
			name:       "Explicit AND",
			jsonFilter: `{"$and": [{"age": {"$gt": 20}}, {"status": "active"}]}`,
			expectErr:  false,
			expected:   true,
		},
		{
			name:       "Explicit OR true",
			jsonFilter: `{"$or": [{"age": {"$lt": 20}}, {"status": "active"}]}`,
			expectErr:  false,
			expected:   true,
		},
		{
			name:       "Explicit OR false",
			jsonFilter: `{"$or": [{"age": {"$lt": 20}}, {"status": "pending"}]}`,
			expectErr:  false,
			expected:   false,
		},
		{
			name:       "Complex Nested",
			jsonFilter: `{"$or": [{"age": {"$lt": 20}}, {"$and": [{"age": {"$gt": 18}}, {"status": "active"}]}]}`,
			expectErr:  false,
			expected:   true,
		},

		// Error cases
		{"Root not object", `"just-string"`, true, false},
		{"Unsupported operator", `{"age": {"$unknown": 18}}`, true, false},
		{"And not array", `{"$and": {"age": 18}}`, true, false},
		{"Or not array", `{"$or": {"age": 18}}`, true, false},
		{"And element error", `{"$and": [{"age": {"$unknown": 18}}]}`, true, false},
		{"Or element error", `{"$or": [{"age": {"$unknown": 18}}]}`, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := parseFilter(tt.jsonFilter)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if filter == nil {
				if !tt.expected {
					t.Errorf("got nil filter (passes everything) but expected false")
				}
				return
			}

			result := filter.Eval(jsonRecord)
			if result != tt.expected {
				t.Errorf("expected eval result %v, got %v", tt.expected, result)
			}
		})
	}
}
