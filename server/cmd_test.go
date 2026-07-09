package server

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/kcmvp/indx/storage"

	"github.com/stretchr/testify/suite"
	"github.com/tidwall/redcon"
	"sync"
)

type CmdTestSuite struct {
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

func (s *CmdTestSuite) SetupSuite() {
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

func (s *CmdTestSuite) SetupTest() {
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

func (s *CmdTestSuite) TearDownTest() {
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

func TestCmdSuite(t *testing.T) {
	suite.Run(t, new(CmdTestSuite))
}

func (s *CmdTestSuite) TestCmd() {
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
			commands:   [][]string{{cmdSet, "k_{id}", "v"}},
			wantErrors: []string{"NOAUTH authentication required"},
			wantClosed: true,
		},
		{
			name:        "auth success stores internal key",
			auth:        false,
			commands:    [][]string{{cmdAuth, internalAuthKey}},
			wantStrings: []string{"OK"},
		},
		{
			name:       "auth failure closes connection",
			auth:       false,
			commands:   [][]string{{cmdAuth, "wrong-key"}},
			wantErrors: []string{"ERR authentication failed"},
			wantClosed: true,
		},
		{
			name:       "auth format failure closes connection",
			auth:       false,
			commands:   [][]string{{cmdAuth}},
			wantErrors: []string{"ERR authentication failed"},
			wantClosed: true,
		},
		{
			name:        "hello command",
			auth:        false,
			commands:    [][]string{{cmdHello}},
			wantStrings: []string{}, // WriteAny doesn't populate strings/bulks in mockConn currently
		},
		{
			name:        "client command",
			auth:        false,
			commands:    [][]string{{cmdClient}},
			wantStrings: []string{"OK"},
		},
		{
			name:       "ping requires authentication",
			auth:       false,
			commands:   [][]string{{cmdPing}},
			wantErrors: []string{"NOAUTH authentication required"},
			wantClosed: true,
		},
		{
			name:        "ping authenticated",
			auth:        true,
			commands:    [][]string{{cmdPing}},
			wantStrings: []string{"PONG"},
		},
		{
			name:       "quit requires authentication",
			auth:       false,
			commands:   [][]string{{cmdQuit}},
			wantErrors: []string{"NOAUTH authentication required"},
			wantClosed: true,
		},
		{
			name:        "quit authenticated",
			auth:        true,
			commands:    [][]string{{cmdQuit}},
			wantStrings: []string{"OK"},
			wantClosed:  true,
		},
		{
			name: "set get setnx and del lifecycle",
			auth: true,
			commands: [][]string{
				{cmdSet, "name_{id}", "alice"},
				{cmdGet, "name_{id}"},
				{cmdSetNX, "name_{id}", "bob"},
				{cmdDel, "name_{id}"},
				{cmdGet, "name_{id}"},
			},
			wantStrings: []string{"OK"},
			wantBulks:   []string{"alice"},
			wantInts:    []int{0, 1},
			wantNulls:   1,
		},
		{
			name:      "get non-existent",
			auth:      true,
			commands:  [][]string{{cmdGet, "nonexistent_{id}"}},
			wantNulls: 1,
		},
		{
			name:        cmdKeys,
			auth:        true,
			commands:    [][]string{{cmdSet, "{id}_key1", "val1"}, {cmdSet, "{id}_key2", "val2"}, {cmdKeys, "{id}_key*"}},
			wantStrings: []string{"OK", "OK"},
			wantArrays:  [][]string{{"{id}_key1", "{id}_key2"}},
		},
		{
			name:       "keys_not_found",
			auth:       true,
			commands:   [][]string{{cmdKeys, "nonexistent*"}},
			wantArrays: [][]string{{}},
		},
		{
			name:     "del non-existent",
			auth:     true,
			commands: [][]string{{cmdDel, "nonexistent_{id}"}},
			wantInts: []int{0},
		},
		{
			name:        "setnx exists",
			auth:        true,
			commands:    [][]string{{cmdSet, "k_{id}", "v"}, {cmdSetNX, "k_{id}", "v2"}},
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
			commands:    [][]string{{cmdSet, "k2_{id}", "v2", "EX", "1"}},
			wantStrings: []string{"OK"},
		},
		{
			name:        "set with PX",
			auth:        true,
			commands:    [][]string{{cmdSet, "k3_{id}", "v3", "PX", "500"}},
			wantStrings: []string{"OK"},
		},
		{
			name:        "setex command",
			auth:        true,
			commands:    [][]string{{cmdSetEx, "k4_{id}", "1", "v4"}},
			wantStrings: []string{"OK"},
		},
		{
			name:       "wrong number of args setex",
			auth:       true,
			commands:   [][]string{{cmdSetEx, "k_{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'setex' command"},
		},
		{
			name:       "setex invalid ttl",
			auth:       true,
			commands:   [][]string{{cmdSetEx, "k_{id}", "abc", "v"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "set wrong number of args",
			auth:       true,
			commands:   [][]string{{cmdSet, "k_{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'set' command"},
		},
		{
			name:       "set EX invalid integer",
			auth:       true,
			commands:   [][]string{{cmdSet, "k_{id}", "v", "EX", "abc"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "set PX invalid integer",
			auth:       true,
			commands:   [][]string{{cmdSet, "k_{id}", "v", "PX", "abc"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "get wrong number of args",
			auth:       true,
			commands:   [][]string{{cmdGet}},
			wantErrors: []string{"ERR wrong number of arguments for 'get' command"},
		},
		{
			name:       "setnx wrong number of args",
			auth:       true,
			commands:   [][]string{{cmdSetNX, "k_{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'setnx' command"},
		},
		{
			name:       "del wrong number of args",
			auth:       true,
			commands:   [][]string{{cmdDel}},
			wantErrors: []string{"ERR wrong number of arguments for 'del' command"},
		},
		{
			name:       "keys wrong number of args",
			auth:       true,
			commands:   [][]string{{cmdKeys}},
			wantErrors: []string{"ERR wrong number of arguments for 'keys' command"},
		},
		{
			name:       "publish wrong number of args",
			auth:       true,
			commands:   [][]string{{cmdPublish, "topic"}},
			wantErrors: []string{"ERR wrong number of arguments for 'publish' command"},
		},
		{
			name:       "subscribe wrong number of args",
			auth:       true,
			commands:   [][]string{{cmdSubscribe}},
			wantErrors: []string{"ERR wrong number of arguments for 'subscribe' command"},
		},
		{
			name:       "wrong number of args set",
			auth:       true,
			commands:   [][]string{{cmdSet, "k_{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'set' command"},
		},
		{
			name:       "wrong number of args get",
			auth:       true,
			commands:   [][]string{{cmdGet}},
			wantErrors: []string{"ERR wrong number of arguments for 'get' command"},
		},
		{
			name:       "wrong number of args setnx",
			auth:       true,
			commands:   [][]string{{cmdSetNX, "k_{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'setnx' command"},
		},
		{
			name:       "wrong number of args del",
			auth:       true,
			commands:   [][]string{{cmdDel}},
			wantErrors: []string{"ERR wrong number of arguments for 'del' command"},
		},
		{
			name:       "wrong number of args publish",
			auth:       true,
			commands:   [][]string{{cmdPublish, "topic"}},
			wantErrors: []string{"ERR wrong number of arguments for 'publish' command"},
		},
		{
			name:       "wrong number of args subscribe",
			auth:       true,
			commands:   [][]string{{cmdSubscribe}},
			wantErrors: []string{"ERR wrong number of arguments for 'subscribe' command"},
		},
		{
			name:       "wrong number of args psubscribe",
			auth:       true,
			commands:   [][]string{{cmdPSubscribe}},
			wantErrors: []string{"ERR wrong number of arguments for 'psubscribe' command"},
		},
		{
			name:     "publish returns subscriber count",
			auth:     true,
			commands: [][]string{{cmdPublish, "topic", "payload"}},
			wantInts: []int{0},
		},
		{
			name:     "subscribe and psubscribe",
			auth:     true,
			commands: [][]string{{cmdSubscribe, "topic-a", "topic-b"}, {cmdPSubscribe, "topic-*"}},
		},
		{
			name:       "set with EX invalid value",
			auth:       true,
			commands:   [][]string{{cmdSet, "k_{id}", "v", "EX", "notanumber"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "set with PX invalid value",
			auth:       true,
			commands:   [][]string{{cmdSet, "k_{id}", "v", "PX", "notanumber"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "setex command invalid time",
			auth:       true,
			commands:   [][]string{{cmdSetEx, "k_{id}", "notanumber", "v"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		}}

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
				b := []byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(cmdAuth), cmdAuth, len(internalAuthKey), internalAuthKey))
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

func (s *CmdTestSuite) TestPubSub() {
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
	if !strings.Contains(subResp, cmdSubscribe) || !strings.Contains(subResp, "topic-1") {
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
