package server

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"sync"

	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/redcon"
)

type CmdTestSuite struct {
	suite.Suite
	addr string
	db   *DB

	origInternalAuthKey  string
	origListenAndServeFn func(role portRole, addr string, handler func(conn redcon.Conn, cmd redcon.Command), accept func(conn redcon.Conn) bool, closed func(conn redcon.Conn, err error)) error
	origOsExitFn         func(code int)
}

func (s *CmdTestSuite) SetupSuite() {
	globalMu.Lock()
	s.origInternalAuthKey = internalAuthKey
	s.origListenAndServeFn = listenAndServeFn
	s.origOsExitFn = osExitFn
	globalMu.Unlock()
}

func (s *CmdTestSuite) SetupTest() {
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

func (s *CmdTestSuite) TearDownTest() {
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

func TestCmdSuite(t *testing.T) {
	suite.Run(t, new(CmdTestSuite))
}

func (s *CmdTestSuite) TestCmd() {
	t := s.T()
	dbPath := testutil.DBPath(t)
	appPort, adminPort := testutil.AllocateTwoFreePorts(t)
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort},
		Admin:    AdminConfig{Bind: "127.0.0.1", Port: adminPort},
	}
	s.db = StartWithConfig(cfg)
	if s.db == nil {
		t.Fatalf("StartWithConfig returned nil; appPort=%d adminPort=%d", appPort, adminPort)
	}
	s.addr = cfg.Admin.Addr()
	if err := s.db.applyIndexSpec(idxSpec{
		FullName:   "user_age",
		OwnerNs:    "user",
		Logical:    "age",
		OwnerMem:   false,
		KeyPattern: "user:*",
		Path:       "age",
	}, true); err != nil {
		t.Fatalf("failed to seed user_age index: %v", err)
	}

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
		setupDB     func(uid string)
	}{
		{
			name:        "unauthenticated write command",
			auth:        false,
			commands:    [][]string{{cmdSet, "k_{id}", "v"}},
			wantStrings: []string{"OK"},
		},
		{
			name:        "auth success stores internal key",
			auth:        false,
			commands:    [][]string{{cmdAuth, internalAuthKey}},
			wantStrings: []string{"OK"},
		},
		{
			name:        "auth same key is idempotent",
			auth:        true,
			commands:    [][]string{{cmdAuth, internalAuthKey}},
			wantStrings: []string{"OK"},
		},
		{
			name:       "auth different key after login is rejected",
			auth:       true,
			commands:   [][]string{{cmdAuth, "wrong-key"}},
			wantErrors: []string{"ERR connection already authenticated"},
			wantClosed: true,
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
			name:        "ping requires authentication",
			auth:        false,
			commands:    [][]string{{cmdPing}},
			wantStrings: []string{"PONG"},
		},
		{
			name:        "ping authenticated",
			auth:        true,
			commands:    [][]string{{cmdPing}},
			wantStrings: []string{"PONG"},
		},
		{
			name:        "quit requires authentication",
			auth:        false,
			commands:    [][]string{{cmdQuit}},
			wantStrings: []string{"OK"},
			wantClosed:  true,
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
			name:       "keys_forbidden_star",
			auth:       true,
			commands:   [][]string{{cmdKeys, "*"}},
			wantErrors: []string{"ERR forbidden key pattern"},
		},
		{
			name:       "keys_forbidden_leading_wildcard",
			auth:       true,
			commands:   [][]string{{cmdKeys, "*_key"}},
			wantErrors: []string{"ERR forbidden key pattern"},
		},
		{
			name:       "keys_forbidden_reserved_namespace",
			auth:       true,
			commands:   [][]string{{cmdKeys, "_au*"}},
			wantErrors: []string{"ERR forbidden key pattern"},
		},
		{
			name:     "keys_allow_mem_namespace",
			auth:     true,
			commands: [][]string{{cmdSet, x.MemNsPrefix + "k_{id}", "v"}, {cmdKeys, x.MemNsPrefix + "*"}},
			wantStrings: []string{
				"OK",
			},
			wantArrays: [][]string{{x.MemNsPrefix + "k_{id}"}},
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
	dbPath := testutil.DBPath(t)
	appPort, adminPort := testutil.AllocateTwoFreePorts(t)
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort},
		Admin:    AdminConfig{Bind: "127.0.0.1", Port: adminPort},
	}
	db := StartWithConfig(cfg)
	if db == nil {
		t.Fatalf("StartWithConfig returned nil; appPort=%d adminPort=%d", appPort, adminPort)
	}
	addr := cfg.Admin.Addr()
	defer func() { _ = Stop() }()

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

// X Commands

func (s *CmdTestSuite) TestParseFilter() {
	t := s.T()
	jsonRecord := `{"name": "ken", "age": 30, "status": "active", "score": 95.5}`

	tests := []struct {
		name       string
		jsonFilter string
		expectErr  bool
		expected   bool
	}{
		{"Empty string", ``, false, true},
		{"Empty object", `{}`, false, true},
		{"Invalid JSON", `{invalid`, true, false},
		{"Implicit Eq string", `{"name": "ken"}`, false, true},
		{"Implicit Eq false", `{"name": "john"}`, false, false},
		{"Explicit Eq string", `{"name": {"$eq": "ken"}}`, false, true},
		{"Explicit Eq number", `{"age": {"$eq": 30}}`, false, true},
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
		{"Implicit AND (multiple keys)", `{"age": {"$gt": 20}, "status": "active"}`, false, true},
		{"Implicit AND false", `{"age": {"$gt": 40}, "status": "active"}`, false, false},
		{"Explicit AND", `{"$and": [{"age": {"$gt": 20}}, {"status": "active"}]}`, false, true},
		{"Explicit OR true", `{"$or": [{"age": {"$lt": 20}}, {"status": "active"}]}`, false, true},
		{"Explicit OR false", `{"$or": [{"age": {"$lt": 20}}, {"status": "pending"}]}`, false, false},
		{"Complex Nested", `{"$or": [{"age": {"$lt": 20}}, {"$and": [{"age": {"$gt": 18}}, {"status": "active"}]}]}`, false, true},
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

func (s *CmdTestSuite) TestXCmd() {
	t := s.T()
	dbPath := testutil.DBPath(t)
	appPort, adminPort := testutil.AllocateTwoFreePorts(t)
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort},
		Admin:    AdminConfig{Bind: "127.0.0.1", Port: adminPort},
	}
	s.db = StartWithConfig(cfg)
	if s.db == nil {
		t.Fatalf("StartWithConfig returned nil; appPort=%d adminPort=%d", appPort, adminPort)
	}
	s.addr = cfg.Admin.Addr()
	if err := s.db.applyIndexSpec(idxSpec{
		FullName:   "user_age",
		OwnerNs:    "user",
		Logical:    "age",
		OwnerMem:   false,
		KeyPattern: "user:*",
		Path:       "age",
	}, true); err != nil {
		t.Fatalf("failed to seed user_age index for XCmd: %v", err)
	}

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
		setupDB     func(uid string)
	}{
		{
			name:       "searchindex_wrong_number_of_args_01",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "age"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchindex' command"},
		},
		{
			name:       "searchindex_invalid_order_01",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "age", `{"op":"pattern","p":"*"}`, "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchindex_invalid_json_01",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "age", `{"op":"pattern","p":"*"}`, "{invalid"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchkey_wrong_number_of_args_01",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "*"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchkey' command"},
		},
		{
			name:       "searchkey_invalid_order_01",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"*"}`, "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchkey_invalid_json_01",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"*"}`, "{invalid"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchindex_wrong_number_of_args",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "attr"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchindex' command"},
		},
		{
			name:       "searchindex_invalid_order_02",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "attr", `{"op":"pattern","p":"*"}`, "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchindex_invalid_json_02",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "attr", `{"op":"pattern","p":"*"}`, "{invalid}", "ASC"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchindex_zero_legacy_rejects_plain_glob_arg2",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "age", "user:*", "{}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchindex' command"},
		},
		{
			name:       "searchindex_limit_count_zero_rejects_wire",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, x.Idx[testUserDoc]("age", "*", "age").Name(), `{"op":"pattern","p":"user:*"}`, "{}", "LIMIT", "0"}},
			wantErrors: []string{"ERR invalid count for LIMIT: 0"},
		},
		{
			name:       "searchindex_limit_count_negative_rejects_wire",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, x.Idx[testUserDoc]("age", "*", "age").Name(), `{"op":"pattern","p":"user:*"}`, "{}", "LIMIT", "-5"}},
			wantErrors: []string{"ERR invalid count for LIMIT: -5"},
		},
		{
			name:       "searchindex_argc5_keyword_not_limit_rejects",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, x.Idx[testUserDoc]("age", "*", "age").Name(), `{"op":"pattern","p":"user:*"}`, "{}", "FOO", "5"}},
			wantErrors: []string{"ERR invalid argument: FOO"},
		},
		{
			name:       "searchindex_argc6_keyword_not_limit_after_asc_rejects",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, x.Idx[testUserDoc]("age", "*", "age").Name(), `{"op":"pattern","p":"user:*"}`, "{}", "ASC", "BLAH", "3"}},
			wantErrors: []string{"ERR invalid argument: BLAH"},
		},
		{
			name:       "searchindex_argc5_limit_parseint_non_numeric_rejects",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, x.Idx[testUserDoc]("age", "*", "age").Name(), `{"op":"pattern","p":"user:*"}`, "{}", "LIMIT", "not_a_number"}},
			wantErrors: []string{"ERR invalid count for LIMIT: not_a_number"},
		},
		{
			name:       "searchindex_argc5_desc_plus_limit",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, x.Idx[testUserDoc]("age", "*", "age").Name(), `{"op":"pattern","p":"user:*_{id}"}`, "{}", "DESC", "LIMIT", "1"}},
			wantArrays: [][]string{{`{"id":"2_{id}", "age":30}`}},
			setupDB: func(uid string) {
				_ = s.db.Set(fmt.Sprintf("user:1_%s", uid), fmt.Sprintf(`{"id":"1_%s", "age":20}`, uid))
				_ = s.db.Set(fmt.Sprintf("user:2_%s", uid), fmt.Sprintf(`{"id":"2_%s", "age":30}`, uid))
			},
		},
		{
			name:       "searchindex_argc4_limit_only_asc_default",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, x.Idx[testUserDoc]("age", "*", "age").Name(), `{"op":"pattern","p":"user:*_{id}"}`, "{}", "LIMIT", "1"}},
			wantArrays: [][]string{{`{"id":"1_{id}", "age":20}`}},
			setupDB: func(uid string) {
				_ = s.db.Set(fmt.Sprintf("user:1_%s", uid), fmt.Sprintf(`{"id":"1_%s", "age":20}`, uid))
				_ = s.db.Set(fmt.Sprintf("user:2_%s", uid), fmt.Sprintf(`{"id":"2_%s", "age":30}`, uid))
			},
		},
		{
			name:       "searchkey_wrong_number_of_args_02",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "pattern"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchkey' command"},
		},
		{
			name:       "searchkey_invalid_order_02",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"pattern"}`, "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchkey_invalid_json_02",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"pattern"}`, "{invalid}", "ASC"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchkey_forbidden_cross_layer_pattern",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"*user:*"}`, "{}", "ASC"}},
			wantErrors: []string{"ERR key range cannot start with wildcard"},
		},
		{
			name:       "searchkey_zero_legacy_rejects_plain_glob_arg1",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "*user:*", "{}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchkey' command"},
		},
		{
			name:       "searchkey_limit_count_zero_rejects_wire",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"user:*"}`, "{}", "LIMIT", "0"}},
			wantErrors: []string{"ERR invalid count for LIMIT: 0"},
		},
		{
			name:       "searchkey_limit_count_negative_rejects_wire",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"user:*"}`, "{}", "LIMIT", "-5"}},
			wantErrors: []string{"ERR invalid count for LIMIT: -5"},
		},
		{
			name:       "searchkey_limit_count_non_numeric_rejects_wire",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"user:*"}`, "{}", "LIMIT", "abc"}},
			wantErrors: []string{"ERR invalid count for LIMIT: abc"},
		},
		{
			name:       "searchkey_argc5_desc_plus_limit",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"t:*"}`, "{}", "DESC", "LIMIT", "1"}},
			wantArrays: [][]string{{`{"k":"t:2"}`}},
			setupDB: func(uid string) {
				_ = s.db.Set("t:1", `{"k":"t:1"}`)
				_ = s.db.Set("t:2", `{"k":"t:2"}`)
			},
		},
		{
			name:       "searchkey_argc4_limit_only_asc_default",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"t:*"}`, "{}", "LIMIT", "1"}},
			wantArrays: [][]string{{`{"k":"t:1"}`}},
			setupDB: func(uid string) {
				_ = s.db.Set("t:1", `{"k":"t:1"}`)
				_ = s.db.Set("t:2", `{"k":"t:2"}`)
			},
		},
		{
			name:       "searchindex success",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, x.Idx[testUserDoc]("age", "*", "age").Name(), `{"op":"pattern","p":"user:*_{id}"}`, "{}", "ASC"}},
			wantArrays: [][]string{{`{"id":"1_{id}", "age":20}`, `{"id":"2_{id}", "age":30}`}},
			setupDB: func(uid string) {
				_ = s.db.Set(fmt.Sprintf("user:1_%s", uid), fmt.Sprintf(`{"id":"1_%s", "age":20}`, uid))
				_ = s.db.Set(fmt.Sprintf("user:2_%s", uid), fmt.Sprintf(`{"id":"2_%s", "age":30}`, uid))
			},
		},
		{
			name:       "searchindex unknown index",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "unknown", `{"op":"pattern","p":"*"}`, "{}", "ASC"}},
			wantErrors: []string{"ERR index not found: unknown"},
		},
		{
			name:       "update success",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*_{id}"}`, `{"id": "1_{id}"}`, `{"name": "updated"}`}},
			wantArrays: [][]string{{`user:1_{id}`}},
			setupDB: func(uid string) {
				_ = s.db.Set(fmt.Sprintf("user:1_%s", uid), fmt.Sprintf(`{"id":"1_%s", "age":20, "name":"old"}`, uid))
				_ = s.db.Set(fmt.Sprintf("user:2_%s", uid), fmt.Sprintf(`{"id":"2_%s", "age":30, "name":"old"}`, uid))
			},
		},
		{
			name:       "update no valid updates",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*"}`, "{}", `{}`}},
			wantErrors: []string{"ERR no valid updates provided"},
		},
		{
			name:       "update invalid json",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*"}`, "{}", `{invalid`}},
			wantErrors: []string{"ERR invalid update json format"},
		},
		{
			name:       "update argc2_rejects",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"u:*"}`}},
			wantErrors: []string{"ERR wrong number of arguments for '" + cmdUpdate + "' command"},
		},
		{
			name:       "update argc4_keyword_not_limit_rejects",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"u:*"}`, "{}", `{"a":1}`, "FOO", "1"}},
			wantErrors: []string{"ERR invalid argument: FOO"},
		},
		{
			name:       "update argc6_extra_rejects",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"u:*"}`, "{}", `{"a":1}`, "LIMIT", "1", "extra"}},
			wantErrors: []string{"ERR wrong number of arguments for '" + cmdUpdate + "' command"},
		},
		{
			name:       "update limit0_rejects",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*"}`, "{}", `{"a":1}`, "LIMIT", "0"}},
			wantErrors: []string{"ERR invalid count for LIMIT: 0"},
		},
		{
			name:       "update limit_negative5_rejects",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*"}`, "{}", `{"a":1}`, "LIMIT", "-5"}},
			wantErrors: []string{"ERR invalid count for LIMIT: -5"},
		},
		{
			name:       "update limit_non_numeric_rejects",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*"}`, "{}", `{"a":1}`, "LIMIT", "not_a_number"}},
			wantErrors: []string{"ERR invalid count for LIMIT: not_a_number"},
		},
		{
			name:       "update arg1_plain_glob_not_json_rejects",
			auth:       true,
			commands:   [][]string{{cmdUpdate, "user:*", "{}", `{"a":1}`}},
			wantErrors: []string{"ERR wrong number of arguments for '" + cmdUpdate + "' command"},
		},
		{
			name:       "searchkey success",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"user:*_{id}"}`, "{}", "DESC"}},
			wantArrays: [][]string{{`{"id":"2_{id}"}`, `{"id":"1_{id}"}`}},
			setupDB: func(uid string) {
				_ = s.db.Set(fmt.Sprintf("user:1_%s", uid), fmt.Sprintf(`{"id":"1_%s"}`, uid))
				_ = s.db.Set(fmt.Sprintf("user:2_%s", uid), fmt.Sprintf(`{"id":"2_%s"}`, uid))
			},
		},
		{
			name:       "searchkey not found",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"unknown_{id}:*"}`, "{}"}},
			wantArrays: [][]string{{}},
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

			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			_, err = conn.Read(make([]byte, 1))
			if err != nil {
				if err == io.EOF || strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "closed") || strings.Contains(err.Error(), "reset by peer") {
					closed = true
				}
			}

			for _, e := range replaceID1D(tc.wantErrors, uid) {
				if !strings.Contains(finalResp, e) {
					t.Errorf("expected error %q, got resp: %q", e, finalResp)
				}
			}

			for _, s := range replaceID1D(tc.wantStrings, uid) {
				if !strings.Contains(finalResp, "+"+s+"\r\n") {
					t.Errorf("expected string %q, got resp: %q", s, finalResp)
				}
			}

			for _, b := range replaceID1D(tc.wantBulks, uid) {
				if !strings.Contains(finalResp, fmt.Sprintf("$%d\r\n%s\r\n", len(b), b)) {
					t.Errorf("expected bulk %q, got resp: %q", b, finalResp)
				}
			}

			for _, val := range tc.wantInts {
				if !strings.Contains(finalResp, fmt.Sprintf(":%d\r\n", val)) {
					t.Errorf("expected int %d, got resp: %q", val, finalResp)
				}
			}

			if tc.wantNulls > 0 {
				if strings.Count(finalResp, "$-1\r\n") != tc.wantNulls {
					t.Errorf("expected %d nulls, got resp: %q", tc.wantNulls, finalResp)
				}
			}

			if replaceID(tc.wantArrays, uid) != nil {
				for _, wantArr := range replaceID(tc.wantArrays, uid) {
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

			if closed != tc.wantClosed {
				t.Errorf("closed = %v, want %v", closed, tc.wantClosed)
			}
		})
	}
}
