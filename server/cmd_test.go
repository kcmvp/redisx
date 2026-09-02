package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"sync"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/require"
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
	appPort, ctrlPort := testutil.AllocateTwoFreePorts(t)
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort},
	}
	s.db = StartWith(cfg)
	if s.db == nil {
		t.Fatalf("StartWith returned nil; appPort=%d ctrlPort=%d", appPort, ctrlPort)
	}
	s.addr = cfg.Ctrl.Addr()
	if err := s.db.writeDocSpec(docSpec{
		Namespace: "user",
		KeyAttrs:  []string{"id"},
		Mem:       false,
	}); err != nil {
		t.Fatalf("failed to seed user doc spec: %v", err)
	}
	if err := s.db.writeIndexSpec(idxSpec{
		OwnerNs:    "user",
		Logical:    "age",
		KeyPattern: "user:*",
		Paths:      []string{"age"},
	}); err != nil {
		t.Fatalf("failed to seed user:age index: %v", err)
	}

	seedDocVer, err := canonicalDocMD5(docSpec{Namespace: "user", KeyAttrs: []string{"id"}, Mem: false})
	if err != nil {
		t.Fatalf("canonicalDocMD5: %v", err)
	}
	seedIdxVer, err := canonicalIdxMD5(idxSpec{OwnerNs: "user", Logical: "age", KeyPattern: "user:*", Paths: []string{"age"}})
	if err != nil {
		t.Fatalf("canonicalIdxMD5: %v", err)
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
			name:        "authenticated write command",
			auth:        true,
			commands:    [][]string{{cmdSet, "k:{id}", "v"}},
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
			name:        "ping requires authentication — AUTH needed on all boots",
			auth:        true,
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
			name:        "quit requires authentication — AUTH needed on all boots",
			auth:        true,
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
				{cmdSet, "name:{id}", "alice"},
				{cmdGet, "name:{id}"},
				{cmdSetNX, "name:{id}", "bob"},
				{cmdDel, "name:{id}"},
				{cmdGet, "name:{id}"},
			},
			wantStrings: []string{"OK"},
			wantBulks:   []string{"alice"},
			wantInts:    []int{0, 1},
			wantNulls:   1,
		},
		{
			name:      "get non-existent",
			auth:      true,
			commands:  [][]string{{cmdGet, "nonexistent:{id}"}},
			wantNulls: 1,
		},
		{
			name:        cmdKeys,
			auth:        true,
			commands:    [][]string{{cmdSet, "user", `{"id":"k1_{id}"}`}, {cmdSet, "user", `{"id":"k2_{id}"}`}, {cmdKeys, "user"}},
			wantStrings: []string{"OK", "OK"},
			setupDB: func(uid string) {
				_ = s.db.writeDocSpec(docSpec{Namespace: "user", KeyAttrs: []string{"id"}, Mem: false})
			},
		},
		{
			name:       "keys_not_found",
			auth:       true,
			commands:   [][]string{{cmdKeys, "nonexistent*"}},
			wantErrors: []string{"doc-path requires bare"},
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
			commands: [][]string{{cmdSet, naming.BuildStorageKey(naming.BuildStorageNs("k", true), "{id}"), "v"}, {cmdKeys, naming.StorageNsKeyPattern(naming.BuildStorageNs("k", true))}},
			wantStrings: []string{
				"OK",
			},
			wantArrays: [][]string{{naming.BuildStorageKey(naming.BuildStorageNs("k", true), "{id}")}},
		},
		{
			name:       "keys_doc_meta_introspection",
			auth:       true,
			commands:   [][]string{{cmdKeys, "_doc_:*"}},
			wantArrays: [][]string{{naming.DocMetaKey("user", seedDocVer)}},
		},
		{
			name:       "keys_doc_meta_bare_ns",
			auth:       true,
			commands:   [][]string{{cmdKeys, "_doc_"}},
			wantArrays: [][]string{{naming.DocMetaKey("user", seedDocVer)}},
		},
		{
			name:       "keys_idx_meta_introspection",
			auth:       true,
			commands:   [][]string{{cmdKeys, "_idx_:*"}},
			wantArrays: [][]string{{naming.IdxMetaKey(naming.BuildIdxFullName("user", "age"), seedIdxVer)}},
		},
		{
			name:       "keys_auth_meta_still_forbidden",
			auth:       true,
			commands:   [][]string{{cmdKeys, "_auth_:*"}},
			wantErrors: []string{"ERR forbidden key pattern"},
		},
		{
			name:     "del non-existent",
			auth:     true,
			commands: [][]string{{cmdDel, "nonexistent:{id}"}},
			wantInts: []int{0},
		},
		{
			name:        "setnx exists",
			auth:        true,
			commands:    [][]string{{cmdSet, "k:{id}", "v"}, {cmdSetNX, "k:{id}", "v2"}},
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
			commands:    [][]string{{cmdSet, "k2:{id}", "v2", "EX", "1"}},
			wantStrings: []string{"OK"},
		},
		{
			name:        "set with PX",
			auth:        true,
			commands:    [][]string{{cmdSet, "k3:{id}", "v3", "PX", "500"}},
			wantStrings: []string{"OK"},
		},
		{
			name:        "setex command",
			auth:        true,
			commands:    [][]string{{cmdSetEx, "k4:{id}", "1", "v4"}},
			wantStrings: []string{"OK"},
		},
		{
			name:       "wrong number of args setex",
			auth:       true,
			commands:   [][]string{{cmdSetEx, "k:{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'setex' command"},
		},
		{
			name:       "setex invalid ttl",
			auth:       true,
			commands:   [][]string{{cmdSetEx, "k:{id}", "abc", "v"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "set wrong number of args",
			auth:       true,
			commands:   [][]string{{cmdSet, "k:{id}"}},
			wantErrors: []string{"ERR wrong number of arguments for 'set' command"},
		},
		{
			name:       "set EX invalid integer",
			auth:       true,
			commands:   [][]string{{cmdSet, "k:{id}", "v", "EX", "abc"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "set PX invalid integer",
			auth:       true,
			commands:   [][]string{{cmdSet, "k:{id}", "v", "PX", "abc"}},
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
			commands:   [][]string{{cmdSetNX, "k:{id}"}},
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
			commands:   [][]string{{cmdSet, "k:{id}"}},
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
			commands:   [][]string{{cmdSetNX, "k:{id}"}},
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
			commands:   [][]string{{cmdSet, "k:{id}", "v", "EX", "notanumber"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "set with PX invalid value",
			auth:       true,
			commands:   [][]string{{cmdSet, "k:{id}", "v", "PX", "notanumber"}},
			wantErrors: []string{"ERR value is not an integer or out of range"},
		},
		{
			name:       "setex command invalid time",
			auth:       true,
			commands:   [][]string{{cmdSetEx, "k:{id}", "notanumber", "v"}},
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
	appPort, ctrlPort := testutil.AllocateTwoFreePorts(t)
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort},
	}
	db := StartWith(cfg)
	if db == nil {
		t.Fatalf("StartWith returned nil; appPort=%d ctrlPort=%d", appPort, ctrlPort)
	}
	addr := cfg.Ctrl.Addr()
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
		{"And empty array passes everything", `{"$and": []}`, false, true},
		{"And empty object element passes everything", `{"$and": [{}]}`, false, true},
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
	appPort, ctrlPort := testutil.AllocateTwoFreePorts(t)
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort},
	}
	s.db = StartWith(cfg)
	if s.db == nil {
		t.Fatalf("StartWith returned nil; appPort=%d ctrlPort=%d", appPort, ctrlPort)
	}
	s.addr = cfg.Ctrl.Addr()
	if err := s.db.writeDocSpec(docSpec{
		Namespace: "user",
		KeyAttrs:  []string{"id"},
		Mem:       false,
	}); err != nil {
		t.Fatalf("failed to seed user doc spec for XCmd: %v", err)
	}
	if err := s.db.writeIndexSpec(idxSpec{
		OwnerNs:    "user",
		Logical:    "age",
		KeyPattern: "user:*",
		Paths:      []string{"age"},
	}); err != nil {
		t.Fatalf("failed to seed user:age index for XCmd: %v", err)
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
			commands:   [][]string{{cmdSearchIndex, "user:age"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchindex' command"},
		},
		{
			name:       "searchindex_invalid_order_01",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user:age", `{"op":"pattern","p":"user:*"}`, "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchindex_invalid_json_01",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user:age", `{"op":"pattern","p":"user:*"}`, "{invalid"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchkey_wrong_number_of_args_01",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "t:*"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchkey' command"},
		},
		{
			name:       "searchkey_invalid_order_01",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"user:*"}`, "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchkey_invalid_json_01",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"user:*"}`, "{invalid"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchindex_wrong_number_of_args",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user:attr"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchindex' command"},
		},
		{
			name:       "searchindex_invalid_order_02",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user:attr", `{"op":"pattern","p":"user:*"}`, "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchindex_invalid_json_02",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user:attr", `{"op":"pattern","p":"user:*"}`, "{invalid}", "ASC"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchindex_zero_legacy_rejects_plain_glob_arg2",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user:age", "user:*", "{}"}},
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
			wantErrors: []string{"ERR SEARCHKEY key-range must be anchored to a namespace (no leading wildcard)"},
		},
		{
			name:       "searchkey_zero_legacy_rejects_plain_glob_arg1",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "*user:*", "{}"}},
			wantErrors: []string{"ERR SEARCHKEY key-range must be anchored to a namespace (no leading wildcard)"},
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
			commands:   [][]string{{cmdSearchIndex, "user:unknown", `{"op":"pattern","p":"user:*"}`, "{}", "ASC"}},
			wantErrors: []string{"ERR index not found: user:unknown"},
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
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*"}`}},
			wantErrors: []string{"ERR wrong number of arguments for '" + cmdUpdate + "' command"},
		},
		{
			name:       "update argc4_keyword_not_limit_rejects",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*"}`, "{}", `{"a":1}`, "FOO", "1"}},
			wantErrors: []string{"ERR invalid argument: FOO"},
		},
		{
			name:       "update argc6_extra_rejects",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*"}`, "{}", `{"a":1}`, "LIMIT", "1", "extra"}},
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
			name:       "update_arg1_plain_glob_sugar_form_runs_normally",
			auth:       true,
			commands:   [][]string{{cmdUpdate, `{"op":"pattern","p":"user:*"}`, `{"id":"__never_matches__"}`, `{"__x":1}`}},
			wantArrays: [][]string{{}},
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
			commands:   [][]string{{cmdSearchKey, `{"op":"pattern","p":"unknownns:*"}`, "{}"}},
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

func (s *CmdTestSuite) TestStrictGates() {
	t := s.T()
	dbPath := testutil.DBPath(t)
	appPort, ctrlPort := testutil.AllocateTwoFreePorts(t)
	appAuth := "app-secret-strict"
	ctrlAuth := "ctrl-secret-strict"
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort, Auth: appAuth},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort, Auth: ctrlAuth},
	}
	s.db = StartWith(cfg)
	if s.db == nil {
		t.Fatalf("StartWith returned nil for strict gates; appPort=%d ctrlPort=%d", appPort, ctrlPort)
	}
	appAddr := cfg.App.Addr()
	ctrlAddr := cfg.Ctrl.Addr()

	runRESP := func(addr string, auth string, cmds [][]string) (string, bool) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("failed to dial %s: %v", addr, err)
		}
		defer func() { _ = conn.Close() }()
		closed := false
		var sb strings.Builder
		if auth != "" {
			b := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(auth), auth)
			if _, err := conn.Write([]byte(b)); err == nil {
				buf := make([]byte, 4096)
				_ = conn.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
				n, _ := conn.Read(buf)
				if n > 0 {
					sb.Write(buf[:n])
				}
			}
		}
		for _, args := range cmds {
			if len(args) == 0 {
				continue
			}
			var b strings.Builder
			fmt.Fprintf(&b, "*%d\r\n", len(args))
			for _, a := range args {
				fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
			}
			if _, err := conn.Write([]byte(b.String())); err != nil {
				closed = true
				break
			}
			buf := make([]byte, 16384)
			_ = conn.SetReadDeadline(time.Now().Add(120 * time.Millisecond))
			n, rerr := conn.Read(buf)
			if rerr == nil && n > 0 {
				sb.Write(buf[:n])
			}
		}
		return sb.String(), closed
	}

	specStr := `{"namespace":"strictuser","mem":false,"key_attrs":["id","org"],"ttl_ns":3600000000000}`
	resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"regsch", specStr}})
	if !strings.Contains(resp, "+OK\r\n") {
		t.Fatalf("REGSCH failed — expected OK, got: %s", resp)
	}

	t.Run("KV_no_colon_SET", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"set", "noblekey", "v"}})
		require.Contains(t, resp, "namespace separator")
	})
	t.Run("KV_no_colon_SETEX", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"setex", "noblekey", "10", "v"}})
		require.Contains(t, resp, "namespace separator")
	})
	t.Run("KV_no_colon_SETNX", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"setnx", "noblekey", "v"}})
		require.Contains(t, resp, "namespace separator")
	})
	t.Run("KV_no_colon_GET", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"get", "noblekey"}})
		require.Contains(t, resp, "namespace separator")
	})
	t.Run("KV_no_colon_DEL", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"del", "noblekey"}})
		require.Contains(t, resp, "namespace separator")
	})

	t.Run("Doc_unregistered_ns_SET", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"set", "nonexistentns", `{"id":"1","org":"acme"}`}})
		require.Contains(t, resp, "not registered")
	})
	t.Run("Doc_unregistered_ns_SETEX", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"setex", "nonexistentns", "60", `{"id":"1","org":"acme"}`}})
		require.Contains(t, resp, "not registered")
	})
	t.Run("Doc_unregistered_ns_SETNX", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"setnx", "nonexistentns", `{"id":"1","org":"acme"}`}})
		require.Contains(t, resp, "not registered")
	})
	t.Run("Doc_unregistered_ns_GET", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"get", "nonexistentns", "1"}})
		require.Contains(t, resp, "not registered")
	})
	t.Run("Doc_unregistered_ns_DEL", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"del", "nonexistentns", "1"}})
		require.Contains(t, resp, "not registered")
	})
	t.Run("Doc_unregistered_ns_UPDATE", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"update", "nonexistentns:*", "{}", `{"$set":{"age":30}}`}})
		require.Contains(t, resp, "not registered", "UPDATE on unregistered namespace must fail-closed with precise registry ERR")
	})

	t.Run("Doc_GET_ns_alone_ERR", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"get", "strictuser"}})
		require.Contains(t, resp, "alone is not a query")
	})
	t.Run("Doc_DEL_ns_alone_ERR", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"del", "strictuser"}})
		require.Contains(t, resp, "alone is not a delete target")
	})

	t.Run("Doc_REGSCH_reserved_indexes_field_ERR", func(t *testing.T) {
		bad := `{"namespace":"bad_ns","mem":false,"key_attrs":["id"],"indexes":[{"name":"x"}]}`
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"regsch", bad}})
		require.Contains(t, resp, "reserved field 'indexes'")
	})

	t.Run("Doc_SET_object_ok", func(t *testing.T) {
		doc := `{"id":"u1","org":"acme","age":30}`
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{
			{"set", "strictuser", doc},
			{"get", "strictuser", naming.JoinPKAttrValues([]string{"u1", "acme"})},
		})
		require.Contains(t, resp, "+OK\r\n")
		require.Contains(t, resp, fmt.Sprintf("$%d\r\n%s\r\n", len(doc), doc))
	})

	t.Run("Doc_SETNX_collision_zero_written", func(t *testing.T) {
		a := `{"id":"u_coll","org":"acme","n":1}`
		b := `{"id":"u_coll","org":"acme","n":2}`
		c := `{"id":"u_second","org":"acme","n":3}`
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{
			{"set", "strictuser", a},
			{"setnx", "strictuser", "[" + b + "," + c + "]"},
			{"get", "strictuser", naming.JoinPKAttrValues([]string{"u_coll", "acme"})},
			{"get", "strictuser", naming.JoinPKAttrValues([]string{"u_second", "acme"})},
		})
		require.Contains(t, resp, "+OK\r\n")
		idx := strings.Index(resp, "+OK\r\n")
		after := resp[idx+len("+OK\r\n"):]
		require.Contains(t, after, "$-1\r\n", "SETNX collision must roll back — second object must NOT exist (Null)")
		require.Contains(t, after, `{"id":"u_coll","org":"acme","n":1}`, "original value after SETNX collision must be preserved")
	})

	t.Run("Doc_UPDATE_pk_mutation_ERR", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{
			{"set", "strictuser", `{"id":"pk1","org":"acme","age":1}`},
			{"update", "strictuser:*", `{"id":"pk1"}`, `{"id":"PK_CHANGED"}`},
		})
		require.Contains(t, resp, "pk mutations are not allowed")
	})

	t.Run("Doc_SEARCHKEY_bare_pivot_ERR", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"searchkey", "*", "{}"}})
		require.Contains(t, resp, "must be anchored to a namespace")
	})

	t.Run("Doc_SEARCHKEY_unregistered_ns_ERR", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"searchkey", "ghostns:*", "{}"}})
		require.Contains(t, resp, "*0\r\n", "kv-pattern range (with ':') bypasses REGSCH gate; SEARCHKEY returns 0 matched values")
	})

	t.Run("Doc_SEARCHINDEX_unregistered_owner_ns_ERR", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{{"searchindex", "ghostdoc:zzz", `{"op":"pattern","p":"*"}`, "{}"}})
		require.Contains(t, resp, "not registered")
	})

	t.Run("KEYS_app_port_Gate1_reject", func(t *testing.T) {
		resp, _ := runRESP(appAddr, appAuth, [][]string{{"keys", "*"}})
		require.Contains(t, resp, "No Privilege")
	})
	t.Run("KEYS_ctrl_port_pass", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{
			{"set", "strictuser", `{"id":"u_admin1","org":"acme","age":1}`},
			{"set", "strictuser", `{"id":"u_admin2","org":"acme","age":2}`},
			{"keys", "strictuser"},
		})
		require.NotContains(t, resp, "No Privilege")
		require.Contains(t, resp, "strictuser:u_admin1_acme")
		require.Contains(t, resp, "strictuser:u_admin2_acme")
	})

	t.Run("REGSCH_DROPSCH_roundtrip_present", func(t *testing.T) {
		docJSON := `{"namespace":"strictmeta","mem":false,"key_attrs":["id"],"ttl_ns":0}`
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{
			{"regsch", docJSON},
			{"dropsch", "strictmeta"},
			{"regsch", docJSON},
		})
		require.Contains(t, resp, "+OK\r\n+OK\r\n+OK\r\n", "registry cmd all OK: reg + drop + reg")
	})

	t.Run("REGIDX_DROPIDX_roundtrip_present", func(t *testing.T) {
		docJSON := `{"namespace":"strictidxowner","mem":false,"key_attrs":["id"],"ttl_ns":0}`
		idxJSON := `{"owner_ns":"strictidxowner","logical":"age","paths":["age"],"key_pattern":"_d_strictidxowner:*"}`
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{
			{"regsch", docJSON},
			{"regidx", idxJSON},
			{"dropidx", "strictidxowner", "age"},
		})
		require.Contains(t, resp, "+OK\r\n+OK\r\n+OK\r\n", "registry cmd all OK: regsch + regidx + dropidx")
	})

	t.Run("KV_mutation_internal_doc_ns_ERR", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{
			{"set", "_doc_:strictuser", `{"ns":"evil"}`},
			{"setnx", "_idx_:strictuser_age:v_1234567890ab", `{}`},
			{"setex", "_auth_:keyleak", "60", "5"},
			{"del", "_auth_:client-test-external-key"},
		})
		require.Contains(t, resp, "managed exclusively via dedicated registry commands", "any mutation of _doc_/_idx_/_auth_ must raise the Write Guard ERR")
	})
	t.Run("KV_DEL_multi_full_keys_ERR", func(t *testing.T) {
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{
			{"set", "strictuser", `{"id":"u_del1","org":"acme","age":1}`},
			{"set", "strictuser", `{"id":"u_del2","org":"acme","age":2}`},
			{"del", "strictuser:u_del1_acme", "strictuser:u_del2_acme"},
		})
		require.Contains(t, resp, "takes exactly one full key", "DEL with argc≥3 and arg1 containing ':' → parameter ERR (user ruling: KV DEL single only)")
	})
	t.Run("SETEX_SETNX_independent_handlers_smoke", func(t *testing.T) {
		pk1 := `{"id":"u_se_1","org":"acme","age":7}`
		pk2 := `{"id":"u_sn_1","org":"acme","age":8}`
		pk2Dup := `{"id":"u_sn_1","org":"acme","age":9999}`
		pkSuffix1 := naming.JoinPKAttrValues([]string{"u_se_1", "acme"})
		pkSuffix2 := naming.JoinPKAttrValues([]string{"u_sn_1", "acme"})
		resp, _ := runRESP(ctrlAddr, ctrlAuth, [][]string{
			{"setex", "strictuser", "60", pk1},
			{"get", "strictuser", pkSuffix1},
			{"setnx", "strictuser", pk2},
			{"get", "strictuser", pkSuffix2},
			{"setnx", "strictuser", pk2Dup},
			{"get", "strictuser", pkSuffix2},
		})
		require.Contains(t, resp, "+OK\r\n", "SETEX doc-path happy-path must return OK (independent cmdSetEx handler, not SET flag fallback)")
		idx := strings.Index(resp, "+OK\r\n")
		afterSetEx := resp[idx+len("+OK\r\n"):]
		require.Contains(t, afterSetEx, pk1, "SETEX doc-path must write the full JSON body (storageNs:pk storage key)")
		require.Contains(t, afterSetEx, ":1\r\n", "first SETNX on fresh pk-suffix must return integer 1 (independent cmdSetNx handler)")
		require.Contains(t, afterSetEx, pk2, "SETNX doc-path must write body of new doc")
		idx1 := strings.Index(afterSetEx, ":1\r\n")
		afterFirstNX := afterSetEx[idx1+len(":1\r\n"):]
		require.Contains(t, afterFirstNX, ":0\r\n", "duplicate SETNX on same pk-suffix must return integer 0 (SETNX NX semantics, not SET overwrite)")
		require.NotContains(t, afterFirstNX[strings.Index(afterFirstNX, ":0\r\n"):], `"age":9999`, "SETNX duplicate must NOT overwrite original body")
	})
}

// respRoundTrip dials addr, optionally AUTHs with auth, sends cmds in order
// and concatenates all replies read back (deadline-bounded reads).
func respRoundTrip(t *testing.T, addr, auth string, cmds [][]string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial %s: %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	var sb strings.Builder
	if auth != "" {
		b := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(auth), auth)
		if _, err := conn.Write([]byte(b)); err == nil {
			buf := make([]byte, 4096)
			_ = conn.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
			n, _ := conn.Read(buf)
			if n > 0 {
				sb.Write(buf[:n])
			}
		}
	}
	for _, args := range cmds {
		if len(args) == 0 {
			continue
		}
		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(args))
		for _, a := range args {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
		}
		if _, err := conn.Write([]byte(b.String())); err != nil {
			break
		}
		buf := make([]byte, 16384)
		_ = conn.SetReadDeadline(time.Now().Add(120 * time.Millisecond))
		n, rerr := conn.Read(buf)
		if rerr == nil && n > 0 {
			sb.Write(buf[:n])
		}
	}
	return sb.String()
}

// TestRegistryCommands exercises the REGSCH/REGIDX/DROPSCH/DROPIDX wire
// command matrix (happy paths, idempotent re-register, fingerprint upgrade,
// argv/JSON/semantic rejections) plus the DEL doc-path multi-pk batch and
// the DB-level CreateIndex / deleteBatchAtomic entry points.
func (s *CmdTestSuite) TestRegistryCommands() {
	t := s.T()
	dbPath := testutil.DBPath(t)
	appPort, ctrlPort := testutil.AllocateTwoFreePorts(t)
	ctrlAuth := "regcmd-ctrl-key"
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort, Auth: "regcmd-app-key"},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort, Auth: ctrlAuth},
	}
	s.db = StartWith(cfg)
	if s.db == nil {
		t.Fatalf("StartWith returned nil for registry commands; appPort=%d ctrlPort=%d", appPort, ctrlPort)
	}
	ctrlAddr := cfg.Ctrl.Addr()

	runRESP := func(cmds [][]string) string {
		return respRoundTrip(t, ctrlAddr, ctrlAuth, cmds)
	}

	sch := func(ns string, mem bool, attrs ...string) string {
		b, _ := json.Marshal(map[string]any{
			"namespace": ns,
			"mem":       mem,
			"key_attrs": attrs,
			"ttl_ns":    0,
		})
		return string(b)
	}

	t.Run("REGSCH_matrix", func(t *testing.T) {
		tests := []struct {
			name    string
			cmds    [][]string
			want    string
			wantNot string
		}{
			{"disk happy", [][]string{{"regsch", sch("regcmd", false, "id")}}, "+OK\r\n", ""},
			{"mem happy", [][]string{{"regsch", sch("regcmdmem", true, "id")}}, "+OK\r\n", ""},
			{"idempotent re-register", [][]string{
				{"regsch", sch("regschidem", false, "id")},
				{"regsch", sch("regschidem", false, "id")},
			}, "+OK\r\n+OK\r\n", ""},
			{"fingerprint upgrade adds attr", [][]string{
				{"regsch", sch("regschup", false, "id")},
				{"regsch", sch("regschup", false, "id", "email")},
			}, "+OK\r\n+OK\r\n", ""},
			{"argc missing json", [][]string{{"regsch"}}, "wrong number of arguments for 'regsch'", ""},
			{"invalid json", [][]string{{"regsch", "{bad"}}, "ERR REGSCH invalid JSON format", ""},
			{"unknown field", [][]string{{"regsch", `{"namespace":"regcmdx","zzz":1}`}}, "ERR REGSCH schema:", ""},
			{"invalid namespace underscore", [][]string{{"regsch", sch("_bad", false, "id")}}, "ERR REGSCH writeDocSpec:", ""},
			{"empty key attr", [][]string{{"regsch", sch("regbad", false, "id", "")}}, "ERR REGSCH writeDocSpec: key_attrs[1] is empty", ""},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				resp := runRESP(tc.cmds)
				require.Contains(t, resp, tc.want)
				if tc.wantNot != "" {
					require.NotContains(t, resp, tc.wantNot)
				}
			})
		}
	})

	t.Run("REGSCH_ttl_default", func(t *testing.T) {
		cases := []struct {
			name    string
			ns      string
			payload string
			wantTTL time.Duration
		}{
			{"absent ttl_ns defaults to permanent -1", "regttlns",
				`{"namespace":"regttlns","mem":false,"key_attrs":["id"]}`, -1},
			{"null ttl_ns defaults to permanent -1", "regttlnull",
				`{"namespace":"regttlnull","mem":false,"key_attrs":["id"],"ttl_ns":null}`, -1},
			{"explicit ttl_ns 0 stays 0", "regttlzero",
				`{"namespace":"regttlzero","mem":false,"key_attrs":["id"],"ttl_ns":0}`, 0},
			{"explicit ttl_ns 5s stays 5s", "regttl5s",
				`{"namespace":"regttl5s","mem":false,"key_attrs":["id"],"ttl_ns":5000000000}`, 5 * time.Second},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				require.Contains(t, runRESP([][]string{{"regsch", tc.payload}}), "+OK\r\n")
				spec, exists, err := s.db.loadDocSpec(naming.BuildStorageNs(tc.ns, false))
				require.NoError(t, err)
				require.True(t, exists)
				require.Equal(t, tc.wantTTL, spec.TTL)
			})
		}
	})

	t.Run("REGIDX_matrix", func(t *testing.T) {
		require.Contains(t, runRESP([][]string{{"regsch", sch("regidxowner", false, "id")}}), "+OK\r\n")
		require.Contains(t, runRESP([][]string{{"regsch", sch("regidxmem", true, "id")}}), "+OK\r\n")

		tests := []struct {
			name string
			cmds [][]string
			want string
		}{
			{"owner_ns plus logical", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"age","paths":["age"]}`},
			}, "+OK\r\n"},
			{"full_name form", [][]string{
				{"regidx", `{"full_name":"regidxowner:email","paths":["email"],"key_pattern":"regidxowner:*"}`},
			}, "+OK\r\n"},
			{"owner_doc_logical_ns disk", [][]string{
				{"regidx", `{"owner_doc_logical_ns":"regidxowner","logical":"org","paths":["org"]}`},
			}, "+OK\r\n"},
			{"owner_doc_logical_ns mem", [][]string{
				{"regidx", `{"owner_doc_logical_ns":"regidxmem","owner_mem":true,"logical":"age","paths":["age"]}`},
			}, "+OK\r\n"},
			{"mem owner_ns form", [][]string{
				{"regidx", `{"owner_ns":"_m_:regidxmem","logical":"email","paths":["email"]}`},
			}, "+OK\r\n"},
			{"default key_pattern omitted", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"nick","paths":["nick"]}`},
			}, "+OK\r\n"},
			{"idempotent re-register", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"dup","paths":["age"]}`},
				{"regidx", `{"owner_ns":"regidxowner","logical":"dup","paths":["age"]}`},
			}, "+OK\r\n+OK\r\n"},
			{"fingerprint upgrade paths change", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"upg","paths":["age"],"key_pattern":"regidxowner:*"}`},
				{"regidx", `{"owner_ns":"regidxowner","logical":"upg","paths":["age","email"],"key_pattern":"regidxowner:*"}`},
			}, "+OK\r\n+OK\r\n"},
			{"path dot to underscore", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"pfx","paths":["profile.email"]}`},
			}, "+OK\r\n"},
			{"argc too few", [][]string{{"regidx"}}, "wrong number of arguments for 'regidx'"},
			{"argc too many", [][]string{{"regidx", "a", "b"}}, "wrong number of arguments for 'regidx'"},
			{"invalid json", [][]string{{"regidx", "{bad"}}, "ERR REGIDX invalid JSON format"},
			{"full_name no separator", [][]string{
				{"regidx", `{"full_name":"regidxownerage","paths":["age"]}`},
			}, "ERR REGIDX index full name"},
			{"owner_ns without logical", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","paths":["age"]}`},
			}, "ERR REGIDX either full_name or (owner_ns + logical) is required"},
			{"no identity fields", [][]string{
				{"regidx", `{"paths":["age"]}`},
			}, "ERR REGIDX either full_name or owner_ns + logical is required"},
			{"owner_doc_logical_ns missing logical", [][]string{
				{"regidx", `{"owner_doc_logical_ns":"regidxowner","paths":["age"]}`},
			}, "ERR REGIDX logical is required"},
			{"owner_doc_logical_ns invalid", [][]string{
				{"regidx", `{"owner_doc_logical_ns":"_bad","logical":"age","paths":["age"]}`},
			}, "ERR REGIDX "},
			{"logical reserved chars", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"a-g-e","paths":["age"]}`},
			}, "ERR REGIDX logical index name"},
			{"logical underscore join char", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"a_b","paths":["age"]}`},
			}, "ERR REGIDX logical index name"},
			{"paths missing", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"nopaths"}`},
			}, `ERR REGIDX either "path" (string, single-field) or "paths" ([string], multi-field) is required`},
			{"paths empty entry", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"emptyp","paths":["age",""]}`},
			}, "ERR REGIDX paths[1] is empty or not a string"},
			{"key_pattern leading wildcard", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"wild","paths":["age"],"key_pattern":"*"}`},
			}, "key_pattern cannot start with wildcard"},
			{"single path legacy form", [][]string{
				{"regidx", `{"owner_ns":"regidxowner","logical":"single","path":"age"}`},
			}, "+OK\r\n"},
			{"owner_ns with owner_doc_logical_ns overrides storage ns", [][]string{
				{"regidx", `{"owner_ns":"ignoredraw","owner_doc_logical_ns":"regidxowner","paths":["age"]}`},
			}, "ERR REGIDX either full_name or (owner_ns + logical) is required"},
			{"owner_ns with owner_doc_logical_ns mem fallback", [][]string{
				{"regidx", `{"owner_ns":"ignoredraw","owner_doc_logical_ns":"regidxmem","owner_mem":true,"paths":["age"]}`},
			}, "ERR REGIDX either full_name or (owner_ns + logical) is required"},
			{"owner_ns with invalid owner_doc_logical_ns", [][]string{
				{"regidx", `{"owner_ns":"ignoredraw","owner_doc_logical_ns":"_bad","paths":["age"]}`},
			}, "ERR REGIDX "},
			{"owner_doc_logical_ns with invalid logical name", [][]string{
				{"regidx", `{"owner_doc_logical_ns":"regidxowner","logical":"a-g-e","paths":["age"]}`},
			}, "ERR REGIDX logical index name"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				resp := runRESP(tc.cmds)
				require.Contains(t, resp, tc.want, "resp=%s", resp)
			})
		}
	})

	t.Run("DROPSCH_DROPIDX_matrix", func(t *testing.T) {
		require.Contains(t, runRESP([][]string{
			{"regsch", sch("regdrop", false, "id")},
			{"regidx", `{"owner_ns":"regdrop","logical":"age","paths":["age"]}`},
			{"regsch", sch("regdropsolo", false, "id")},
		}), "+OK\r\n+OK\r\n+OK\r\n")

		tests := []struct {
			name string
			cmds [][]string
			want string
		}{
			{"dropsch blocked by attached index", [][]string{
				{"dropsch", "regdrop"},
			}, "still has 1 attached index(es)"},
			{"dropidx two-arg form", [][]string{
				{"dropidx", "regdrop", "age"},
			}, "+OK\r\n"},
			{"dropsch after index removed", [][]string{
				{"dropsch", "regdrop"},
			}, "+OK\r\n"},
			{"dropsch unregistered", [][]string{
				{"dropsch", "neverreg"},
			}, "not registered"},
			{"dropidx full name form", [][]string{
				{"regsch", sch("regdropfull", false, "id")},
				{"regidx", `{"owner_ns":"regdropfull","logical":"age","paths":["age"]}`},
				{"dropidx", "regdropfull:age"},
			}, "+OK\r\n+OK\r\n+OK\r\n"},
			{"dropidx unregistered", [][]string{
				{"dropidx", "neverreg", "age"},
			}, "not registered"},
			{"dropsch argc zero", [][]string{{"dropsch"}}, "wrong number of arguments for 'dropsch'"},
			{"dropsch argc too many", [][]string{{"dropsch", "a", "b"}}, "wrong number of arguments for 'dropsch'"},
			{"dropidx argc zero", [][]string{{"dropidx"}}, "wrong number of arguments for 'dropidx'"},
			{"dropidx argc too many", [][]string{{"dropidx", "a", "b", "c"}}, "wrong number of arguments for 'dropidx'"},
			{"dropsch solo schema", [][]string{
				{"dropsch", "regdropsolo"},
			}, "+OK\r\n"},
			{"dropsch invalid ns name", [][]string{
				{"dropsch", "_bad"},
			}, "ERR DROPSCH "},
			{"dropidx invalid ns name", [][]string{
				{"dropidx", "_bad", "age"},
			}, "ERR DROPIDX "},
			{"dropidx invalid logical name", [][]string{
				{"dropidx", "regidxowner", "a-g-e"},
			}, "ERR DROPIDX "},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				resp := runRESP(tc.cmds)
				require.Contains(t, resp, tc.want, "resp=%s", resp)
			})
		}
	})

	t.Run("DEL_doc_multipk_batch", func(t *testing.T) {
		require.Contains(t, runRESP([][]string{{"regsch", sch("regdel", false, "id")}}), "+OK\r\n")
		seed := [][]string{
			{"set", "regdel", `{"id":"d1","age":1}`},
			{"set", "regdel", `{"id":"d2","age":2}`},
			{"set", "regdel", `{"id":"d3","age":3}`},
		}
		require.Contains(t, runRESP(seed), "+OK\r\n+OK\r\n+OK\r\n")

		tests := []struct {
			name string
			cmds [][]string
			want string
		}{
			{"batch delete two", [][]string{{"del", "regdel", "d1", "d2"}}, ":2\r\n"},
			{"delete missing counts zero", [][]string{{"del", "regdel", "nope"}}, ":0\r\n"},
			{"mixed found and missing", [][]string{{"del", "regdel", "d3", "nope"}}, ":1\r\n"},
			{"duplicate suffix rejected", [][]string{{"del", "regdel", "d1", "d1"}}, "ERR DEL batch: duplicate key"},
			{"unregistered ns rejected", [][]string{{"del", "neverreg2", "x"}}, "not registered"},
			{"pk with colon rejected", [][]string{{"del", "regdel", "a:b"}}, "ERR DEL"},
			{"ns alone not a target", [][]string{{"del", "regdel"}}, "doc-pattern requires <ns> <pk-suffix>"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				resp := runRESP(tc.cmds)
				require.Contains(t, resp, tc.want, "resp=%s", resp)
			})
		}
	})

	t.Run("DB_level_CreateIndex", func(t *testing.T) {
		require.NoError(t, s.db.createIndex(x.RawIndex(
			naming.BuildIdxFullName("regidxowner", "dblvl"),
			"regidxowner:*",
			"age",
		)))
		err := s.db.createIndex(x.RawIndex("bogusns:cidx", "*", "age"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "key_pattern cannot start with wildcard")
	})

	t.Run("DB_level_deleteBatchAtomic_direct", func(t *testing.T) {
		n, err := s.db.deleteBatchAtomic(nil)
		require.NoError(t, err)
		require.Equal(t, 0, n)

		_, err = s.db.deleteBatchAtomic([]string{"regdel:d1", "_m_:regdel:d1"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot span storage layers")

		_, err = s.db.deleteBatchAtomic([]string{"regdel:dup", "regdel:dup"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate key")
	})
}

// bootServerFor starts a fresh dual-port server for one test method and
// returns the ctrl/app addresses plus the db handle. Empty auth seeds are
// replaced with defaults so bootstrapAuth never generates random keys
// (which would force NOAUTH on every unauthenticated connection).
func (s *CmdTestSuite) bootServerFor(appAuth, ctrlAuth string) (appAddr, ctrlAddr string, db *DB) {
	t := s.T()
	if appAuth == "" {
		appAuth = "matrix-app-key"
	}
	if ctrlAuth == "" {
		ctrlAuth = "matrix-ctrl-key"
	}
	dbPath := testutil.DBPath(t)
	appPort, ctrlPort := testutil.AllocateTwoFreePorts(t)
	cfg := &Config{
		DataPath: dbPath,
		App:      AppConfig{Bind: "127.0.0.1", Port: appPort, Auth: appAuth},
		Ctrl:     CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort, Auth: ctrlAuth},
	}
	db = StartWith(cfg)
	if db == nil {
		t.Fatalf("StartWith returned nil; appPort=%d ctrlPort=%d", appPort, ctrlPort)
	}
	return cfg.App.Addr(), cfg.Ctrl.Addr(), db
}

// TestWireProtocolBasics covers the handshake/status command family
// (HELLO, CLIENT, PING) plus SUBSCRIBE/PSUBSCRIBE/PUBLISH argv errors and
// a full pattern-subscribe → publish delivery round trip.
func (s *CmdTestSuite) TestWireProtocolBasics() {
	_, ctrlAddr, _ := s.bootServerFor("", "")
	ctrlAuth := "matrix-ctrl-key"

	tests := []struct {
		name string
		cmds [][]string
		want string
	}{
		{"hello", [][]string{{"hello"}}, "redisx"},
		{"client setinfo", [][]string{{"client", "setinfo", "lib-name", "redisx-test"}}, "+OK\r\n"},
		{"ping", [][]string{{"ping"}}, "+PONG\r\n"},
		{"publish argc too few", [][]string{{"publish", "chan"}}, "wrong number of arguments for 'publish'"},
		{"publish argc too many", [][]string{{"publish", "chan", "msg", "extra"}}, "wrong number of arguments for 'publish'"},
		{"subscribe argc zero", [][]string{{"subscribe"}}, "wrong number of arguments for 'subscribe'"},
		{"psubscribe argc zero", [][]string{{"psubscribe"}}, "wrong number of arguments for 'psubscribe'"},
		{"unknown command", [][]string{{"nosuchcmd"}}, "unknown command"},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			resp := respRoundTrip(s.T(), ctrlAddr, ctrlAuth, tc.cmds)
			s.Contains(resp, tc.want, "resp=%s", resp)
		})
	}

	// Subscriber connections are locked to (P)SUBSCRIBE commands after the
	// first subscription, so pushes are asserted with a two-connection
	// harness: conn A subscribes, conn B publishes.
	pubsubPush := func(subCmd, channel, pattern, pushKind string) {
		t := s.T()
		sub, err := net.Dial("tcp", ctrlAddr)
		if err != nil {
			t.Fatalf("dial subscriber: %v", err)
		}
		defer func() { _ = sub.Close() }()
		b := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(ctrlAuth), ctrlAuth)
		_, _ = sub.Write([]byte(b))
		buf := make([]byte, 4096)
		_ = sub.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
		_, _ = sub.Read(buf)

		var w strings.Builder
		fmt.Fprintf(&w, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(subCmd), subCmd, len(pattern), pattern)
		_, _ = sub.Write([]byte(w.String()))
		_ = sub.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
		n, _ := sub.Read(buf)
		confirm := string(buf[:n])
		s.Contains(confirm, subCmd)

		pub := respRoundTrip(t, ctrlAddr, ctrlAuth, [][]string{{"publish", channel, "hello"}})
		s.Contains(pub, ":1\r\n")

		_ = sub.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
		n, _ = sub.Read(buf)
		push := string(buf[:n])
		s.Contains(push, pushKind)
		s.Contains(push, channel)
		s.Contains(push, "hello")
	}

	s.Run("psubscribe then publish delivers push", func() {
		pubsubPush("psubscribe", "news.tech", "news.*", "pmessage")
	})

	s.Run("subscribe then publish delivers push", func() {
		pubsubPush("subscribe", "alerts", "alerts", "message")
	})
}

// TestSetCommandFlagsMatrix covers SET/SETEX/SETNX/GET/DEL argv shapes,
// TTL flag parsing (EX/PX, bad values), NX semantics, the internal-NS
// write guard, and the doc-path branch (unregistered ns, bad JSON,
// array-of-scalars, scalar value, missing pk attrs).
func (s *CmdTestSuite) TestSetCommandFlagsMatrix() {
	_, ctrlAddr, db := s.bootServerFor("", "")
	ctrlAuth := "matrix-ctrl-key"
	require.NoError(s.T(), db.writeDocSpec(docSpec{
		Namespace: "flagdoc",
		KeyAttrs:  []string{"id"},
	}))
	rt := func(cmds [][]string) string { return respRoundTrip(s.T(), ctrlAddr, ctrlAuth, cmds) }

	tests := []struct {
		name string
		cmds [][]string
		want string
	}{
		{"set argc too few", [][]string{{"set", "flagkv:k1"}}, "wrong number of arguments for 'set'"},
		{"set plain", [][]string{{"set", "flagkv:a", "v1"}}, "+OK\r\n"},
		{"set ex ok", [][]string{{"set", "flagkv:b", "v2", "EX", "60"}}, "+OK\r\n"},
		{"set px ok", [][]string{{"set", "flagkv:c", "v3", "PX", "5000"}}, "+OK\r\n"},
		{"set ex not integer", [][]string{{"set", "flagkv:d", "v", "EX", "bad"}}, "ERR value is not an integer or out of range"},
		{"set px not integer", [][]string{{"set", "flagkv:e", "v", "PX", "bad"}}, "ERR value is not an integer or out of range"},
		{"set nx fresh", [][]string{{"set", "flagkv:nx1", "v", "NX"}}, "+OK\r\n"},
		{"set nx existing returns null", [][]string{
			{"set", "flagkv:nx1", "orig", "NX"},
			{"set", "flagkv:nx1", "v2", "NX"},
		}, "$-1\r\n"},
		{"set internal ns guard", [][]string{{"set", "_doc_:x", "v"}}, "ERR SET "},
		{"set unregistered ns json intent", [][]string{{"set", "nope", `{"id":1}`}}, "not registered — doc-pattern requires REGSCH first"},
		{"set unregistered ns kv intent", [][]string{{"set", "nope", "plain"}}, "kv-pattern key must contain namespace separator"},
		{"set doc invalid json", [][]string{{"set", "flagdoc", "{bad"}}, "value must be valid JSON"},
		{"set doc array of scalars", [][]string{{"set", "flagdoc", "[1,2]"}}, "array must contain JSON objects"},
		{"set doc scalar value", [][]string{{"set", "flagdoc", "123"}}, "value must be JSON object or array of objects"},
		{"set doc missing pk attr", [][]string{{"set", "flagdoc", `{"name":"x"}`}}, "object[0]"},
		{"set doc happy", [][]string{{"set", "flagdoc", `{"id":"d1","age":1}`}}, "+OK\r\n"},
		{"set doc batch happy", [][]string{{"set", "flagdoc", `[{"id":"d2"},{"id":"d3"}]`}}, "+OK\r\n"},
		{"setex argc too few", [][]string{{"setex", "flagkv:f", "60"}}, "wrong number of arguments for 'setex'"},
		{"setex bad seconds", [][]string{{"setex", "flagkv:f", "bad", "v"}}, "ERR value is not an integer or out of range"},
		{"setex ok", [][]string{{"setex", "flagkv:f", "60", "v"}}, "+OK\r\n"},
		{"setex doc happy", [][]string{{"setex", "flagdoc", "60", `{"id":"d4"}`}}, "+OK\r\n"},
		{"setex doc invalid json", [][]string{{"setex", "flagdoc", "60", "{bad"}}, "ERR SETEX doc-pattern: value must be valid JSON"},
		{"setnx argc too few", [][]string{{"setnx", "flagkv:g"}}, "wrong number of arguments for 'setnx'"},
		{"setnx fresh then duplicate", [][]string{
			{"setnx", "flagkv:g", "v1"},
			{"setnx", "flagkv:g", "v2"},
		}, ":1\r\n:0\r\n"},
		{"setnx internal ns guard", [][]string{{"setnx", "_idx_:x", "v"}}, "ERR SETNX "},
		{"get happy", [][]string{{"set", "flagkv:h", "vv"}, {"get", "flagkv:h"}}, "$2\r\nvv\r\n"},
		{"get missing returns null", [][]string{{"get", "flagkv:none"}}, "$-1\r\n"},
		{"get argc zero", [][]string{{"get"}}, "wrong number of arguments for 'get'"},
		{"get kv argc too many", [][]string{{"get", "flagkv:h", "extra"}}, "kv-pattern accepts exactly 1 key argument"},
		{"get kv reserved mem prefix", [][]string{{"get", "_m_x:1"}}, "must not start with reserved internal prefix"},
		{"get doc pk suffix with colon", [][]string{{"get", "flagdoc", "a:b"}}, "ERR GET pk suffix must not contain namespace separator"},
		{"get doc unregistered ns", [][]string{{"get", "nope", "x"}}, "not registered"},
		{"get doc happy", [][]string{
			{"set", "flagdoc", `{"id":"d5"}`},
			{"get", "flagdoc", "d5"},
		}, `"id":"d5"`},
		{"set doc nx fresh returns ok", [][]string{{"set", "flagdoc", `{"id":"nx1"}`, "NX"}}, "+OK\r\n"},
		{"set doc nx duplicate returns null", [][]string{
			{"set", "flagdoc", `{"id":"nx2"}`, "NX"},
			{"set", "flagdoc", `{"id":"nx2"}`, "NX"},
		}, "$-1\r\n"},
		{"set doc nx batch duplicate pk errors", [][]string{
			{"set", "flagdoc", `[{"id":"nx3"},{"id":"nx3"}]`, "NX"},
		}, `ERR SET batch: duplicate key "flagdoc:nx3"`},
		{"setex argc too many", [][]string{{"setex", "flagkv:f", "60", "v", "x"}}, "wrong number of arguments for 'setex'"},
		{"setex internal ns guard", [][]string{{"setex", "_doc_:x", "60", "v"}}, "ERR SETEX "},
		{"setex unregistered ns json intent", [][]string{{"setex", "nope", "60", `{"id":1}`}}, "not registered — doc-pattern requires REGSCH first"},
		{"setex unregistered ns kv intent", [][]string{{"setex", "nope", "60", "plain"}}, "kv-pattern key must contain namespace separator"},
		{"setex doc array of scalars", [][]string{{"setex", "flagdoc", "60", "[1,2]"}}, "array must contain JSON objects"},
		{"setex doc scalar value", [][]string{{"setex", "flagdoc", "60", "123"}}, "value must be JSON object or array of objects"},
		{"setex doc missing pk attr", [][]string{{"setex", "flagdoc", "60", `{"name":"x"}`}}, "object[0]"},
		{"setnx doc happy then duplicate", [][]string{
			{"setnx", "flagdoc", `{"id":"sx1"}`},
			{"setnx", "flagdoc", `{"id":"sx1"}`},
		}, ":1\r\n:0\r\n"},
		{"setnx unregistered ns json intent", [][]string{{"setnx", "nope", `{"id":1}`}}, "not registered — doc-pattern requires REGSCH first"},
		{"setnx unregistered ns kv intent", [][]string{{"setnx", "nope", "plain"}}, "kv-pattern key must contain namespace separator"},
		{"setnx doc invalid json", [][]string{{"setnx", "flagdoc", "{bad"}}, "ERR SETNX doc-pattern: value must be valid JSON"},
		{"setnx doc array of scalars", [][]string{{"setnx", "flagdoc", "[1,2]"}}, "array must contain JSON objects"},
		{"setnx doc scalar value", [][]string{{"setnx", "flagdoc", "123"}}, "value must be JSON object or array of objects"},
		{"setnx doc missing pk attr", [][]string{{"setnx", "flagdoc", `{"name":"x"}`}}, "object[0]"},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			resp := rt(tc.cmds)
			s.Contains(resp, tc.want, "resp=%s", resp)
		})
	}
}

// TestSearchArgvMatrix covers SEARCHKEY / SEARCHINDEX argv bounds, order /
// LIMIT tail parsing errors, key-range validation, and filter JSON errors.
func (s *CmdTestSuite) TestSearchArgvMatrix() {
	_, ctrlAddr, db := s.bootServerFor("", "")
	ctrlAuth := "matrix-ctrl-key"
	require.NoError(s.T(), db.writeDocSpec(docSpec{
		Namespace: "srchdoc",
		KeyAttrs:  []string{"id"},
	}))
	require.NoError(s.T(), db.writeIndexSpec(idxSpec{
		OwnerNs:    "srchdoc",
		Logical:    "age",
		KeyPattern: "srchdoc:*",
		Paths:      []string{"age"},
	}))
	rt := func(cmds [][]string) string { return respRoundTrip(s.T(), ctrlAddr, ctrlAuth, cmds) }
	kr := `{"op":"pattern","p":"srchdoc:*"}`

	tests := []struct {
		name string
		cmds [][]string
		want string
	}{
		{"searchkey argc zero", [][]string{{"searchkey"}}, "wrong number of arguments for 'searchkey'"},
		{"searchkey argc one", [][]string{{"searchkey", kr}}, "wrong number of arguments for 'searchkey'"},
		{"searchkey argc six", [][]string{{"searchkey", kr, "{}", "ASC", "LIMIT", "1", "x"}}, "wrong number of arguments for 'searchkey'"},
		{"searchkey invalid kr json", [][]string{{"searchkey", "{bad", "{}"}}, "ERR invalid key range"},
		{"searchkey leading wildcard", [][]string{{"searchkey", `{"op":"pattern","p":"*x"}`, "{}"}}, "must be anchored to a namespace"},
		// NOTE: the "not registered — doc-pattern requires REGSCH first"
		// branch in searchKeyCommand is defensive dead code: it only fires
		// for colon-free anchors, but storageNsFromKRAnchor always errors
		// on anchors without the ":" separator. Actual behaviour for an
		// unregistered colon-free range is an empty result.
		{"searchkey unregistered colon-free ns returns empty", [][]string{{"searchkey", `{"op":"gte","pivot":"norp"}`, "{}"}}, "*0\r\n"},
		{"searchkey bad order", [][]string{{"searchkey", kr, "{}", "SIDEWAYS"}}, "ERR invalid order: SIDEWAYS"},
		{"searchkey order word instead of limit", [][]string{{"searchkey", kr, "{}", "5"}}, "ERR invalid order: 5"},
		{"searchkey limit without keyword", [][]string{{"searchkey", kr, "{}", "TAKE", "1"}}, "ERR invalid argument: TAKE"},
		{"searchkey limit bad count", [][]string{{"searchkey", kr, "{}", "LIMIT", "0"}}, "ERR invalid count for LIMIT: 0"},
		{"searchkey order then bad limit kw", [][]string{{"searchkey", kr, "{}", "ASC", "TAKE", "1"}}, "ERR invalid argument: TAKE"},
		{"searchkey invalid filter json", [][]string{{"searchkey", kr, "{bad"}}, "ERR invalid query"},
		{"searchindex argc too few", [][]string{{"searchindex", "srchdoc:age", kr}}, "wrong number of arguments for 'searchindex'"},
		{"searchindex argc too many", [][]string{{"searchindex", "srchdoc:age", kr, "{}", "ASC", "LIMIT", "1", "x"}}, "wrong number of arguments for 'searchindex'"},
		{"searchindex kr not json", [][]string{{"searchindex", "srchdoc:age", "notjson", "{}"}}, "wrong number of arguments for 'searchindex'"},
		{"searchindex kr invalid json", [][]string{{"searchindex", "srchdoc:age", "{bad", "{}"}}, "ERR invalid key range"},
		{"searchindex invalid index name", [][]string{{"searchindex", "nostrategy", kr, "{}"}}, "ERR SEARCHINDEX invalid index name"},
		{"searchindex unregistered owner ns", [][]string{{"searchindex", "nope:age", kr, "{}"}}, "not registered"},
		{"searchindex six args bad order", [][]string{{"searchindex", "srchdoc:age", kr, "{}", "SIDEWAYS", "LIMIT", "1"}}, "ERR invalid order: SIDEWAYS"},
		{"searchindex six args bad limit keyword", [][]string{{"searchindex", "srchdoc:age", kr, "{}", "ASC", "TAKE", "1"}}, "ERR invalid argument: TAKE"},
		{"searchindex six args bad count", [][]string{{"searchindex", "srchdoc:age", kr, "{}", "ASC", "LIMIT", "0"}}, "ERR invalid count for LIMIT: 0"},
		{"searchkey five args bad order", [][]string{{"searchkey", kr, "{}", "5", "LIMIT", "1"}}, "ERR invalid order: 5"},
		{"searchkey five args bad limit keyword", [][]string{{"searchkey", kr, "{}", "ASC", "TAKE", "1"}}, "ERR invalid argument: TAKE"},
		{"searchkey five args bad count", [][]string{{"searchkey", kr, "{}", "ASC", "LIMIT", "0"}}, "ERR invalid count for LIMIT: 0"},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			resp := rt(tc.cmds)
			s.Contains(resp, tc.want, "resp=%s", resp)
		})
	}

	s.Run("searchkey happy with tail variants", func() {
		resp := rt([][]string{
			{"set", "srchdoc", `{"id":"s1","age":10}`},
			{"set", "srchdoc", `{"id":"s2","age":20}`},
			{"searchkey", kr, "{}", "ASC"},
			{"searchkey", kr, "{}", "LIMIT", "1"},
			{"searchkey", kr, "{}", "DESC", "LIMIT", "1"},
		})
		s.Contains(resp, `{"id":"s1","age":10}`)
		s.Contains(resp, `{"id":"s2","age":20}`)
	})

	s.Run("searchindex happy with order and limit", func() {
		resp := rt([][]string{
			{"searchindex", "srchdoc:age", kr, "{}", "ASC", "LIMIT", "1"},
			{"searchindex", "srchdoc:age", kr, "{}", "DESC", "LIMIT", "1"},
		})
		s.Contains(resp, `{"id":"s1","age":10}`)
		s.Contains(resp, `{"id":"s2","age":20}`)
	})
}

// TestKeysArgvMatrix covers KEYS argv bounds, the leading-wildcard /
// reserved-namespace guards, registry introspection expansions (_doc_ /
// _idx_), doc-path bare-namespace validation, and the empty-pattern
// resolveLayer error.
func (s *CmdTestSuite) TestKeysArgvMatrix() {
	_, ctrlAddr, db := s.bootServerFor("", "")
	ctrlAuth := "matrix-ctrl-key"
	require.NoError(s.T(), db.writeDocSpec(docSpec{
		Namespace: "keysdoc",
		KeyAttrs:  []string{"id"},
	}))
	require.NoError(s.T(), db.writeIndexSpec(idxSpec{
		OwnerNs:    "keysdoc",
		Logical:    "age",
		KeyPattern: "keysdoc:*",
		Paths:      []string{"age"},
	}))
	rt := func(cmds [][]string) string { return respRoundTrip(s.T(), ctrlAddr, ctrlAuth, cmds) }
	require.Contains(s.T(), rt([][]string{{"set", "keysdoc", `{"id":"k1","age":1}`}}), "+OK\r\n")

	tests := []struct {
		name string
		cmds [][]string
		want string
	}{
		{"keys argc zero", [][]string{{"keys"}}, "wrong number of arguments for 'keys'"},
		{"keys argc too many", [][]string{{"keys", "a", "b"}}, "wrong number of arguments for 'keys'"},
		{"keys empty pattern", [][]string{{"keys", ""}}, "ERR KEYS storage key or pattern is required"},
		{"keys leading wildcard", [][]string{{"keys", "*x"}}, "ERR forbidden key pattern"},
		{"keys auth ns forbidden", [][]string{{"keys", "_auth_:x"}}, "ERR forbidden key pattern"},
		{"keys bare idx ns expands", [][]string{{"keys", "_idx_"}}, "_idx_"},
		{"keys bare doc ns expands", [][]string{{"keys", "_doc_"}}, "_doc_"},
		{"keys doc ns glob forbidden", [][]string{{"keys", "abc*"}}, "ERR KEYS for doc-path requires bare"},
		{"keys bare unregistered ns", [][]string{{"keys", "notregns"}}, "not registered"},
		{"keys bare registered ns lists storage keys", [][]string{{"keys", "keysdoc"}}, "keysdoc:k1"},
		{"keys kv pattern passthrough", [][]string{{"keys", "keysdoc:*"}}, "keysdoc:k1"},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			resp := rt(tc.cmds)
			s.Contains(resp, tc.want, "resp=%s", resp)
		})
	}
}

// TestUpdateArgvMatrix covers UPDATE argv bounds, key-range validation
// (invalid JSON, leading-wildcard anchor), the LIMIT tail (bad keyword,
// bad count, happy), filter JSON errors, and update-JSON parse errors.
func (s *CmdTestSuite) TestUpdateArgvMatrix() {
	_, ctrlAddr, db := s.bootServerFor("", "")
	ctrlAuth := "matrix-ctrl-key"
	require.NoError(s.T(), db.writeDocSpec(docSpec{
		Namespace: "upddoc",
		KeyAttrs:  []string{"id"},
	}))
	rt := func(cmds [][]string) string { return respRoundTrip(s.T(), ctrlAddr, ctrlAuth, cmds) }
	require.Contains(s.T(), rt([][]string{
		{"set", "upddoc", `{"id":"u1","age":10}`},
		{"set", "upddoc", `{"id":"u2","age":20}`},
	}), "+OK\r\n+OK\r\n")
	upd := `[{"op":"replace","path":"/age","value":21}]`

	tests := []struct {
		name string
		cmds [][]string
		want string
	}{
		{"update argc too few", [][]string{{"update", "upddoc:*", "{}"}}, "wrong number of arguments for 'update'"},
		{"update argc too many", [][]string{{"update", "upddoc:*", "{}", upd, "LIMIT", "1", "x"}}, "wrong number of arguments for 'update'"},
		{"update invalid kr json", [][]string{{"update", "{bad", "{}", upd}}, "ERR invalid key range"},
		{"update leading wildcard anchor", [][]string{{"update", `{"op":"pattern","p":"*x"}`, "{}", upd}}, "must be anchored to a registered storage namespace"},
		{"update limit bad keyword", [][]string{{"update", "upddoc:*", "{}", upd, "TAKE", "1"}}, "ERR invalid argument: TAKE"},
		{"update limit bad count", [][]string{{"update", "upddoc:*", "{}", upd, "LIMIT", "0"}}, "ERR invalid count for LIMIT: 0"},
		{"update invalid filter json", [][]string{{"update", "upddoc:*", "{bad", upd}}, "ERR invalid query"},
		{"update invalid update json", [][]string{{"update", "upddoc:*", "{}", "{bad"}}, "ERR"},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			resp := rt(tc.cmds)
			s.Contains(resp, tc.want, "resp=%s", resp)
		})
	}

	s.Run("update happy with limit", func() {
		resp := rt([][]string{{"update", "upddoc:*", `{"age":{"$gt":0}}`, upd, "LIMIT", "1"}})
		s.Contains(resp, "upddoc:")
	})
}

// TestPortRoleGates covers the auth gates over the wire: AUTH role
// mismatch (WRONGPASS on the wrong port), re-AUTH semantics, gate1
// (_auth_ namespace + wildcard KEYS on app port), gate3 (ctrl-only
// commands on app port) and NOAUTH for unauthenticated commands.
func (s *CmdTestSuite) TestPortRoleGates() {
	appAuth, ctrlAuth := "gate-app-key", "gate-ctrl-key"
	appAddr, ctrlAddr, _ := s.bootServerFor(appAuth, ctrlAuth)

	s.Run("auth argc wrong closes conn", func() {
		resp := respRoundTrip(s.T(), appAddr, "", [][]string{{"auth", appAuth, "extra"}})
		s.Contains(resp, "ERR authentication failed")
	})

	s.Run("auth with ctrl key on app port rejected", func() {
		resp := respRoundTrip(s.T(), appAddr, "", [][]string{{"auth", ctrlAuth}})
		s.Contains(resp, "WRONGPASS invalid or wrong auth key for app port")
	})

	s.Run("auth with app key on ctrl port rejected", func() {
		resp := respRoundTrip(s.T(), ctrlAddr, "", [][]string{{"auth", appAuth}})
		s.Contains(resp, "WRONGPASS invalid or wrong auth key for ctrl port")
	})

	s.Run("reauth same key ok different key rejected", func() {
		resp := respRoundTrip(s.T(), ctrlAddr, ctrlAuth, [][]string{
			{"auth", ctrlAuth},
			{"auth", "another-key"},
		})
		s.Contains(resp, "+OK\r\n")
		s.Contains(resp, "ERR connection already authenticated")
	})

	s.Run("unauthenticated command gets NOAUTH", func() {
		resp := respRoundTrip(s.T(), appAddr, "", [][]string{{"get", "somekey"}})
		s.Contains(resp, "NOAUTH authentication required")
	})

	s.Run("gate1 literal auth ns key on app port", func() {
		resp := respRoundTrip(s.T(), appAddr, appAuth, [][]string{{"get", "_auth_:app_0"}})
		s.Contains(resp, "No Privilege")
	})

	s.Run("gate1 wildcard keys sweep on app port", func() {
		resp := respRoundTrip(s.T(), appAddr, appAuth, [][]string{{"keys", "user:*"}})
		s.Contains(resp, "No Privilege")
	})

	s.Run("gate3 ctrl-only command on app port", func() {
		resp := respRoundTrip(s.T(), appAddr, appAuth, [][]string{{"regsch", `{"namespace":"g","key_attrs":["id"]}`}})
		s.Contains(resp, "only allowed on the ctrl port")
	})

	s.Run("app port kv round trip works with correct key", func() {
		resp := respRoundTrip(s.T(), appAddr, appAuth, [][]string{
			{"set", "gatekv:a", "v"},
			{"get", "gatekv:a"},
		})
		s.Contains(resp, "+OK\r\n")
		s.Contains(resp, "$1\r\nv\r\n")
	})
}
