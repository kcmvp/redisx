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

type CmdXTestSuite struct {
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

func (s *CmdXTestSuite) SetupSuite() {
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

func (s *CmdXTestSuite) SetupTest() {
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

func (s *CmdXTestSuite) TearDownTest() {
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

func TestCmdXSuite(t *testing.T) {
	suite.Run(t, new(CmdXTestSuite))
}

func (s *CmdXTestSuite) TestParseFilter() {
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

func (s *CmdXTestSuite) TestCmdX() {
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
			name:       "searchindex_wrong_number_of_args_01",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user_idx", "age"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchindex' command"},
		},
		{
			name:       "searchindex_invalid_order_01",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user_idx", "age", "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchindex_invalid_json_01",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user_idx", "age", "{invalid"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchkey_wrong_number_of_args_01",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "user_key", "*"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchkey' command"},
		},
		{
			name:       "searchkey_invalid_order_01",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "user_key", "*", "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchkey_invalid_json_01",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "user_key", "*", "{invalid"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchindex_wrong_number_of_args",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "schema", "attr"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchindex' command"},
		},
		{
			name:       "searchindex_invalid_order_02",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "schema", "attr", "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchindex_invalid_json_02",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "schema", "attr", "{invalid}", "ASC"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchkey_wrong_number_of_args_02",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "schema", "pattern"}},
			wantErrors: []string{"ERR wrong number of arguments for 'searchkey' command"},
		},
		{
			name:       "searchkey_invalid_order_02",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "schema", "pattern", "{}", "INVALID"}},
			wantErrors: []string{"ERR invalid order: INVALID"},
		},
		{
			name:       "searchkey_invalid_json_02",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "schema", "pattern", "{invalid}", "ASC"}},
			wantErrors: []string{"ERR invalid query: invalid JSON filter format"},
		},
		{
			name:       "searchindex success",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user_idx", "age", "{}", "ASC"}},
			wantArrays: [][]string{{`{"id":"1_{id}", "age":20}`, `{"id":"2_{id}", "age":30}`}},
			setupDB: func(uid string) {

				_ = s.db.Save(s.schemas[0], fmt.Sprintf(`{"id":"1_%s", "age":20}`, uid))
				_ = s.db.Save(s.schemas[0], fmt.Sprintf(`{"id":"2_%s", "age":30}`, uid))
			},
		},
		{
			name:       "searchindex not found",
			auth:       true,
			commands:   [][]string{{cmdSearchIndex, "user_no_idx", "unknown", "{}", "ASC"}},
			wantErrors: []string{"ERR index unknown not found for schema user_no_idx"},
			setupDB: func(uid string) {
			},
		},
		{
			name:       "update success",
			auth:       true,
			commands:   [][]string{{cmdUpdate, "user_idx", `{"id": "1_{id}"}`, `{"name": "updated"}`}},
			wantArrays: [][]string{{`user_idx:1_{id}`}},
			setupDB: func(uid string) {
				// use dynamic uid so we don't accidentally update docs from previous tests
				_ = s.db.Save(s.schemas[0], fmt.Sprintf(`{"id":"1_%s", "age":20, "name":"old"}`, uid))
				_ = s.db.Save(s.schemas[0], fmt.Sprintf(`{"id":"2_%s", "age":30, "name":"old"}`, uid))
			},
		},
		{
			name:       "update no valid updates",
			auth:       true,
			commands:   [][]string{{cmdUpdate, "user_idx", "{}", `{}`}},
			wantErrors: []string{"ERR no valid updates provided"},
		},
		{
			name:       "update invalid json",
			auth:       true,
			commands:   [][]string{{cmdUpdate, "user_idx", "{}", `{invalid`}},
			wantErrors: []string{"ERR invalid update json format"},
		},
		{
			name:       "searchkey success",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "user_key", "*_{id}", "{}", "DESC"}},
			wantArrays: [][]string{{`{"id":"2_{id}"}`, `{"id":"1_{id}"}`}},
			setupDB: func(uid string) {

				_ = s.db.Save(s.schemas[1], fmt.Sprintf(`{"id":"1_%s"}`, uid))
				_ = s.db.Save(s.schemas[1], fmt.Sprintf(`{"id":"2_%s"}`, uid))
			},
		},
		{
			name:       "searchkey not found",
			auth:       true,
			commands:   [][]string{{cmdSearchKey, "user_key", "unknown_{id}:*", "{}"}},
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
