package raw_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/kcmvp/redisx/client/internal/conn"
	"github.com/kcmvp/redisx/client/internal/hook"
	"github.com/kcmvp/redisx/client/raw"
	"github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/server"
	"github.com/kcmvp/redisx/x"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
)

var rawTestServerAddr string

const rawTestAuthKey = "raw-test-external-key"
const rawTestAuthLimit = "50"

type rawTestUserDoc string

func (rawTestUserDoc) Namespace() string  { return "user" }
func (rawTestUserDoc) Mem() bool          { return false }
func (rawTestUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u rawTestUserDoc) RawJSON() string  { return string(u) }
func (rawTestUserDoc) TTL() time.Duration { return 0 }

type wireSchemaJSON struct {
	Namespace string        `json:"namespace"`
	Mem       bool          `json:"mem"`
	KeyAttrs  []string      `json:"key_attrs"`
	TTL       time.Duration `json:"ttl_ns"`
}

type wireIndexJSON struct {
	FullName   string   `json:"full_name"`
	KeyPattern string   `json:"key_pattern"`
	Paths      []string `json:"paths"`
}

func seedRawTestServer(t *testing.T, db *server.DB, addr string) {
	t.Helper()
	req := require.New(t)

	req.NoError(db.Set(naming.AuthStorageKey(rawTestAuthKey), rawTestAuthLimit))

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: rawTestAuthKey})
	defer func() { _ = rdb.Close() }()

	wireREGSCH := func(ns string, mem bool, keyAttrs []string, ttl time.Duration) {
		t.Helper()
		b, err := json.Marshal(wireSchemaJSON{Namespace: ns, Mem: mem, KeyAttrs: keyAttrs, TTL: ttl})
		req.NoError(err)
		status := rdb.Do(ctx, "REGSCH", string(b))
		req.NoError(status.Err())
		req.Equal("OK", status.Val())
	}
	wireREGIDX := func(idx x.Index) {
		t.Helper()
		paths := idx.Paths()
		if paths == nil {
			paths = []string{}
		}
		b, err := json.Marshal(wireIndexJSON{FullName: idx.Name(), KeyPattern: idx.KeyPattern(), Paths: paths})
		req.NoError(err)
		status := rdb.Do(ctx, "REGIDX", string(b))
		req.NoError(status.Err())
		req.Equal("OK", status.Val())
	}

	wireREGSCH("user", false, []string{"id"}, 0)

	wireREGIDX(x.Idx[rawTestUserDoc]("age", "*", "age"))
	wireREGIDX(x.Idx[rawTestUserDoc]("email", "*", "email"))
}

type RawTestSuite struct {
	suite.Suite
}

func (s *RawTestSuite) SetupSuite() {
	s.T().Setenv("HOME", s.T().TempDir())
	dbPath := filepath.Join(s.T().TempDir(), "redisx_raw_test.db")

	appPort, ctrlPort := testutil.AllocateTwoFreePorts(s.T())
	cfg := &server.Config{
		DataPath: dbPath,
		App:      server.AppConfig{Bind: "127.0.0.1", Port: appPort},
		Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort},
	}
	rawTestServerAddr = cfg.Ctrl.Addr()
	db := server.StartWith(cfg, rawTestUserDoc(""))
	s.Require().NotNil(db)

	seedRawTestServer(s.T(), db, rawTestServerAddr)
}

func (s *RawTestSuite) TearDownSuite() {
	if prev := conn.SetSharedClient(nil); prev != nil {
		_ = prev.Close()
	}
	_ = server.Stop()
	rawTestServerAddr = ""
}

func (s *RawTestSuite) SetupTest() {
	if prev := conn.SetSharedClient(nil); prev != nil {
		_ = prev.Close()
	}
	hook.Reset()
}

func (s *RawTestSuite) connectRaw() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     rawTestServerAddr,
		Password: rawTestAuthKey,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := rdb.Ping(ctx).Err()
	s.Require().NoError(err, "failed to connect to test server")
	conn.SetSharedClient(rdb)
}

func TestRawSuite(t *testing.T) {
	suite.Run(t, new(RawTestSuite))
}

func (s *RawTestSuite) TestGetSetCommand() {
	tests := []struct {
		name       string
		setup      func()
		action     func() (string, error)
		expectErr  bool
		wantErrMsg string
		expectVal  string
	}{
		{
			"Get not connected",
			nil,
			func() (string, error) { return raw.Get("cli:k") },
			true, "resp client is not connected", "",
		},
		{
			"Set not connected",
			nil,
			func() (string, error) { return "", raw.Set("cli:k", "v") },
			true, "resp client is not connected", "",
		},
		{
			"Get empty key",
			func() { s.connectRaw() },
			func() (string, error) { return raw.Get("") },
			false, "", "",
		},
		{
			"Set empty key",
			func() { s.connectRaw() },
			func() (string, error) { return "", raw.Set("", "v") },
			false, "", "",
		},
		{
			"Set and Get Success",
			func() { s.connectRaw() },
			func() (string, error) {
				if err := raw.Set("raw:external-key", "external-value"); err != nil {
					return "", err
				}
				return raw.Get("raw:external-key")
			},
			false, "", "external-value",
		},
		{
			"SetWithTTL Success and Expire",
			func() { s.connectRaw() },
			func() (string, error) {
				if err := raw.SetWithTTL("raw:ttl_key", "ttl_val", 100*time.Millisecond); err != nil {
					return "", err
				}
				v1, _ := raw.Get("raw:ttl_key")
				if v1 != "ttl_val" {
					return v1, nil
				}
				time.Sleep(200 * time.Millisecond)
				v2, err := raw.Get("raw:ttl_key")
				return v2, err
			},
			true, "redis: nil", "",
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			s.SetupTest()
			if tc.setup != nil {
				tc.setup()
			}

			val, err := tc.action()
			if tc.expectErr {
				s.Error(err)
				if tc.wantErrMsg != "" && err != nil {
					s.Contains(err.Error(), tc.wantErrMsg)
				}
			} else {
				s.NoError(err)
				s.Equal(tc.expectVal, val)
			}
		})
	}
}

func (s *RawTestSuite) TestCrudCommands() {
	s.Run("SetNXWithTTL not connected", func() {
		s.SetupTest()
		ok, err := raw.SetNXWithTTL("raw:k", "v", time.Second)
		s.Error(err)
		s.Contains(err.Error(), "resp client is not connected")
		s.False(ok)
	})

	s.Run("SetNXWithTTL empty key", func() {
		s.SetupTest()
		s.connectRaw()

		ok, err := raw.SetNXWithTTL("", "v", time.Second)
		s.NoError(err)
		s.False(ok)
	})

	s.Run("SetNXWithTTL falls back when ttl is non-positive", func() {
		s.SetupTest()
		s.connectRaw()

		ok, err := raw.SetNXWithTTL("raw:setnx-fallback", "v1", 0)
		s.NoError(err)
		s.True(ok)

		ok, err = raw.SetNXWithTTL("raw:setnx-fallback", "v2", 0)
		s.NoError(err)
		s.False(ok)
	})

	s.Run("SetNXWithTTL success and expire", func() {
		s.SetupTest()
		s.connectRaw()

		ok, err := raw.SetNXWithTTL("raw:setnx-ttl", "ttl-value", 100*time.Millisecond)
		s.NoError(err)
		s.True(ok)

		val, err := raw.Get("raw:setnx-ttl")
		s.NoError(err)
		s.Equal("ttl-value", val)

		time.Sleep(200 * time.Millisecond)

		_, err = raw.Get("raw:setnx-ttl")
		s.Error(err)
		s.Contains(err.Error(), "redis: nil")
	})

	s.Run("SetNX not connected", func() {
		s.SetupTest()
		ok, err := raw.SetNX("raw:k", "v")
		s.Error(err)
		s.Contains(err.Error(), "resp client is not connected")
		s.False(ok)
	})

	s.Run("Delete not connected", func() {
		s.SetupTest()
		deleted, err := raw.Delete("raw:k")
		s.Error(err)
		s.Contains(err.Error(), "resp client is not connected")
		s.False(deleted)
	})

	s.Run("Keys not connected", func() {
		s.SetupTest()
		keysRes := raw.Keys("raw:k*")
		s.True(keysRes.IsError())
		s.Contains(keysRes.Error().Error(), "resp client is not connected")
	})

	s.Run("SetNX Delete Keys Success", func() {
		s.SetupTest()
		s.connectRaw()

		ok, err := raw.SetNX("raw:key_nx", "val1")
		s.NoError(err)
		s.True(ok)

		ok, err = raw.SetNX("raw:key_nx", "val2")
		s.NoError(err)
		s.False(ok)

		v, err := raw.Get("raw:key_nx")
		s.NoError(err)
		s.Equal("val1", v)

		_ = raw.Set("raw:key_another", "val")
		keysRes := raw.Keys("raw:key_*")
		s.False(keysRes.IsError())
		s.ElementsMatch([]string{"raw:key_nx", "raw:key_another"}, keysRes.MustGet())

		deleted, err := raw.Delete("raw:key_nx")
		s.NoError(err)
		s.True(deleted)

		keysRes = raw.Keys("raw:key_*")
		s.False(keysRes.IsError())
		s.ElementsMatch([]string{"raw:key_another"}, keysRes.MustGet())

		deleted, err = raw.Delete("raw:key_another")
		s.NoError(err)
		s.True(deleted)
	})
}

func (s *RawTestSuite) TestSearchIndexCommand() {
	s.SetupTest()
	s.connectRaw()

	data := []struct {
		key string
		val string
	}{
		{"user:1", `{"id": "1", "email": "ken@example.com", "age": 30, "status": "active"}`},
		{"user:2", `{"id": "2", "email": "john@example.com", "age": 20, "status": "pending"}`},
		{"user:3", `{"id": "3", "email": "admin@example.com", "age": 40, "status": "active"}`},
	}

	for _, d := range data {
		err := raw.Set(d.key, d.val)
		s.NoError(err)
	}

	tests := []struct {
		name      string
		index     string
		kr        x.KeyRange
		filter    x.Filter
		desc      bool
		expectErr bool
		expectLen int
	}{
		{"Missing index", "", x.KeysPattern("user:*"), x.Eq("email", "ken@example.com"), false, true, 0},
		{"Unknown index", "unknown", x.KeysPattern("user:*"), x.Eq("email", "ken@example.com"), false, true, 0},
		{"Eq string", x.Idx[rawTestUserDoc]("email", "*", "email").Name(), x.KeysPattern("user:*"), x.Eq("email", "ken@example.com"), false, false, 1},
		{"Eq false", x.Idx[rawTestUserDoc]("email", "*", "email").Name(), x.KeysPattern("user:*"), x.Eq("email", "nobody@example.com"), false, false, 0},
		{"Gt number", x.Idx[rawTestUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.Gt("age", 25), false, false, 2},
		{"Lt number", x.Idx[rawTestUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.Lt("age", 35), false, false, 2},
		{"And true", x.Idx[rawTestUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.And(x.Gt("age", 25), x.Eq("status", "active")), false, false, 2},
		{"And false", x.Idx[rawTestUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.And(x.Gt("age", 35), x.Eq("status", "pending")), false, false, 0},
		{"Or", x.Idx[rawTestUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.Or(x.Lt("age", 25), x.Eq("status", "active")), false, false, 3},
		{"Empty filter", x.Idx[rawTestUserDoc]("email", "*", "email").Name(), x.KeysPattern("user:*"), nil, false, false, 3},
		{"Key pattern narrows index scan", x.Idx[rawTestUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:2"), nil, false, false, 1},
		{"Descend test", x.Idx[rawTestUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.Gt("age", 10), true, false, 3},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			res := raw.SearchIndex(tt.index, tt.kr, tt.filter, tt.desc)

			if tt.expectErr {
				s.True(res.IsError())
			} else {
				s.False(res.IsError())
				results := res.MustGet()
				s.Len(results, tt.expectLen)
			}
		})
	}
}

func (s *RawTestSuite) TestSearchKeyCommand() {
	s.SetupTest()
	s.connectRaw()

	data := []struct {
		key string
		val string
	}{
		{"product:1", `{"id": "1", "name": "Apple", "price": 10, "stock": 100}`},
		{"product:2", `{"id": "2", "name": "Banana", "price": 5, "stock": 50}`},
		{"product:3", `{"id": "3", "name": "Orange", "price": 8, "stock": 200}`},
	}

	for _, d := range data {
		err := raw.Set(d.key, d.val)
		s.NoError(err)
	}

	tests := []struct {
		name      string
		kr        x.KeyRange
		filter    x.Filter
		desc      bool
		expectErr bool
		expectLen int
	}{
		{"Missing pattern", nil, x.Eq("name", "Apple"), false, true, 0},
		{"Match one", x.KeysPattern("product:*"), x.Eq("name", "Apple"), false, false, 1},
		{"Match none by filter", x.KeysPattern("product:*"), x.Eq("name", "Grape"), false, false, 0},
		{"Match none by pattern", x.KeysPattern("99*"), x.Eq("name", "Apple"), false, false, 0},
		{"Gt number", x.KeysPattern("product:*"), x.Gt("price", 6), false, false, 2},
		{"Lt number", x.KeysPattern("product:*"), x.Lt("stock", 150), false, false, 2},
		{"And true", x.KeysPattern("product:*"), x.And(x.Gt("price", 6), x.Lt("stock", 150)), false, false, 1},
		{"Empty filter", x.KeysPattern("product:*"), nil, false, false, 3},
		{"Descend test", x.KeysPattern("product:*"), x.Gt("price", 4), true, false, 3},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			res := raw.SearchKey(tt.kr, tt.filter, tt.desc)

			if tt.expectErr {
				s.True(res.IsError())
			} else {
				s.False(res.IsError())
				results := res.MustGet()
				s.Len(results, tt.expectLen)
			}
		})
	}
}

func (s *RawTestSuite) TestUpdateCommand() {
	s.SetupTest()
	s.connectRaw()

	data := []struct {
		key string
		val string
	}{
		{"user:1", `{"id":"1","status":"pending","age":17}`},
		{"user:2", `{"id":"2","status":"pending","age":22}`},
		{"user:3", `{"id":"3","status":"active","age":30}`},
	}

	for _, d := range data {
		err := raw.Set(d.key, d.val)
		s.NoError(err)
	}

	tests := []struct {
		name      string
		kr        x.KeyRange
		filter    x.Filter
		updates   []x.Mutation
		expectErr bool
		expectLen int
		check     func()
	}{
		{
			name:      "missing keyrange",
			kr:        nil,
			filter:    x.Eq("status", "pending"),
			updates:   []x.Mutation{x.Set("status", "active")},
			expectErr: true,
		},
		{
			name:      "missing update values",
			kr:        x.KeysPattern("user:*"),
			filter:    x.Eq("status", "pending"),
			expectErr: true,
		},
		{
			name:      "update filtered documents",
			kr:        x.KeysPattern("user:*"),
			filter:    x.Eq("status", "pending"),
			updates:   []x.Mutation{x.Set("status", "active"), x.Set("verified", true), x.Set("profile.age", 18)},
			expectLen: 2,
			check: func() {
				val, err := raw.Get("user:1")
				s.NoError(err)
				s.Equal("active", gjson.Get(val, "status").String())
				s.True(gjson.Get(val, "verified").Bool())
				s.Equal(float64(18), gjson.Get(val, "profile.age").Float())
			},
		},
		{
			name:      "update with nil filter",
			kr:        x.KeysPattern("user:*"),
			filter:    nil,
			updates:   []x.Mutation{x.Set("version", 2)},
			expectLen: 3,
			check: func() {
				val, err := raw.Get("user:3")
				s.NoError(err)
				s.Equal(float64(2), gjson.Get(val, "version").Float())
			},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			res := raw.Update(tt.kr, tt.filter, tt.updates...)
			if tt.expectErr {
				s.True(res.IsError())
				return
			}
			s.False(res.IsError())
			s.Len(res.MustGet(), tt.expectLen)
			if tt.check != nil {
				tt.check()
			}
		})
	}
}

// ─── DropSchema / DropIndex ───

func (s *RawTestSuite) TestDropSchemaAndIndex() {
	tests := []struct {
		name       string
		setup      func()
		action     func() error
		expectErr  bool
		errPhrase  string
	}{
		{
			name:      "DropSchema not connected",
			setup:     nil,
			action:    func() error { return raw.DropSchema("user") },
			expectErr: true,
			errPhrase: "resp client is not connected",
		},
		{
			name:      "DropSchema empty ns",
			setup:     func() { s.connectRaw() },
			action:    func() error { return raw.DropSchema("") },
			expectErr: true,
			errPhrase: "logical ns is empty",
		},
		{
			name:      "DropIndex not connected",
			setup:     nil,
			action:    func() error { return raw.DropIndex("user", "age") },
			expectErr: true,
			errPhrase: "resp client is not connected",
		},
		{
			name:      "DropIndex empty arg",
			setup:     func() { s.connectRaw() },
			action:    func() error { return raw.DropIndex("") },
			expectErr: true,
			errPhrase: "owner ns or full name is required",
		},
		{
			name:      "DropIndex too many args",
			setup:     func() { s.connectRaw() },
			action:    func() error { return raw.DropIndex("a", "b", "c") },
			expectErr: true,
			errPhrase: "at most 2 args",
		},
		{
			name:      "DropSchema nonexistent",
			setup:     func() { s.connectRaw() },
			action:    func() error { return raw.DropSchema("nonexistent") },
			expectErr: true,
			errPhrase: "not registered",
		},
		{
			name:      "DropIndex nonexistent",
			setup:     func() { s.connectRaw() },
			action:    func() error { return raw.DropIndex("nonexistent", "fakeidx") },
			expectErr: true,
			errPhrase: "not registered",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.SetupTest()
			if tt.setup != nil {
				tt.setup()
			}
			err := tt.action()
			if tt.expectErr {
				s.Error(err)
				if tt.errPhrase != "" {
					s.Contains(err.Error(), tt.errPhrase)
				}
			} else {
				s.NoError(err)
			}
		})
	}
}
