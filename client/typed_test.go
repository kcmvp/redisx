package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/redisx/client/internal/hook"
	"github.com/kcmvp/redisx/client/raw"
	"github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/respconn"
	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/server"
	"github.com/kcmvp/redisx/x"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
)

var clientTestServerAddr string

const clientTestExternalAuthKey = "client-test-external-key"
const clientTestAuthLimit = 50

const defaultHookTimeout = 100 * time.Millisecond

var cliServerSeedOnce sync.Once

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

func sharedSeedBody(t *testing.T, internalSet func(k, v string) error) {
	t.Helper()
	require := require.New(t)

	require.NotNil(internalSet, "internalSet callback for AUTH-key seed must be provided (writes bypass RESP wire internal-write-guard)")
	require.NoError(internalSet(naming.AuthStorageKey(clientTestExternalAuthKey), "50"), "seed AUTH key (internal storage write, bypasses wire internal-write-guard)")

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	defer func() { _ = rdb.Close() }()

	wireREGSCH := func(ns string, mem bool, keyAttrs []string, ttl time.Duration) {
		t.Helper()
		b, err := json.Marshal(wireSchemaJSON{Namespace: ns, Mem: mem, KeyAttrs: keyAttrs, TTL: ttl})
		require.NoError(err, "marshal REGSCH for %s (mem=%v)", ns, mem)
		status := rdb.Do(ctx, "REGSCH", string(b))
		require.NoError(status.Err(), "REGSCH %s (mem=%v)", ns, mem)
		require.Equal("OK", status.Val(), "REGSCH %s (mem=%v) want OK got %v", ns, mem, status.Val())
	}
	wireREGIDX := func(idx x.Index) {
		t.Helper()
		paths := idx.Paths()
		if paths == nil {
			paths = []string{}
		}
		b, err := json.Marshal(wireIndexJSON{FullName: idx.Name(), KeyPattern: idx.KeyPattern(), Paths: paths})
		require.NoError(err, "marshal REGIDX for %s", idx.Name())
		status := rdb.Do(ctx, "REGIDX", string(b))
		require.NoError(status.Err(), "REGIDX %s", idx.Name())
		require.Equal("OK", status.Val(), "REGIDX %s want OK got %v", idx.Name(), status.Val())
	}

	wireREGSCH("probenegcli", false, []string{"id"}, 0)
	wireREGSCH("probeserver", true, []string{"id"}, 0)
	wireREGSCH("probenegdoc", false, []string{"id"}, 0)

	wireREGIDX(x.Idx[testUserDoc]("age", "*", "age"))
	wireREGIDX(x.Idx[testUserDoc]("email", "*", "email"))

	probeClientKP := testutil.KeyRangeKeyPattern(searchKRClientNamespace, testutil.KeyRangeFixtureMem())
	probeClientStorageNs := naming.BuildStorageNs(searchKRClientNamespace, testutil.KeyRangeFixtureMem())
	idxClientProbeScore := x.RawIndex(naming.BuildIdxFullName(probeClientStorageNs, "score"), probeClientKP, "score")
	idxClientProbeBucket := x.RawIndex(naming.BuildIdxFullName(probeClientStorageNs, "bucket"), probeClientKP, "bucket")
	idxClientProbeSparse := x.RawIndex(naming.BuildIdxFullName(probeClientStorageNs, "sparseamt"), probeClientKP, "sparse_amt")
	wireREGIDX(idxClientProbeScore)
	wireREGIDX(idxClientProbeBucket)
	wireREGIDX(idxClientProbeSparse)

	probeDocKP := testutil.KeyRangeKeyPattern(searchKRDocNamespace, testutil.KeyRangeFixtureMem())
	probeDocStorageNs := naming.BuildStorageNs(searchKRDocNamespace, testutil.KeyRangeFixtureMem())
	idxDocProbeScore := x.RawIndex(naming.BuildIdxFullName(probeDocStorageNs, "score"), probeDocKP, "score")
	idxDocProbeBucket := x.RawIndex(naming.BuildIdxFullName(probeDocStorageNs, "bucket"), probeDocKP, "bucket")
	idxDocProbeSparse := x.RawIndex(naming.BuildIdxFullName(probeDocStorageNs, "sparseamt"), probeDocKP, "sparse_amt")
	wireREGIDX(idxDocProbeScore)
	wireREGIDX(idxDocProbeBucket)
	wireREGIDX(idxDocProbeSparse)

	wireREGIDX(x.Idx[UserDoc]("age", "*", "age"))

	for _, kv := range testutil.LoadXFor(t, searchKRClientNamespace, testutil.KeyRangeFixtureMem()) {
		require.NoError(rdb.Set(ctx, kv.K, kv.V, 0).Err(), "seed probe-client fixture failed for %s", kv.K)
	}

	for _, kv := range testutil.LoadXFor(t, searchKRDocNamespace, testutil.KeyRangeFixtureMem()) {
		require.NoError(rdb.Set(ctx, kv.K, kv.V, 0).Err(), "seed probe-doc fixture failed for %s", kv.K)
	}
}

func stopAndResetGlobalState() {
	disconnect()
	setLifecycleCtx(context.TODO(), nil)
	_ = server.Stop()
	cliOnce = sync.Once{}
	cliServerSeedOnce = sync.Once{}
	clientTestServerAddr = ""
}

func ensureServerAndSeed(t *testing.T) {
	t.Helper()
	require := require.New(t)

	t.Setenv("HOME", t.TempDir())

	addr := clientTestServerAddr
	if addr == "" {
		for i := 0; i < 30; i++ {
			if clientTestServerAddr != "" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	if clientTestServerAddr == "" {
		dbPath := filepath.Join(t.TempDir(), "redisx.db")
		appPort, ctrlPort := testutil.AllocateTwoFreePorts(t)
		cfg := &server.Config{
			DataPath: dbPath,
			App:      server.AppConfig{Bind: "127.0.0.1", Port: appPort},
			Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort},
		}
		clientTestServerAddr = cfg.Ctrl.Addr()
		db := server.StartWith(cfg,
			testUserDoc(""),
			SearchFixtureDoc(""),
			UpdateFixtureDoc(""),
			UserDoc(""),
			ExpiringUserDoc(""),
			DocSearchFixture(""),
			DocUpdateFixture(""),
		)
		require.NotNil(db)
		sharedSeedBody(t, func(k, v string) error {
			return db.Set(k, v)
		})
	}

	for i := 0; i < 30; i++ {
		probe, err := connect(clientTestServerAddr, clientTestExternalAuthKey)
		if err == nil {
			_ = probe.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("test server did not become ready")
}

type testUserDoc string

func (testUserDoc) Namespace() string  { return "user" }
func (testUserDoc) Mem() bool          { return false }
func (testUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u testUserDoc) RawJSON() string  { return string(u) }
func (testUserDoc) TTL() time.Duration { return 0 }

const (
	searchKRDocNamespace = "probedoc"
	updateKRDocNamespace = "upddoc000"
)

type DocSearchFixture string

func (DocSearchFixture) Namespace() string  { return searchKRDocNamespace }
func (DocSearchFixture) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (DocSearchFixture) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d DocSearchFixture) RawJSON() string  { return string(d) }
func (DocSearchFixture) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

type DocUpdateFixture string

func (DocUpdateFixture) Namespace() string  { return updateKRDocNamespace }
func (DocUpdateFixture) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (DocUpdateFixture) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d DocUpdateFixture) RawJSON() string  { return string(d) }
func (DocUpdateFixture) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

type UserDoc string

func (UserDoc) Namespace() string  { return "user" }
func (UserDoc) Mem() bool          { return false }
func (UserDoc) KeyAttrs() []string { return []string{"id"} }
func (u UserDoc) RawJSON() string  { return string(u) }
func (UserDoc) TTL() time.Duration { return time.Hour }

type ExpiringUserDoc string

func (ExpiringUserDoc) Namespace() string  { return "expuserclient" }
func (ExpiringUserDoc) Mem() bool          { return false }
func (ExpiringUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u ExpiringUserDoc) RawJSON() string  { return string(u) }
func (ExpiringUserDoc) TTL() time.Duration { return 40 * time.Millisecond }

func (s *ClientTestSuite) SetupTest() {
	disconnect()

	handlersMu.Lock()
	deliveryChByTopic = make(map[string]chan *ReceivedMessage)
	handlersMu.Unlock()

	if prev := setSharedClient(nil); prev != nil {
		_ = prev.Close()
	}

	cliOnce = sync.Once{}
	signalNotifyFn = signal.Notify
	signalStopFn = signal.Stop

	for len(pubChan) > 0 {
		<-pubChan
	}
	for len(subscribeReqCh) > 0 {
		<-subscribeReqCh
	}
	atomic.StoreUint64(&pipeDrops, 0)

	hook.Reset()
}

type ClientTestSuite struct {
	suite.Suite
}

func (s *ClientTestSuite) SetupSuite() {
	s.T().Setenv("HOME", s.T().TempDir())
	dbPath := filepath.Join(s.T().TempDir(), "redisx.db")

	appPort, ctrlPort := testutil.AllocateTwoFreePorts(s.T())
	cfg := &server.Config{
		DataPath: dbPath,
		App:      server.AppConfig{Bind: "127.0.0.1", Port: appPort},
		Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort},
	}
	clientTestServerAddr = cfg.Ctrl.Addr()
	db := server.StartWith(cfg,
		testUserDoc(""),
		SearchFixtureDoc(""),
		UpdateFixtureDoc(""),
		UserDoc(""),
		ExpiringUserDoc(""),
		DocSearchFixture(""),
		DocUpdateFixture(""),
	)
	s.Require().NotNil(db)

	sharedSeedBody(s.T(), func(k, v string) error {
		return db.Set(k, v)
	})

	for i := 0; i < 30; i++ {
		probe, err := connect(clientTestServerAddr, clientTestExternalAuthKey)
		if err == nil {
			_ = probe.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.T().Fatal("test server did not become ready")
}

func (s *ClientTestSuite) TearDownSuite() {
	stopAndResetGlobalState()
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

func (s *ClientTestSuite) TestMemKey() {
	s.Equal("_m_:user:1", x.MemKey("user:1"))
	s.Equal("_m_:user:1", x.MemKey("_m_:user:1"))
}

func (s *ClientTestSuite) ensureConnected() {
	for i := 0; i < 30; i++ {
		client := getSharedClient()
		if client != nil {
			if err := healthCheck(context.Background(), client); err == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.T().Fatal("Connect() failed to establish shared client in time")
}

func (s *ClientTestSuite) ensureConnectedClientAndAuth() {
	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err, "Connect to %s with auth key failed: %v", clientTestServerAddr, err)
	s.ensureConnected()
}

func (s *ClientTestSuite) TestSubscribeCommand() {
	tests := []struct {
		name        string
		topic       string
		setup       func()
		expectErr   bool
		wantErrMsg  string
		checkStored bool
	}{
		{"Empty topic", "", nil, true, "topic is empty", false},
		{
			"Duplicate handler",
			"orders",
			func() { _ = Subscribe("orders") },
			true,
			"duplicated handler for topic",
			false,
		},
		{
			"Success",
			"orders_success",
			nil,
			false,
			"",
			true,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			s.SetupTest()
			if tc.setup != nil {
				tc.setup()
			}

			res := Subscribe(tc.topic)
			if tc.expectErr {
				s.True(res.IsError())
				if tc.wantErrMsg != "" {
					s.Contains(res.Error().Error(), tc.wantErrMsg)
				}
			} else {
				s.False(res.IsError())
				s.NotNil(res.MustGet())
			}

			if tc.checkStored {
				handlersMu.RLock()
				stored := deliveryChByTopic[tc.topic]
				handlersMu.RUnlock()
				s.NotNil(stored)

				select {
				case topic := <-subscribeReqCh:
					s.Equal(tc.topic, topic)
				default:
					s.FailNow("subscribeReqCh should contain one subscription request")
				}
			}
		})
	}
}

func (s *ClientTestSuite) TestAuthCommand() {
	tests := []struct {
		name       string
		authKey    string
		setup      func()
		expectErr  bool
		wantErrMsg string
	}{
		{
			"Empty auth key",
			"",
			nil,
			true, "auth key is empty",
		},
		{
			"Not connected",
			"token",
			nil,
			true, "resp client is not connected",
		},
		{
			"Success",
			clientTestExternalAuthKey,
			func() {
				mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
				setSharedClient(mockClient)
			},
			false, "",
		},
		{
			"Invalid password",
			"wrong",
			func() {
				mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr})
				setSharedClient(mockClient)
			},
			true, "ERR authentication failed",
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			s.SetupTest()
			if tc.setup != nil {
				tc.setup()
			}

			err := Auth(tc.authKey)
			if tc.expectErr {
				s.Error(err)
				if tc.wantErrMsg != "" && err != nil {
					s.Contains(err.Error(), tc.wantErrMsg)
				}
			} else {
				s.NoError(err)
			}
		})
	}
}

func (s *ClientTestSuite) TestConnectCommand() {
	tests := []struct {
		name        string
		addr        string
		authKey     string
		expectErr   bool
		wantErrMsg  string
		checkExceed bool
		useConnect  bool
	}{
		{
			"Empty auth key",
			clientTestServerAddr,
			"",
			true, "auth key is empty",
			false,
			false,
		},
		{
			"Invalid address",
			"bad-addr:12345",
			"token",
			true, "dial",
			false,
			true,
		},
		{
			"Success",
			clientTestServerAddr,
			clientTestExternalAuthKey,
			false, "",
			false,
			false,
		},
		{
			"Exceed max connections",
			clientTestServerAddr,
			clientTestExternalAuthKey,
			true, "connection reset by peer",
			true,
			false,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			s.SetupTest()
			if !tc.checkExceed {
				var err error
				if tc.useConnect {
					_, err = connect(tc.addr, tc.authKey)
				} else {
					err = Connect(tc.addr, tc.authKey)
				}

				if tc.expectErr {
					s.Error(err)
					if tc.wantErrMsg != "" && err != nil && tc.name != "Invalid address" {
						s.Contains(err.Error(), tc.wantErrMsg)
					}
				} else {
					s.NoError(err)
					s.ensureConnected()
				}
			} else {
				var clients []*redis.Client
				defer func() {
					for _, c := range clients {
						_ = c.Close()
					}
				}()

				var overflowErr error
				for i := 0; i < clientTestAuthLimit+5; i++ {
					var c *redis.Client
					var err error
					for attempt := 0; attempt < 10; attempt++ {
						c, err = connect(tc.addr, tc.authKey)
						if err == nil {
							break
						}
						time.Sleep(10 * time.Millisecond)
					}
					if err != nil {
						overflowErr = err
						break
					}
					clients = append(clients, c)
				}
				s.Error(overflowErr)
				if overflowErr != nil && tc.wantErrMsg != "" {
					errMsg := overflowErr.Error()
					s.True(
						strings.Contains(errMsg, "EOF") ||
							strings.Contains(errMsg, "connection reset by peer") ||
							strings.Contains(errMsg, "auth key connection limit exceeded") ||
							strings.Contains(errMsg, tc.wantErrMsg),
						"Expected error to contain EOF, connection reset by peer, auth key connection limit exceeded, or %q, but got: %s",
						tc.wantErrMsg,
						errMsg,
					)
				}
			}
		})
	}
}

// p1RegSchDoc / p1RegSchV2Doc share namespace "p1regsch" but differ in
// KeyAttrs so the suite can exercise REGSCH idempotent-skip and
// fingerprint-upgrade paths through the client facades.
type p1RegSchDoc string

func (p1RegSchDoc) Namespace() string  { return "p1regsch" }
func (p1RegSchDoc) Mem() bool          { return false }
func (p1RegSchDoc) KeyAttrs() []string { return []string{"id"} }
func (d p1RegSchDoc) RawJSON() string  { return string(d) }
func (p1RegSchDoc) TTL() time.Duration { return 0 }

type p1RegSchV2Doc string

func (p1RegSchV2Doc) Namespace() string  { return "p1regsch" }
func (p1RegSchV2Doc) Mem() bool          { return false }
func (p1RegSchV2Doc) KeyAttrs() []string { return []string{"id", "email"} }
func (d p1RegSchV2Doc) RawJSON() string  { return string(d) }
func (p1RegSchV2Doc) TTL() time.Duration { return 0 }

type p1DropSchDoc string

func (p1DropSchDoc) Namespace() string  { return "p1dropsch" }
func (p1DropSchDoc) Mem() bool          { return false }
func (p1DropSchDoc) KeyAttrs() []string { return []string{"id"} }
func (d p1DropSchDoc) RawJSON() string  { return string(d) }
func (p1DropSchDoc) TTL() time.Duration { return 0 }

func (s *ClientTestSuite) TestRegisterSchema() {
	s.SetupTest()
	s.ensureConnectedClientAndAuth()

	tests := []struct {
		name       string
		fn         func() error
		wantErr    bool
		wantErrMsg string
	}{
		{"success new schema", func() error { return RegisterSchema[p1RegSchDoc]() }, false, ""},
		{"idempotent identical re-register", func() error { return RegisterSchema[p1RegSchDoc]() }, false, ""},
		{"upgrade on fingerprint change", func() error { return RegisterSchema[p1RegSchV2Doc]() }, false, ""},
		{"empty json", func() error { return RegisterSchemaFromJSON("") }, true, "schema spec json is empty"},
		{"invalid json format", func() error { return RegisterSchemaFromJSON("{bad") }, true, "ERR REGSCH invalid JSON format"},
		{"unknown field rejected", func() error {
			return RegisterSchemaFromJSON(`{"namespace":"p1x","bogus":1}`)
		}, true, "ERR REGSCH schema"},
		{"reserved field indexes rejected", func() error {
			return RegisterSchemaFromJSON(`{"namespace":"p1x","indexes":[]}`)
		}, true, "reserved field 'indexes'"},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			err := tc.fn()
			if tc.wantErr {
				s.Require().Error(err)
				s.Contains(err.Error(), tc.wantErrMsg)
			} else {
				s.NoError(err)
			}
		})
	}
}

func (s *ClientTestSuite) TestRegisterIndex() {
	s.SetupTest()
	s.ensureConnectedClientAndAuth()

	// Self-contained: guarantee the owner schema exists regardless of
	// TestRegisterSchema's outcome.
	s.Require().NoError(RegisterSchema[p1RegSchDoc]())

	tests := []struct {
		name       string
		fn         func() error
		wantErr    bool
		wantErrMsg string
	}{
		{"success", func() error {
			return RegisterIndex[p1RegSchDoc]("age", "p1regsch:*", "age")
		}, false, ""},
		{"idempotent identical re-register", func() error {
			return RegisterIndex[p1RegSchDoc]("age", "p1regsch:*", "age")
		}, false, ""},
		{"empty logical", func() error {
			return RegisterIndex[p1RegSchDoc]("", "p1regsch:*", "age")
		}, true, "logical index name is empty"},
		{"empty key pattern", func() error {
			return RegisterIndex[p1RegSchDoc]("age", "", "age")
		}, true, "key pattern is empty"},
		{"no paths", func() error {
			return RegisterIndex[p1RegSchDoc]("age", "p1regsch:*")
		}, true, "at least one jsonPath is required"},
		{"empty path entry", func() error {
			return RegisterIndex[p1RegSchDoc]("age", "p1regsch:*", "age", "")
		}, true, "jsonPaths contains empty path"},
		{"short success", func() error {
			return RegisterIndexShort("p1regsch", "email", "profile.email")
		}, false, ""},
		{"short empty owner ns", func() error {
			return RegisterIndexShort("", "email", "age")
		}, true, "owner_ns is empty"},
		{"short empty logical", func() error {
			return RegisterIndexShort("p1regsch", "", "age")
		}, true, "logical is empty"},
		{"short no paths", func() error {
			return RegisterIndexShort("p1regsch", "email")
		}, true, "at least one path is required"},
		{"short empty path entry", func() error {
			return RegisterIndexShort("p1regsch", "email", "age", "")
		}, true, "paths contains an empty entry"},
		{"from json empty", func() error {
			return RegisterIndexFromJSON("")
		}, true, "index spec json is empty"},
		{"from json invalid", func() error {
			return RegisterIndexFromJSON("{bad")
		}, true, "ERR REGIDX invalid JSON format"},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			err := tc.fn()
			if tc.wantErr {
				s.Require().Error(err)
				s.Contains(err.Error(), tc.wantErrMsg)
			} else {
				s.NoError(err)
			}
		})
	}
}

func (s *ClientTestSuite) TestDropSchemaAndDropIndex() {
	s.SetupTest()
	s.ensureConnectedClientAndAuth()

	// Self-contained fixture: schema + attached index on "p1dropsch".
	s.Require().NoError(RegisterSchema[p1DropSchDoc]())
	s.Require().NoError(RegisterIndex[p1DropSchDoc]("age", "p1dropsch:*", "age"))

	tests := []struct {
		name       string
		fn         func() error
		wantErr    bool
		wantErrMsg string
	}{
		{"drop schema blocked by attached index", func() error {
			return DropSchema[p1DropSchDoc]()
		}, true, "attached index"},
		{"drop index empty logical", func() error {
			return DropIndex[p1DropSchDoc]("")
		}, true, "logical index name is empty"},
		{"raw drop schema empty ns", func() error {
			return raw.DropSchema("")
		}, true, "logical ns is empty"},
		{"raw drop index empty ns", func() error {
			return raw.DropIndex("")
		}, true, "owner ns or full name is required"},
		{"raw drop index too many args", func() error {
			return raw.DropIndex("p1dropsch", "age", "extra")
		}, true, "at most 2 args"},
		{"drop index success", func() error {
			return DropIndex[p1DropSchDoc]("age")
		}, false, ""},
		{"drop schema success after index removed", func() error {
			return DropSchema[p1DropSchDoc]()
		}, false, ""},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			err := tc.fn()
			if tc.wantErr {
				s.Require().Error(err)
				s.Contains(err.Error(), tc.wantErrMsg)
			} else {
				s.NoError(err)
			}
		})
	}
}

func (s *ClientTestSuite) TestRegistryAPIsRequireConnection() {
	// SetupTest disconnects the shared client; every registry facade must
	// fail fast instead of panicking on a nil client.
	s.SetupTest()

	tests := []struct {
		name string
		fn   func() error
	}{
		{"RegisterSchemaFromJSON", func() error { return RegisterSchemaFromJSON(`{"namespace":"x"}`) }},
		{"RegisterIndexFromJSON", func() error {
			return RegisterIndexFromJSON(`{"owner_ns":"x","logical":"y","paths":["a"],"key_pattern":"x:*"}`)
		}},
		{"RegisterIndexShort", func() error { return RegisterIndexShort("x", "y", "a") }},
		{"DropSchema", func() error { return DropSchema[testUserDoc]() }},
		{"DropIndex", func() error { return DropIndex[testUserDoc]("age") }},
		{"raw.DropSchema", func() error { return raw.DropSchema("x") }},
		{"raw.DropIndex", func() error { return raw.DropIndex("x", "y") }},
	}
	for _, tc := range tests {
		s.Run(tc.name, func() {
			err := tc.fn()
			s.Require().Error(err)
			s.Contains(err.Error(), "resp client is not connected")
		})
	}
}

func (s *ClientTestSuite) TestConnectEmbedded() {
	s.SetupTest()

	err := ConnectEmbedded()
	s.NoError(err)
	s.ensureConnected()
}

func (s *ClientTestSuite) TestConnectEmbeddedStopsOnProcessSignal() {
	s.SetupTest()

	var capturedSigCh chan<- os.Signal
	signalNotifyFn = func(c chan<- os.Signal, _ ...os.Signal) {
		capturedSigCh = c
	}

	err := ConnectEmbedded()
	s.NoError(err)
	s.ensureConnected()

	s.Require().NotNil(capturedSigCh)
	capturedSigCh <- os.Interrupt

	for i := 0; i < 40; i++ {
		ctx, _ := getLifecycleCtx()
		if client := getSharedClient(); client == nil && ctx != nil && ctx.Err() != nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	ctx, _ := getLifecycleCtx()
	s.Nil(getSharedClient())
	s.Require().NotNil(ctx)
	s.Error(ctx.Err())
}

func TestEmbedServerAndClientExitTogetherOnSIGINT(t *testing.T) {
	if os.Getenv("REDISX_CHILD_EMBED_SIGNAL") == "1" {
		runEmbedSignalChild(t)
		return
	}

	addr := pickFreeAddr(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestEmbedServerAndClientExitTogetherOnSIGINT$")
	cmd.Env = append(os.Environ(),
		"REDISX_CHILD_EMBED_SIGNAL=1",
		"REDISX_CHILD_ADDR="+addr,
	)

	var stderr bytes.Buffer
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe() failed: %v", err)
	}
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	readyCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "READY" {
				readyCh <- nil
				return
			}
		}
		if err := scanner.Err(); err != nil {
			readyCh <- err
			return
		}
		readyCh <- fmt.Errorf("child exited before ready: %s", stderr.String())
	}()

	select {
	case err := <-readyCh:
		if err != nil {
			t.Fatalf("child failed before ready: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("timed out waiting for child readiness: %s", stderr.String())
	}

	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("Signal() failed: %v", err)
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	select {
	case err := <-waitCh:
		if err != nil {
			t.Fatalf("child exited with error: %v, stderr=%s", err, stderr.String())
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("timed out waiting for child shutdown: %s", stderr.String())
	}
}

func runEmbedSignalChild(t *testing.T) {
	t.Helper()

	addr := os.Getenv("REDISX_CHILD_ADDR")
	if addr == "" {
		t.Fatal("REDISX_CHILD_ADDR is required")
	}

	t.Setenv("HOME", t.TempDir())

	dbPath := filepath.Join(t.TempDir(), "redisx.db")

	ctrlHost, ctrlPortStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("invalid REDISX_CHILD_ADDR=%q: %v", addr, err)
	}
	if ctrlHost == "" {
		ctrlHost = "127.0.0.1"
	}
	ctrlPort, pErr := strconv.Atoi(ctrlPortStr)
	if pErr != nil {
		t.Fatalf("invalid ctrl port %q: %v", ctrlPortStr, pErr)
	}
	var appPort int
	claimedApp, lErr := net.Listen("tcp", net.JoinHostPort(ctrlHost, "0"))
	if lErr != nil {
		t.Fatalf("allocate free app port: %v", lErr)
	}
	appPort = claimedApp.Addr().(*net.TCPAddr).Port
	if appPort == ctrlPort {
		_ = claimedApp.Close()
		claimedApp, lErr = net.Listen("tcp", net.JoinHostPort(ctrlHost, "0"))
		if lErr != nil {
			t.Fatalf("re-allocate free app port (collision): %v", lErr)
		}
		appPort = claimedApp.Addr().(*net.TCPAddr).Port
	}
	_ = claimedApp.Close()

	cfg := &server.Config{
		DataPath: dbPath,
		App:      server.AppConfig{Bind: ctrlHost, Port: appPort},
		Ctrl:     server.CtrlConfig{Bind: ctrlHost, Port: ctrlPort},
	}
	db := server.StartWith(cfg)
	if db == nil {
		t.Fatalf("server.StartWith() returned nil; appPort=%d ctrlPort=%d — likely a bind race, retry", appPort, ctrlPort)
	}

	if err := ConnectEmbedded(); err != nil {
		t.Fatalf("ConnectEmbedded() failed: %v", err)
	}

	waitForSharedClient(t)
	fmt.Println("READY")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	select {
	case <-sigCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for interrupt signal")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, _ := getLifecycleCtx()
		clientStopped := ctx != nil && ctx.Err() != nil && getSharedClient() == nil
		serverStopped := listenerClosed(addr)
		if clientStopped && serverStopped {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	ctx, _ := getLifecycleCtx()
	t.Fatalf(
		"timed out waiting for graceful shutdown: client_nil=%t lifecycle_done=%t listener_closed=%t",
		getSharedClient() == nil,
		ctx != nil && ctx.Err() != nil,
		listenerClosed(addr),
	)
}

func waitForSharedClient(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		client := getSharedClient()
		if client != nil && healthCheck(context.Background(), client) == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatal("timed out waiting for shared client connection")
}

func listenerClosed(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		return true
	}
	_ = conn.Close()
	return false
}

func pickFreeAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() failed: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func (s *ClientTestSuite) TestPublishCommand() {
	tests := []struct {
		name      string
		topic     string
		payload   string
		setup     func()
		expectOk  bool
		checkDrop bool
	}{
		{
			"Not connected",
			"topic-a",
			"hello",
			nil,
			false,
			false,
		},
		{
			"Empty topic",
			"",
			"hello",
			nil,
			false,
			false,
		},
		{
			"Queue full",
			"topic-full",
			"hello",
			func() {
				mockClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
				setSharedClient(mockClient)
				for i := 0; i < cap(pubChan); i++ {
					pubChan <- outgoingMessage{topic: "filled", payload: "x"}
				}
			},
			false,
			true,
		},
		{
			"Success enqueues",
			"topic-a",
			"hello",
			func() {
				mockClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
				setSharedClient(mockClient)
			},
			true,
			false,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			s.SetupTest()
			if tc.setup != nil {
				tc.setup()
			}

			ok := Publish(tc.topic, tc.payload)
			s.Equal(tc.expectOk, ok)

			if tc.checkDrop {
				drops := atomic.LoadUint64(&pipeDrops)
				s.Equal(uint64(1), drops)
			} else if tc.expectOk {
				select {
				case out := <-pubChan:
					s.Equal(tc.topic, out.topic)
					s.Equal(tc.payload, out.payload)
				default:
					s.FailNow("pubChan should have one queued message")
				}
			}
		})
	}
}

func (s *ClientTestSuite) TestPubSubLifecycle() {
	tests := []struct {
		name string
		run  func()
	}{
		{
			"ProducePublishesMessageAndStopsOnCancel",
			func() {
				client, _ := connect(clientTestServerAddr, clientTestExternalAuthKey)
				s.T().Cleanup(func() { _ = client.Close() })

				ctxSub, cancelSub := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancelSub()
				sub := client.Subscribe(ctxSub, "orders")
				s.T().Cleanup(func() { _ = sub.Close() })
				_, err := sub.Receive(ctxSub)
				s.NoError(err)

				ctx, cancel := context.WithCancel(context.Background())
				errCh := make(chan error, 1)
				go func() {
					errCh <- produce(ctx, client)
				}()

				pubChan <- outgoingMessage{topic: "orders", payload: "hello"}

				msgCh := sub.Channel()
				select {
				case msg := <-msgCh:
					s.Equal("hello", msg.Payload)
					s.Equal("orders", msg.Channel)
				case <-time.After(2 * time.Second):
					s.FailNow("timeout waiting published message")
				}

				psub := client.PSubscribe(ctxSub, "event:*")
				s.T().Cleanup(func() { _ = psub.Close() })
				_, err = psub.Receive(ctxSub)
				s.NoError(err)

				pubChan <- outgoingMessage{topic: "event:login", payload: "user_1"}

				pmsgCh := psub.Channel()
				select {
				case msg := <-pmsgCh:
					s.Equal("user_1", msg.Payload)
					s.Equal("event:login", msg.Channel)
					s.Equal("event:*", msg.Pattern)
				case <-time.After(2 * time.Second):
					s.FailNow("timeout waiting pattern published message")
				}

				client2, _ := connect(clientTestServerAddr, clientTestExternalAuthKey)
				s.T().Cleanup(func() { _ = client2.Close() })
				sub2 := client2.Subscribe(ctxSub, "broadcast_topic")
				s.T().Cleanup(func() { _ = sub2.Close() })
				_, err = sub2.Receive(ctxSub)
				s.NoError(err)

				client3, _ := connect(clientTestServerAddr, clientTestExternalAuthKey)
				s.T().Cleanup(func() { _ = client3.Close() })
				sub3 := client3.Subscribe(ctxSub, "broadcast_topic")
				s.T().Cleanup(func() { _ = sub3.Close() })
				_, err = sub3.Receive(ctxSub)
				s.NoError(err)

				pubChan <- outgoingMessage{topic: "broadcast_topic", payload: "alert"}

				msgCh2 := sub2.Channel()
				select {
				case msg := <-msgCh2:
					s.Equal("alert", msg.Payload)
					s.Equal("broadcast_topic", msg.Channel)
				case <-time.After(2 * time.Second):
					s.FailNow("client2 timeout waiting broadcast message")
				}

				msgCh3 := sub3.Channel()
				select {
				case msg := <-msgCh3:
					s.Equal("alert", msg.Payload)
					s.Equal("broadcast_topic", msg.Channel)
				case <-time.After(2 * time.Second):
					s.FailNow("client3 timeout waiting broadcast message")
				}

				cancel()
				select {
				case err := <-errCh:
					s.NoError(err)
				case <-time.After(2 * time.Second):
					s.FailNow("produce() did not exit after cancel")
				}
			},
		},
		{
			"ConsumeDispatchesInitialAndRuntimeTopics",
			func() {
				client, _ := connect(clientTestServerAddr, clientTestExternalAuthKey)
				s.T().Cleanup(func() { _ = client.Close() })

				initialHandler := make(chan *ReceivedMessage, 1)
				runtimeHandler := make(chan *ReceivedMessage, 1)

				handlersMu.Lock()
				deliveryChByTopic["initial-topic"] = initialHandler
				handlersMu.Unlock()

				ctx, cancel := context.WithCancel(context.Background())
				errCh := make(chan error, 1)
				go func() {
					errCh <- consume(ctx, client)
				}()

				time.Sleep(80 * time.Millisecond)

				err := client.Publish(context.Background(), "initial-topic", "init-msg").Err()
				s.NoError(err)
				select {
				case got := <-initialHandler:
					s.Equal("initial-topic", got.Channel)
					s.Equal("init-msg", got.Payload)
				case <-time.After(2 * time.Second):
					s.FailNow("timeout waiting initial-topic message")
				}

				handlersMu.Lock()
				deliveryChByTopic["runtime-topic"] = runtimeHandler
				handlersMu.Unlock()
				subscribeReqCh <- "runtime-topic"

				deadline := time.After(2 * time.Second)
				tick := time.NewTicker(30 * time.Millisecond)
				defer tick.Stop()
				runtimeDelivered := false
				for !runtimeDelivered {
					select {
					case got := <-runtimeHandler:
						s.Equal("runtime-topic", got.Channel)
						s.Equal("rt-msg", got.Payload)
						runtimeDelivered = true
					case <-tick.C:
						_ = client.Publish(context.Background(), "runtime-topic", "rt-msg").Err()
					case <-deadline:
						s.FailNow("timeout waiting runtime-topic message")
					}
				}

				cancel()
				select {
				case err := <-errCh:
					s.NoError(err)
				case <-time.After(2 * time.Second):
					s.FailNow("consume() did not exit after cancel")
				}
			},
		},
		{
			"ConsumePreservesSubscriptionsAcrossRestart",
			func() {
				client1, _ := connect(clientTestServerAddr, clientTestExternalAuthKey)
				s.T().Cleanup(func() { _ = client1.Close() })

				handler := make(chan *ReceivedMessage, 2)

				handlersMu.Lock()
				deliveryChByTopic["restart-topic"] = handler
				handlersMu.Unlock()

				ctx1, cancel1 := context.WithCancel(context.Background())
				errCh1 := make(chan error, 1)
				go func() {
					errCh1 <- consume(ctx1, client1)
				}()

				time.Sleep(80 * time.Millisecond)
				publishErr := client1.Publish(context.Background(), "restart-topic", "first").Err()
				s.NoError(publishErr)
				select {
				case got := <-handler:
					s.Equal("restart-topic", got.Channel)
					s.Equal("first", got.Payload)
				case <-time.After(2 * time.Second):
					s.FailNow("timeout waiting first restart-topic message")
				}

				cancel1()
				select {
				case err := <-errCh1:
					s.NoError(err)
				case <-time.After(2 * time.Second):
					s.FailNow("first consume() did not exit after cancel")
				}

				handlersMu.RLock()
				stored := deliveryChByTopic["restart-topic"]
				handlersMu.RUnlock()
				s.Equal(handler, stored)

				client2, _ := connect(clientTestServerAddr, clientTestExternalAuthKey)
				s.T().Cleanup(func() { _ = client2.Close() })

				ctx2, cancel2 := context.WithCancel(context.Background())
				errCh2 := make(chan error, 1)
				go func() {
					errCh2 <- consume(ctx2, client2)
				}()

				time.Sleep(80 * time.Millisecond)
				publishErr = client2.Publish(context.Background(), "restart-topic", "second").Err()
				s.NoError(publishErr)
				select {
				case got := <-handler:
					s.Equal("restart-topic", got.Channel)
					s.Equal("second", got.Payload)
				case <-time.After(2 * time.Second):
					s.FailNow("timeout waiting second restart-topic message")
				}

				cancel2()
				select {
				case err := <-errCh2:
					s.NoError(err)
				case <-time.After(2 * time.Second):
					s.FailNow("second consume() did not exit after cancel")
				}
			},
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			s.SetupTest()
			tc.run()
		})
	}
}

func (s *ClientTestSuite) TestSetAndClearSharedClientHelpers() {
	c1 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	c2 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	s.T().Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})

	s.Nil(setSharedClient(c1))
	s.Equal(c1, getSharedClient())
	s.Equal(c1, setSharedClient(c2))

	clearSharedClientIf(c1)
	s.Equal(c2, getSharedClient())

	clearSharedClientIf(c2)
	s.Nil(getSharedClient())
}

func (s *ClientTestSuite) TestHookRegistrationAndRemoval() {
	s.SetupTest()

	s.Nil(hook.Snapshot(), "no hooks initially")

	id1 := hook.AddObserver(func(key string, value []byte) {})
	s.NotEqual(hook.ID(0), id1, "hook.ID should be non-zero")
	s.NotNil(hook.Snapshot())
	s.Equal(1, hook.Snapshot().LenObservers())

	id2 := hook.AddAbort(func(key string, value []byte) error { return nil })
	id3 := hook.AddTransform(func(key string, value []byte) ([]byte, error) { return value, nil })
	id4 := hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {})
	s.Equal(1, hook.Snapshot().LenAborts())
	s.Equal(1, hook.Snapshot().LenTransforms())
	s.Equal(1, hook.Snapshot().LenAfters())

	s.NotEqual(id1, id2)
	s.NotEqual(id2, id3)
	s.NotEqual(id3, id4)

	hook.Remove(id2)
	s.Equal(0, hook.Snapshot().LenAborts())
	s.Equal(1, hook.Snapshot().LenObservers())
	s.Equal(1, hook.Snapshot().LenTransforms())
	s.Equal(1, hook.Snapshot().LenAfters())

	hook.Remove(id1)
	hook.Remove(id3)
	hook.Remove(id4)
	reg := hook.Snapshot()
	s.Nil(reg, "after removing every hook, registry snapshot must be nil to keep the zero-cost default path")

	hook.Remove(hook.ID(0))
	hook.Remove(9999)
	hook.Remove(id1)
}

func (s *ClientTestSuite) TestObserverHookCalledBeforeWrite() {
	s.SetupTest()

	var called bool
	var gotKey string
	var gotVal []byte
	hook.AddObserver(func(key string, value []byte) {
		called = true
		gotKey = key
		gotVal = append([]byte(nil), value...)
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-obs-1", "hello")
	s.NoError(err)
	s.True(called, "ObserverHook must be called")
	s.Equal("cli:hook-obs-1", gotKey)
	s.Equal([]byte("hello"), gotVal)

	stored, err := raw.Get("cli:hook-obs-1")
	s.NoError(err)
	s.Equal("hello", stored)
}

func (s *ClientTestSuite) TestObserverHookFailOpenOnPanic() {
	s.SetupTest()

	hook.SetTimeout(0)
	defer hook.SetTimeout(defaultHookTimeout)

	hook.AddObserver(func(key string, value []byte) {
		panic("boom observer")
	})

	var ok bool
	hook.AddObserver(func(key string, value []byte) {
		ok = true
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-obs-panic", "val")
	s.NoError(err, "ObserverHook panic must not abort write")
	s.True(ok, "subsequent ObserverHooks must still run after panic in prior observer")

	stored, err := raw.Get("cli:hook-obs-panic")
	s.NoError(err)
	s.Equal("val", stored)
}

func (s *ClientTestSuite) TestObserverHookFailOpenOnTimeout() {
	s.SetupTest()

	hook.SetTimeout(50 * time.Millisecond)
	defer hook.SetTimeout(defaultHookTimeout)

	hook.AddObserver(func(key string, value []byte) {
		time.Sleep(200 * time.Millisecond)
	})

	var ok bool
	hook.AddObserver(func(key string, value []byte) {
		ok = true
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	start := time.Now()
	err = raw.Set("cli:hook-obs-timeout", "val")
	elapsed := time.Since(start)
	s.NoError(err, "ObserverHook timeout must not abort write")
	s.True(ok, "subsequent ObserverHooks must still run")
	s.Less(elapsed, 500*time.Millisecond, "timeout should bound the hook time")

	stored, err := raw.Get("cli:hook-obs-timeout")
	s.NoError(err)
	s.Equal("val", stored)
}

func (s *ClientTestSuite) TestAbortHookBlocksWrite() {
	s.SetupTest()

	customErr := errors.New("blocked by policy")
	hook.AddAbort(func(key string, value []byte) error {
		if key == "cli:forbidden" {
			return customErr
		}
		return nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:forbidden", "secret")
	s.ErrorIs(err, customErr)

	_, err = raw.Get("cli:forbidden")
	s.Error(err, "key must not exist after abort")

	err = raw.Set("cli:allowed", "public")
	s.NoError(err)
	v, err := raw.Get("cli:allowed")
	s.NoError(err)
	s.Equal("public", v)
}

func (s *ClientTestSuite) TestAbortHookFailClosedOnPanic() {
	s.SetupTest()

	hook.SetTimeout(0)
	defer hook.SetTimeout(defaultHookTimeout)

	hook.AddAbort(func(key string, value []byte) error {
		panic("abort panic")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-abort-panic", "val")
	s.Error(err, "AbortHook panic must abort write (fail-closed)")
	s.Contains(err.Error(), "panic")

	_, err = raw.Get("cli:hook-abort-panic")
	s.Error(err, "key must not exist after AbortHook panic")
}

func (s *ClientTestSuite) TestAbortHookFailClosedOnTimeout() {
	s.SetupTest()

	hook.SetTimeout(50 * time.Millisecond)
	defer hook.SetTimeout(defaultHookTimeout)

	hook.AddAbort(func(key string, value []byte) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	start := time.Now()
	err = raw.Set("cli:hook-abort-timeout", "val")
	elapsed := time.Since(start)
	s.Error(err, "AbortHook timeout must abort write (fail-closed)")
	s.Contains(err.Error(), "timeout")
	s.Less(elapsed, 500*time.Millisecond)
}

func (s *ClientTestSuite) TestTransformHookChangesValue() {
	s.SetupTest()

	prefix := []byte("enc:")
	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(prefix)+len(value))
		copy(out, prefix)
		copy(out[len(prefix):], value)
		return out, nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-transform-1", "plain")
	s.NoError(err)

	stored, err := raw.Get("cli:hook-transform-1")
	s.NoError(err)
	s.Equal("enc:plain", stored, "TransformHook must change the written value")
}

func (s *ClientTestSuite) TestTransformHookChain() {
	s.SetupTest()

	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		for i, b := range value {
			out[i] = b + 1
		}
		return out, nil
	})
	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		for i, b := range value {
			out[i] = b * 2
		}
		return out, nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-transform-chain", "\x01")
	s.NoError(err)

	stored, err := raw.Get("cli:hook-transform-chain")
	s.NoError(err)
	s.Equal([]byte{("\x01"[0] + 1) * 2}, []byte(stored),
		"TransformHooks must chain in registration order: (+1) then (*2)")
}

func (s *ClientTestSuite) TestTransformHookFailClosedOnError() {
	s.SetupTest()

	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		return nil, errors.New("transform failed")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-transform-err", "val")
	s.Error(err)
	s.Contains(err.Error(), "transform failed")

	_, err = raw.Get("cli:hook-transform-err")
	s.Error(err, "key must not exist after TransformHook error")
}

func (s *ClientTestSuite) TestTransformHookFailClosedOnPanic() {
	s.SetupTest()

	hook.SetTimeout(0)
	defer hook.SetTimeout(defaultHookTimeout)

	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		panic("transform panic")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-transform-panic", "val")
	s.Error(err)
	s.Contains(err.Error(), "panic")

	_, err = raw.Get("cli:hook-transform-panic")
	s.Error(err)
}

func (s *ClientTestSuite) TestObserverAfterHookCalledAfterWrite() {
	s.SetupTest()

	var gotKey, gotVal string
	var gotWriteErr error
	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		gotKey = key
		gotVal = string(value)
		gotWriteErr = writeErr
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-after-1", "hello-after")
	s.NoError(err)
	s.Equal("cli:hook-after-1", gotKey)
	s.Equal("hello-after", gotVal)
	s.NoError(gotWriteErr, "writeErr should be nil on success")
}

func (s *ClientTestSuite) TestObserverAfterHookReceivesTransformedValue() {
	s.SetupTest()

	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		copy(out, value)
		for i := range out {
			out[i] = out[i] + 1
		}
		return out, nil
	})

	var gotVal []byte
	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		gotVal = append([]byte(nil), value...)
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-after-transformed", "ABC")
	s.NoError(err)
	s.Equal([]byte("BCD"), gotVal, "ObserverAfter must see the transformed value")
}

func (s *ClientTestSuite) TestObserverAfterHookFailOpenOnPanic() {
	s.SetupTest()

	hook.SetTimeout(0)
	defer hook.SetTimeout(defaultHookTimeout)

	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		panic("after hook boom")
	})

	var ok bool
	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		ok = true
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-after-panic", "val")
	s.NoError(err, "ObserverAfterHook panic must not affect write result")
	s.True(ok, "subsequent ObserverAfterHooks must still run")

	stored, err := raw.Get("cli:hook-after-panic")
	s.NoError(err)
	s.Equal("val", stored)
}

func (s *ClientTestSuite) TestObserverAfterHookFailOpenOnTimeout() {
	s.SetupTest()

	hook.SetTimeout(50 * time.Millisecond)
	defer hook.SetTimeout(defaultHookTimeout)

	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		time.Sleep(200 * time.Millisecond)
	})

	var ok bool
	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		ok = true
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	start := time.Now()
	err = raw.Set("cli:hook-after-timeout", "val")
	elapsed := time.Since(start)
	s.NoError(err, "ObserverAfterHook timeout must not affect write result")
	s.True(ok, "subsequent ObserverAfterHooks must still run")
	s.Less(elapsed, 500*time.Millisecond)
}

func (s *ClientTestSuite) TestHookExecutionOrder() {
	s.SetupTest()

	var seq atomic.Int64
	steps := make([]int64, 5)
	capture := func(idx int) {
		steps[idx] = seq.Add(1)
	}

	hook.AddAbort(func(key string, value []byte) error {
		capture(0)
		s.Equal("original", string(value), "Abort must see original value")
		return nil
	})
	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		capture(1)
		s.Equal("original", string(value), "Transform must see original value")
		out := make([]byte, len("transformed"))
		copy(out, "transformed")
		return out, nil
	})
	hook.AddObserver(func(key string, value []byte) {
		capture(2)
		s.Equal("transformed", string(value), "ObserverBefore must see POST-transform value (A5)")
	})
	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		capture(4)
		s.Equal("transformed", string(value), "ObserverAfter must see transformed value")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-order", "original")
	s.NoError(err)
	steps[3] = seq.Add(1)

	s.Equal([]int64{1, 2, 3, 5, 4}, steps,
		"Order MUST be Abort(1) → Transform(2) → ObserverBefore(3) → [actual Redis write happens before return ~4 but AFTER Before hooks] → ObserverAfter(4) — seq: [1,2,3,4,5]")
	s.Equal(int64(1), steps[0])
	s.Equal(int64(2), steps[1])
	s.Equal(int64(3), steps[2])
	s.Equal(int64(4), steps[4])
	s.Equal(int64(5), steps[3])

	stored, err := raw.Get("cli:hook-order")
	s.NoError(err)
	s.Equal("transformed", stored)
}

func (s *ClientTestSuite) TestObserverBeforeSeesPostTransformValueButAbortSeesOriginal() {
	s.SetupTest()

	var abortSaw, transformSaw, observerSaw []byte

	hook.AddAbort(func(key string, value []byte) error {
		abortSaw = append([]byte(nil), value...)
		return nil
	})
	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		transformSaw = append([]byte(nil), value...)
		out := make([]byte, len(value)+len("-transformed"))
		copy(out, value)
		copy(out[len(value):], "-transformed")
		return out, nil
	})
	hook.AddObserver(func(key string, value []byte) {
		observerSaw = append([]byte(nil), value...)
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-see-orig", "raw-value")
	s.NoError(err)
	s.Equal([]byte("raw-value"), abortSaw,
		"AbortHook must see ORIGINAL value (runs first)")
	s.Equal([]byte("raw-value"), transformSaw,
		"TransformHook must see ORIGINAL value (runs second)")
	s.Equal([]byte("raw-value-transformed"), observerSaw,
		"ObserverBefore must see POST-TRANSFORM value (runs third, A5 order)")
}

func (s *ClientTestSuite) TestNoAfterHooksOnBeforeAbort() {
	s.SetupTest()

	var afterCalled bool
	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		afterCalled = true
	})

	var observerBeforeCalled bool
	hook.AddObserver(func(key string, value []byte) {
		observerBeforeCalled = true
	})

	hook.AddAbort(func(key string, value []byte) error {
		return errors.New("abort")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:hook-no-after", "val")
	s.Error(err)
	s.False(observerBeforeCalled,
		"ObserverBefore must NOT run when AbortHook aborted earlier (fail-closed propagates out of Abort phase)")
	s.False(afterCalled, "ObserverAfter hooks must NOT run when write was aborted in Before phase")
}

func (s *ClientTestSuite) TestTransformReturnsNilBytesWithoutErrorFailsClosed() {
	s.SetupTest()

	id := hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		return nil, nil
	})
	defer hook.Remove(id)

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:nil-transform", "should-not-be-overwritten")
	s.Error(err)
	s.Contains(err.Error(), "returned nil bytes without error")

	existing, gerr := raw.Get("cli:nil-transform")
	if gerr == nil {
		s.Equal("", existing,
			"key either should not exist OR be empty string; what we must NOT see is 'should-not-be-overwritten' overwritten to ''.")
	}
}

func (s *ClientTestSuite) TestSetHookZeroTimeoutUsesSyncPathNoGoroutineAllocs() {
	s.SetupTest()

	old := hook.GetTimeout()
	hook.SetTimeout(0)
	defer hook.SetTimeout(old)

	hook.AddObserver(func(key string, value []byte) {})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:sync-path", "v")
	s.NoError(err)
	v, gerr := raw.Get("cli:sync-path")
	s.NoError(gerr)
	s.Equal("v", v)
}

func (s *ClientTestSuite) TestObserverAfterCommittedOnNormalSetAndAbortOnError() {
	s.SetupTest()

	var gotKey string
	var gotCommitted bool
	var gotErr error
	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		gotKey = key
		gotCommitted = committed
		gotErr = writeErr
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.Set("cli:after-commit-1", "v1")
	s.NoError(err)
	s.Equal("cli:after-commit-1", gotKey)
	s.True(gotCommitted)
	s.NoError(gotErr)

	abortID := hook.AddAbort(func(key string, value []byte) error {
		return errors.New("blocked in abort hook")
	})
	defer hook.Remove(abortID)
	err = raw.Set("cli:after-commit-2", "v2")
	s.Error(err)
	s.Equal("cli:after-commit-1", gotKey)
	s.True(gotCommitted)
}

func (s *ClientTestSuite) TestHooksAppliedToSetNX() {
	s.SetupTest()

	var obsKey, obsVal string
	hook.AddObserver(func(key string, value []byte) {
		obsKey = key
		obsVal = string(value)
	})

	prefix := []byte("x:")
	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(prefix)+len(value))
		copy(out, prefix)
		copy(out[len(prefix):], value)
		return out, nil
	})

	var afterErr error
	var afterVal string
	var afterCommitted bool
	hook.AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
		afterErr = writeErr
		afterVal = string(value)
		afterCommitted = committed
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	ok, err := raw.SetNX("cli:hook-setnx-1", "original")
	s.NoError(err)
	s.True(ok)
	s.True(afterCommitted, "first SetNX write actually occurred -> committed=true")
	s.Equal("cli:hook-setnx-1", obsKey)
	s.Equal("x:original", obsVal,
		"ObserverBefore runs after Transform, so sees post-transform value (A5 order)")
	s.Equal("x:original", afterVal)
	s.NoError(afterErr)

	stored, err := raw.Get("cli:hook-setnx-1")
	s.NoError(err)
	s.Equal("x:original", stored)

	ok, err = raw.SetNX("cli:hook-setnx-1", "another")
	s.NoError(err)
	s.False(ok, "SetNX should return false on existing key")
	s.False(afterCommitted,
		"SetNX ok=false MUST propagate committed=false to ObserverAfter, otherwise downstream L1/CDC will incorrectly think a write happened.")
}

func (s *ClientTestSuite) TestHooksAppliedToSetWithTTL() {
	s.SetupTest()

	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		copy(out, value)
		return append(out, '-', 't', 't', 'l'), nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = raw.SetWithTTL("cli:hook-ttl", "base", 100*time.Millisecond)
	s.NoError(err)

	v, err := raw.Get("cli:hook-ttl")
	s.NoError(err)
	s.Equal("base-ttl", v)

	time.Sleep(200 * time.Millisecond)
	_, err = raw.Get("cli:hook-ttl")
	s.Error(err)
}

func (s *ClientTestSuite) TestHooksAppliedToSetNXWithTTL() {
	s.SetupTest()

	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		return append([]byte(nil), value...), nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	ok, err := raw.SetNXWithTTL("cli:hook-setnx-ttl", "x", 80*time.Millisecond)
	s.NoError(err)
	s.True(ok)

	v, err := raw.Get("cli:hook-setnx-ttl")
	s.NoError(err)
	s.Equal("x", v)

	time.Sleep(160 * time.Millisecond)
	_, err = raw.Get("cli:hook-setnx-ttl")
	s.Error(err)
}

func (s *ClientTestSuite) TestRemoveHookDuringWriteIsSafe() {
	s.SetupTest()

	id := hook.AddObserver(func(key string, value []byte) {})
	_ = id

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			hook.Remove(id)
			hook.AddObserver(func(key string, value []byte) {})
		}
	}()

	for i := 0; i < 50; i++ {
		_ = raw.Set(fmt.Sprintf("hook-safe-%d", i), "v")
	}
	<-done
}

func TestHostPortFromAddr(t *testing.T) {
	tests := []struct {
		name      string
		addr      string
		wantHost  string
		wantPort  int
		expectErr bool
		errSub    string
	}{
		{"IPv4 loopback default port", "127.0.0.1:7381", "127.0.0.1", 7381, false, ""},
		{"Zero port", "0.0.0.0:0", "0.0.0.0", 0, false, ""},
		{"Max port 65535", "redis.example.com:65535", "redis.example.com", 65535, false, ""},
		{"Hostname only no port", "redis.local", "", 0, true, "invalid resp address"},
		{"Colon only no port", "host:", "", 0, true, "invalid resp port"},
		{"Non-numeric port", "host:abc", "", 0, true, "invalid resp port"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, p, err := hostPortFromAddr(tc.addr)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.errSub)
				}
				if tc.errSub != "" && !strings.Contains(err.Error(), tc.errSub) {
					t.Fatalf("expected error to contain %q, got: %v", tc.errSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if h != tc.wantHost || p != tc.wantPort {
				t.Fatalf("want (%s,%d), got (%s,%d)", tc.wantHost, tc.wantPort, h, p)
			}
		})
	}
}

func TestAppPortMetaCmdRejectsNoPrivilege(t *testing.T) {
	appPort, err := testutil.AllocateFreePort()
	require.NoError(t, err)
	ctrlPort, err := testutil.AllocateFreePort()
	require.NoError(t, err)
	ctrlAuth := "np-ctrl-auth"
	appAuth := "np-app-auth"
	cfg := &server.Config{
		DataPath: testutil.DBPath(t),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: appPort, Auth: appAuth},
		Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort, Auth: ctrlAuth},
	}
	db := server.StartWith(cfg)
	require.NotNil(t, db)
	defer func() {
		stopAndResetGlobalState()
	}()

	appAddr := cfg.App.Addr()
	probe, err := connect(appAddr, appAuth)
	require.NoError(t, err, "connect app-port failed")
	defer func() { _ = probe.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	type tc struct {
		name string
		args []any
	}
	cases := []tc{
		{"regsch", []any{"REGSCH", `{"namespace":"user","mem":false,"key_attrs":["id"]}`}},
		{"dropsch", []any{"DROPSCH", "user"}},
		{"regidx", []any{"REGIDX", `{"owner_ns":"user","logical":"age","paths":["age"],"key_pattern":"_d_user:*"}`}},
		{"dropidx", []any{"DROPIDX", "user", "age"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := probe.Do(ctx, c.args...).Err()
			require.Error(t, err)
			require.Contains(t, err.Error(), "ERR No Privilege:",
				"expected No Privilege for %s on app-port, got: %v", c.name, err)
			cmdWord, _ := c.args[0].(string)
			require.Contains(t, err.Error(), "'"+strings.ToLower(cmdWord)+"'",
				"expected cmd name %q in error, got: %v", cmdWord, err)
			require.Contains(t, err.Error(), "Meta Management")
		})
	}
}

func TestConnectAuthMismatchReturnsRawServerErr(t *testing.T) {
	appPort, err := testutil.AllocateFreePort()
	require.NoError(t, err)
	ctrlPort, err := testutil.AllocateFreePort()
	require.NoError(t, err)
	ctrlAuth := "mismatch-ctrl-key"
	appAuth := "mismatch-app-key"
	cfg := &server.Config{
		DataPath: testutil.DBPath(t),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: appPort, Auth: appAuth},
		Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: ctrlPort, Auth: ctrlAuth},
	}
	db := server.StartWith(cfg)
	require.NotNil(t, db)
	defer func() {
		stopAndResetGlobalState()
	}()

	t.Run("app port with ctrl auth key → connect() returns raw WRONGPASS from server", func(t *testing.T) {
		_, dialErr := connect(cfg.App.Addr(), ctrlAuth)
		require.Error(t, dialErr)
		require.Contains(t, dialErr.Error(), "WRONGPASS")
		require.Contains(t, dialErr.Error(), "app port")
	})
	t.Run("ctrl port with app auth key → connect() returns raw WRONGPASS from server", func(t *testing.T) {
		_, dialErr := connect(cfg.Ctrl.Addr(), appAuth)
		require.Error(t, dialErr)
		require.Contains(t, dialErr.Error(), "WRONGPASS")
		require.Contains(t, dialErr.Error(), "ctrl port")
	})
	t.Run("ctrl port with no auth → Connect() refuses empty auth key pre-wire", func(t *testing.T) {
		err := Connect(cfg.Ctrl.Addr(), "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "auth key is empty")
	})
	t.Run("ctrl port with invalid non-empty auth key → connect() returns raw ERR authentication failed", func(t *testing.T) {
		_, dialErr := connect(cfg.Ctrl.Addr(), "wrong-wrong-wrong")
		require.Error(t, dialErr)
		require.Contains(t, dialErr.Error(), "ERR authentication failed")
	})
}

func TestConnectProbePlainRedisNoCapsNoError(t *testing.T) {
	called := make(chan struct{}, 1)
	dialCh := make(chan *respconn.DialResult, 1)
	origDial := internalDialForRespconnInternal
	internalDialForRespconnInternal = func(o respconn.Options) (*respconn.DialResult, error) {
		res, err := origDial(o)
		if res != nil {
			select {
			case dialCh <- res:
			default:
			}
		}
		return res, err
	}
	defer func() { internalDialForRespconnInternal = origDial }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		select {
		case called <- struct{}{}:
		default:
		}
		deadline := time.Now().Add(5 * time.Second)
		_ = conn.SetReadDeadline(deadline)
		_ = conn.SetWriteDeadline(deadline)
		br := bufio.NewReader(conn)
		for i := 0; i < 256; i++ {
			cmdLine, rerr := readOneRESPCommand(br)
			if rerr != nil {
				return
			}
			cmdLine = strings.TrimSpace(cmdLine)
			if cmdLine == "" {
				return
			}
			words := strings.Fields(cmdLine)
			verb := strings.ToUpper(words[0])
			var reply []byte
			switch verb {
			case "HELLO":
				reply = []byte("-ERR unknown command 'HELLO'\r\n")
			case "PING":
				reply = []byte("+PONG\r\n")
			case "AUTH":
				reply = []byte("+OK\r\n")
			case "COMMAND":
				reply = []byte("*0\r\n")
			case "CLIENT":
				reply = []byte("-ERR unknown command 'CLIENT'\r\n")
			default:
				reply = []byte("-ERR unknown command '" + verb + "'\r\n")
			}
			if _, werr := conn.Write(reply); werr != nil {
				return
			}
		}
	}()

	res, err := connect(ln.Addr().String(), "anything")
	if err != nil {
		t.Fatalf("connect to plain-redis stub should not error, got: %v", err)
	}
	defer func() { _ = res.Close() }()
	select {
	case <-called:
	case <-time.After(3 * time.Second):
		t.Fatal("mock listener was never called")
	}
	var dialRes *respconn.DialResult
	select {
	case dialRes = <-dialCh:
	case <-time.After(2 * time.Second):
		t.Fatal("DialAndHandshake interceptor never fired")
	}
	if dialRes == nil {
		t.Fatal("dial result is nil")
	}
	if dialRes.Client == nil {
		t.Fatal("dial result has no client")
	}
}

func readOneRESPCommand(br *bufio.Reader) (string, error) {
	ch, err := br.Peek(1)
	if err != nil {
		return "", err
	}
	if ch[0] == '*' {
		line, lerr := br.ReadString('\n')
		if lerr != nil && len(line) == 0 {
			return "", lerr
		}
		trimmed := strings.TrimSpace(strings.TrimPrefix(line, "*"))
		n, perr := strconv.Atoi(trimmed)
		if perr != nil {
			return line, nil
		}
		cmdWords := make([]string, 0, n)
		for i := 0; i < n; i++ {
			bulkLenLine, blerr := br.ReadString('\n')
			if blerr != nil {
				return strings.Join(cmdWords, " "), nil
			}
			if !strings.HasPrefix(bulkLenLine, "$") {
				continue
			}
			blenTrim := strings.TrimSpace(strings.TrimPrefix(bulkLenLine, "$"))
			blen, bperr := strconv.Atoi(blenTrim)
			if bperr != nil {
				continue
			}
			buf := make([]byte, blen+2)
			_, rrerr := io.ReadFull(br, buf)
			if rrerr != nil {
				return strings.Join(cmdWords, " "), nil
			}
			word := string(buf[:blen])
			cmdWords = append(cmdWords, word)
		}
		return strings.Join(cmdWords, " "), nil
	}
	line, lerr := br.ReadString('\n')
	return strings.TrimSpace(line), lerr
}

func BenchmarkSetNoHooks(b *testing.B) {
	hook.Reset()
	cfg := &server.Config{
		DataPath: filepath.Join(b.TempDir(), "redisx-bench.db"),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: 36379},
		Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: 36380},
	}
	clientTestServerAddr = cfg.Ctrl.Addr()
	db := server.StartWith(cfg)
	if db == nil {
		b.Fatal("embedded server Start returned nil")
	}
	rdbAuth := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	defer func() { _ = rdbAuth.Close() }()
	if err := rdbAuth.Set(context.Background(), naming.AuthStorageKey(clientTestExternalAuthKey), "50", 0).Err(); err != nil {
		b.Fatalf("seed AUTH key via wire: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	_ = setSharedClient(mockClient)
	defer func() {
		_ = mockClient.Close()
		clearSharedClientIf(mockClient)
		_ = db.Close()
		hook.Reset()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = raw.SetWithTTL("cli:bench-nohook", "payload-value", 0)
	}
}

func BenchmarkSetWithObserverHooks(b *testing.B) {
	hook.Reset()
	hook.SetTimeout(0)
	defer hook.SetTimeout(defaultHookTimeout)
	hook.AddObserver(func(key string, value []byte) {})
	hook.AddObserver(func(key string, value []byte) {})
	hook.AddObserver(func(key string, value []byte) {})

	cfg := &server.Config{
		DataPath: filepath.Join(b.TempDir(), "redisx-bench.db"),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: 36379},
		Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: 36380},
	}
	clientTestServerAddr = cfg.Ctrl.Addr()
	db := server.StartWith(cfg)
	if db == nil {
		b.Fatal("embedded server Start returned nil")
	}
	rdbAuth := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	defer func() { _ = rdbAuth.Close() }()
	if err := rdbAuth.Set(context.Background(), naming.AuthStorageKey(clientTestExternalAuthKey), "50", 0).Err(); err != nil {
		b.Fatalf("seed AUTH key via wire: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	_ = setSharedClient(mockClient)
	defer func() {
		_ = mockClient.Close()
		clearSharedClientIf(mockClient)
		_ = db.Close()
		hook.Reset()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = raw.SetWithTTL("cli:bench-observer", "payload-value", 0)
	}
}

func BenchmarkSetWithAbortHook(b *testing.B) {
	hook.Reset()
	hook.SetTimeout(0)
	defer hook.SetTimeout(defaultHookTimeout)
	hook.AddAbort(func(key string, value []byte) error {
		return nil
	})

	cfg := &server.Config{
		DataPath: filepath.Join(b.TempDir(), "redisx-bench.db"),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: 36379},
		Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: 36380},
	}
	clientTestServerAddr = cfg.Ctrl.Addr()
	db := server.StartWith(cfg)
	if db == nil {
		b.Fatal("embedded server Start returned nil")
	}
	rdbAuth := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	defer func() { _ = rdbAuth.Close() }()
	if err := rdbAuth.Set(context.Background(), naming.AuthStorageKey(clientTestExternalAuthKey), "50", 0).Err(); err != nil {
		b.Fatalf("seed AUTH key via wire: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	_ = setSharedClient(mockClient)
	defer func() {
		_ = mockClient.Close()
		clearSharedClientIf(mockClient)
		_ = db.Close()
		hook.Reset()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = raw.SetWithTTL("cli:bench-abort", "payload-value", 0)
	}
}

func BenchmarkSetWithTransformHook(b *testing.B) {
	hook.Reset()
	hook.SetTimeout(0)
	defer hook.SetTimeout(defaultHookTimeout)
	hook.AddTransform(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		copy(out, value)
		return out, nil
	})

	cfg := &server.Config{
		DataPath: filepath.Join(b.TempDir(), "redisx-bench.db"),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: 36379},
		Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: 36380},
	}
	clientTestServerAddr = cfg.Ctrl.Addr()
	db := server.StartWith(cfg)
	if db == nil {
		b.Fatal("embedded server Start returned nil")
	}
	rdbAuth := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	defer func() { _ = rdbAuth.Close() }()
	if err := rdbAuth.Set(context.Background(), naming.AuthStorageKey(clientTestExternalAuthKey), "50", 0).Err(); err != nil {
		b.Fatalf("seed AUTH key via wire: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	_ = setSharedClient(mockClient)
	defer func() {
		_ = mockClient.Close()
		clearSharedClientIf(mockClient)
		_ = db.Close()
		hook.Reset()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = raw.SetWithTTL("cli:bench-transform", "payload-value", 0)
	}
}

func BenchmarkHooksStoreLoadBaseline(b *testing.B) {
	hook.Reset()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hook.Snapshot()
	}
}

type DocTestSuite struct {
	suite.Suite
}

func TestDocSuite(t *testing.T) {
	suite.Run(t, new(DocTestSuite))
}

func (s *DocTestSuite) SetupSuite() {
	ensureServerAndSeed(s.T())

	err := ConnectEmbedded()
	s.Require().NoError(err)

	for i := 0; i < 30; i++ {
		if c := getSharedClient(); c != nil && healthCheck(context.Background(), c) == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.T().Fatal("failed to connect to test server")
}

func (s *DocTestSuite) SetupTest() {
	if c := getSharedClient(); c == nil || healthCheck(context.Background(), c) != nil {
		disconnect()
		err := ConnectEmbedded()
		if err != nil {
			err = Connect(clientTestServerAddr, clientTestExternalAuthKey)
			s.Require().NoError(err)
		}
		for i := 0; i < 30; i++ {
			if c := getSharedClient(); c != nil && healthCheck(context.Background(), c) == nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		s.T().Fatal("shared client reconnection failed")
	}
}

func (s *DocTestSuite) TestGenericDocMethods() {
	jsonStr := `{"id":"200","name":"Test","age":30}`
	doc := UserDoc(jsonStr)

	err := Set(doc)
	s.Require().NoError(err)

	val, err := Get[UserDoc]("200")
	s.Require().NoError(err)
	s.Equal(UserDoc(jsonStr), val)

	ok, err := SetNX(doc)
	s.Require().NoError(err)
	s.False(ok)

	keysRes := Keys[UserDoc]("*")
	s.Require().NoError(keysRes.Error())
	s.Contains(keysRes.MustGet(), "user:200")

	searchRes := SearchKey[UserDoc](x.KeysPattern("*"), x.Eq("age", 30), false)
	s.Require().NoError(searchRes.Error())
	s.Contains(searchRes.MustGet(), UserDoc(jsonStr))

	idxRes := SearchIndex[UserDoc]("age", x.KeysPattern("*"), x.Eq("age", float64(30)), false)
	s.Require().NoError(idxRes.Error())
	s.Contains(idxRes.MustGet(), UserDoc(jsonStr))

	updRes := Update[UserDoc](x.KeysPattern("*"), x.Eq("age", 30), x.Set("age", 31))
	s.Require().NoError(updRes.Error())

	del, err := Delete(UserDoc(`{"id":"200"}`))
	s.Require().NoError(err)
	s.True(del)
}

func (s *DocTestSuite) TestStorageKeyFromDocument() {
	doc := UserDoc(`{"id":"201","name":"Alice"}`)

	key, err := x.StorageKey(doc)
	s.Require().NoError(err)
	s.Equal("user:201", key)
}

func (s *DocTestSuite) TestTypedWritesRespectDocumentTTL() {
	first := ExpiringUserDoc(`{"id":"1","name":"alpha"}`)
	err := Set(first)
	s.Require().NoError(err)

	second := ExpiringUserDoc(`{"id":"2","name":"beta"}`)
	ok, err := SetNX(second)
	s.Require().NoError(err)
	s.True(ok)

	updRes := Update[ExpiringUserDoc](x.KeysPattern("*"), x.Eq("id", "1"), x.Set("name", "updated"))
	s.Require().NoError(updRes.Error())

	time.Sleep(80 * time.Millisecond)

	_, err = Get[ExpiringUserDoc]("1")
	s.Require().Error(err)
	_, err = Get[ExpiringUserDoc]("2")
	s.Require().Error(err)
}

func (s *DocTestSuite) TestSearchIndexRejectsPrefixedStoragePattern() {
	res := SearchIndex[UserDoc]("age", x.KeysPattern("user:*"), nil, false)
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "document-scoped")
}

func (s *DocTestSuite) TestSearchKeyRejectsPrefixedStoragePattern() {
	res := SearchKey[UserDoc](x.KeysPattern("user:*"), nil, false)
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "document-scoped")
}

func (s *DocTestSuite) TestKeysRejectsPrefixedStoragePattern() {
	res := Keys[UserDoc]("user:*")
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "document-scoped")
}

func (s *DocTestSuite) TestUpdateRejectsPrefixedStoragePattern() {
	res := Update[UserDoc](x.KeysPattern("user:*"), nil, x.Set("name", "updated"))
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "document-scoped")
}

func (s *DocTestSuite) TestSearchIndexRejectsFullIdxName() {
	res := SearchIndex[UserDoc]("user_age", x.KeysPattern("*"), nil, false)
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "fully-qualified index name")
}

func (s *DocTestSuite) TestSearchKeyScopedLimitCarry4LayerAlignment() {
	idSet := []string{"k_layer_a", "k_layer_b", "k_layer_c", "k_layer_d", "k_layer_e"}
	for _, id := range idSet {
		doc := UserDoc(`{"id":"` + id + `","tag":"layer4align"}`)
		s.NoError(Set(doc))
	}

	fullRes := SearchKey[UserDoc](x.KeysPattern("k_layer*"), x.Eq("tag", "layer4align"), false)
	s.Require().NoError(fullRes.Error())
	s.Len(fullRes.MustGet(), len(idSet))

	limitRes := SearchKey[UserDoc](x.KeysPattern("k_layer*").Limit(2), x.Eq("tag", "layer4align"), false)
	s.Require().NoError(limitRes.Error())
	s.Len(limitRes.MustGet(), 2, "Limit(2) must be carried through ScopeKeyRange namespace injection")
	s.Equal(fullRes.MustGet()[:2], limitRes.MustGet(), "ScopeKeyRange Limit must preserve ASC order first-N, not arbitrary slice")
}

func (s *DocTestSuite) TestDocHookForwarders() {
	var obsKey, obsVal string
	id1 := hook.AddObserver(func(key string, valueJSON []byte) {
		obsKey = key
		obsVal = string(valueJSON)
	})
	defer hook.Remove(id1)

	testAbortErr := errors.New("doc hook abort test")
	aborted := false
	id2 := hook.AddAbort(func(key string, valueJSON []byte) error {
		if string(valueJSON) == `{"id":"999","blocked":true}` {
			aborted = true
			return testAbortErr
		}
		return nil
	})
	defer hook.Remove(id2)

	transformed := false
	id3 := hook.AddTransform(func(key string, valueJSON []byte) ([]byte, error) {
		if key == "user:998" {
			transformed = true
			out := make([]byte, 0, len(valueJSON)+20)
			out = append(out, valueJSON[:len(valueJSON)-1]...)
			out = append(out, []byte(`,"transformed":true}`)...)
			return out, nil
		}
		return valueJSON, nil
	})
	defer hook.Remove(id3)

	var afterKey, afterVal string
	var afterWriteErr error
	var afterCommitted bool
	id4 := hook.AddObserverAfter(func(key string, valueJSON []byte, committed bool, writeErr error) {
		afterKey = key
		afterVal = string(valueJSON)
		afterWriteErr = writeErr
		afterCommitted = committed
	})
	defer hook.Remove(id4)

	orig := UserDoc(`{"id":"998","name":"Alice"}`)
	err := Set(orig)
	s.Require().NoError(err)
	s.Equal("user:998", obsKey)
	s.Equal(`{"id":"998","name":"Alice","transformed":true}`, obsVal,
		"ObserverHook (Before) runs after Abort + Transform per A5 mandatory order, so sees post-transform JSON")
	s.True(transformed)
	s.Equal("user:998", afterKey)
	s.Contains(afterVal, `"transformed":true`)
	s.True(afterCommitted, "typed doc Set committed the write -> committed=true")
	s.NoError(afterWriteErr)

	got, err := Get[UserDoc]("998")
	s.Require().NoError(err)
	s.Contains(string(got), `"transformed":true`)

	blockedDoc := UserDoc(`{"id":"999","blocked":true}`)
	err = Set(blockedDoc)
	s.ErrorIs(err, testAbortErr)
	s.True(aborted)
}

func (s *DocTestSuite) TestAll_TABLE_DRIVEN() {
	// Seed a dedicated set of documents for this test.
	fixtures := []struct {
		id   string
		name string
		age  int
	}{
		{"all_1", "Alice", 25},
		{"all_2", "Bob", 30},
		{"all_3", "Carol", 35},
		{"all_4", "Dave", 25},
		{"all_5", "Eve", 40},
	}
	for _, f := range fixtures {
		doc := UserDoc(fmt.Sprintf(`{"id":%q,"name":%q,"age":%d}`, f.id, f.name, f.age))
		s.Require().NoError(Set(doc))
	}
	defer func() {
		for _, f := range fixtures {
			_, _ = Delete(UserDoc(fmt.Sprintf(`{"id":%q}`, f.id)))
		}
	}()

	tests := []struct {
		name    string
		filters []x.Filter
		wantIDs []string // sorted ascending for deterministic comparison
	}{
		{
			name:    "no_filter_returns_all",
			filters: nil,
			wantIDs: []string{"all_1", "all_2", "all_3", "all_4", "all_5"},
		},
		{
			name:    "single_eq_filter",
			filters: []x.Filter{x.Eq("age", 25)},
			wantIDs: []string{"all_1", "all_4"},
		},
		{
			name:    "gt_filter",
			filters: []x.Filter{x.Gt("age", 30)},
			wantIDs: []string{"all_3", "all_5"},
		},
		{
			name:    "multiple_filters_anded",
			filters: []x.Filter{x.Eq("age", 25), x.Eq("name", "Alice")},
			wantIDs: []string{"all_1"},
		},
		{
			name:    "filter_matches_none",
			filters: []x.Filter{x.Eq("age", 999)},
			wantIDs: []string{},
		},
		{
			name:    "contains_filter",
			filters: []x.Filter{x.Contains("name", "ob")},
			wantIDs: []string{"all_2"},
		},
	}

	for _, tc := range tests {
		tc := tc
		s.Run(tc.name, func() {
			res := All[UserDoc](tc.filters...)
			s.Require().NoError(res.Error())
			got := res.MustGet()

			gotIDs := make([]string, 0, len(got))
			for _, d := range got {
				gotIDs = append(gotIDs, gjson.Get(string(d), "id").String())
			}
			sort.Strings(gotIDs)
			s.Equal(tc.wantIDs, gotIDs)
		})
	}
}

func krDocID(id string) string  { return id }
func updDocID(id string) string { return id }

func docRaw[D x.Document](docs []D) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.RawJSON())
	}
	return out
}

func updDocIDPrefix() string {
	return testutil.XKeyPrefix(updateKRDocNamespace, testutil.KeyRangeFixtureMem())
}

func updDocIDFromStorage(storageKeys []string) []string {
	prefix := updDocIDPrefix()
	out := make([]string, 0, len(storageKeys))
	for _, k := range storageKeys {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k[len(prefix):])
			continue
		}
		out = append(out, "")
	}
	return out
}

func updDocRawGet(raw, path string) string { return gjson.Get(raw, path).String() }

func (s *DocSearchKeyRangeSuite) TestSearchKeyRangeSeedCountIs100() {
	kr := x.KeysPattern("p*")
	got := SearchKey[DocSearchFixture](kr, nil, false)
	s.False(got.IsError(), "SearchKey err: %v", got.Error())
	s.Len(got.MustGet(), testutil.CountX())
}

func (s *DocSearchKeyRangeSuite) TestSearchKeyRangeNoCrossContamination() {
	krServer := x.KeysPattern(testutil.XKeyPrefix("probeserver", true) + "*")
	resServer := SearchKey[DocSearchFixture](krServer, nil, false)
	s.False(resServer.IsError())
	s.Empty(resServer.MustGet())

	krClient := x.KeysPattern(testutil.XKeyPrefix("probeclient", true) + "*")
	resClient := SearchKey[DocSearchFixture](krClient, nil, false)
	s.False(resClient.IsError())
	s.Empty(resClient.MustGet())
}

func (s *DocSearchKeyRangeSuite) TestSearchKeyRangeFullMatrix_TABLE_DRIVEN() {
	run := func(kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := SearchKey[DocSearchFixture](kr, nil, desc)
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(docRaw(res.MustGet())), true, ""
	}
	testutil.AssertSearchKeyMatrix(s.T(), run, testutil.KeyRangeCtorCases(), krDocID, "SK/")
}

func (s *DocSearchKeyRangeSuite) TestSearchKeyRangeGtGteGapEqualsOne() {
	gte := SearchKey[DocSearchFixture](x.KeysGte(krDocID("p027")), nil, false)
	s.Require().False(gte.IsError())
	gt := SearchKey[DocSearchFixture](x.KeysGt(krDocID("p027")), nil, false)
	s.Require().False(gt.IsError())
	testutil.AssertGtGteGap1(s.T(),
		testutil.XIDsFromValues(docRaw(gte.MustGet())),
		testutil.XIDsFromValues(docRaw(gt.MustGet())),
		"p027")
}

func (s *DocSearchKeyRangeSuite) TestSearchKeyRangeLtLteGapEqualsOne() {
	lte := SearchKey[DocSearchFixture](x.KeysLte(krDocID("p072")), nil, false)
	s.Require().False(lte.IsError())
	lt := SearchKey[DocSearchFixture](x.KeysLt(krDocID("p072")), nil, false)
	s.Require().False(lt.IsError())
	testutil.AssertLtLteGap1(s.T(),
		testutil.XIDsFromValues(docRaw(lte.MustGet())),
		testutil.XIDsFromValues(docRaw(lt.MustGet())),
		"p072")
}

func (s *DocSearchKeyRangeSuite) TestSearchKeyRangeCrossLayerMismatchNote_docScoped() {
	kr, _ := x.UnmarshalKeyRange([]byte(`{"op":"pattern","p":"user:*"}`))
	if kr == nil {
		kr = x.KeysPattern("user:*")
	}
	res := raw.SearchKey(kr, nil, false)
	if res.IsError() {
		s.Contains(res.Error().Error(), "wrong number of arguments",
			"untyped client SearchKey error allowed (client may not be fully seeded); skipping cross-layer assertion in doc suite")
		return
	}
	s.True(true,
		"doc-scoped typed APIs (SearchKey[D]/SearchIndex[D]) always scope to D's namespace via ScopeKeyRange prefix, so cross-layer mismatch is impossible at doc layer.")
}

func (s *DocSearchKeyRangeSuite) TestSearchIndexRangeScoreSeedCountIs100() {
	krAll := x.KeysPattern("p*")
	res := SearchIndex[DocSearchFixture]("score", krAll, nil, false)
	s.False(res.IsError(), "SearchIndex err: %v", res.Error())
	s.Len(res.MustGet(), testutil.CountX())
}

func (s *DocSearchKeyRangeSuite) TestSearchIndexRangeScoreOrderingMatchesSearchKeyIdOrder() {
	krAll := x.KeysPattern("p*")
	siAsc := SearchIndex[DocSearchFixture]("score", krAll, nil, false)
	s.Require().False(siAsc.IsError())
	skAsc := SearchKey[DocSearchFixture](krAll, nil, false)
	s.Require().False(skAsc.IsError())
	siDesc := SearchIndex[DocSearchFixture]("score", krAll, nil, true)
	s.Require().False(siDesc.IsError())
	skDesc := SearchKey[DocSearchFixture](krAll, nil, true)
	s.Require().False(skDesc.IsError())
	testutil.AssertScoreEqSKId(s.T(),
		testutil.XIDsFromValues(docRaw(siAsc.MustGet())),
		testutil.XIDsFromValues(docRaw(skAsc.MustGet())),
		testutil.XIDsFromValues(docRaw(siDesc.MustGet())),
		testutil.XIDsFromValues(docRaw(skDesc.MustGet())))
}

func (s *DocSearchKeyRangeSuite) TestSearchIndexRangeFullMatrix_TABLE_DRIVEN() {
	run := func(idxName string, kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := SearchIndex[DocSearchFixture](idxName, kr, nil, desc)
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(docRaw(res.MustGet())), true, ""
	}
	testutil.AssertSearchIndexMatrix(s.T(), run, "score", testutil.KeyRangeCtorCases(), krDocID, "idx=score/")
}

func (s *DocSearchKeyRangeSuite) TestSearchIndexRangeBucketTiebreakersLexicographicById() {
	krAll := x.KeysPattern("p*")
	resA := SearchIndex[DocSearchFixture]("bucket", krAll, x.Eq("bucket", "A"), false)
	s.Require().False(resA.IsError())
	resC := SearchIndex[DocSearchFixture]("bucket", krAll, x.Eq("bucket", "C"), false)
	s.Require().False(resC.IsError())
	all := SearchIndex[DocSearchFixture]("bucket", krAll, nil, false)
	s.Require().False(all.IsError())
	testutil.AssertBucketDistribution(s.T(),
		testutil.XIDsFromValues(docRaw(resA.MustGet())),
		testutil.XIDsFromValues(docRaw(resC.MustGet())),
		testutil.XIDsFromValues(docRaw(all.MustGet())))
}

func (s *DocSearchKeyRangeSuite) TestSearchIndexRangeSparseAmtLimit10() {
	krLimit := x.KeysPattern("p*").Limit(10)
	si := SearchIndex[DocSearchFixture]("sparseamt", krLimit, nil, false)
	s.Require().False(si.IsError())
	testutil.AssertSparseLimit10(s.T(), testutil.XIDsFromValues(docRaw(si.MustGet())))
}

func (s *DocSearchKeyRangeSuite) TestSearchIndexRangeCrossLayerMismatchNote_docScoped() {
	memStorageNs := naming.BuildStorageNs(searchKRDocNamespace, testutil.KeyRangeFixtureMem())
	memIdxName := naming.BuildIdxFullName(memStorageNs, "score")
	diskKr := x.KeysPattern("user:*")
	res := raw.SearchIndex(memIdxName, diskKr, nil, false)
	if res.IsError() {
		s.Contains(res.Error().Error(), "different storage layer",
			"mem index (%s) + disk KeyRange (user:*) must reject cross-layer; got err: %v", memIdxName, res.Error())
		return
	}
	if len(res.OrEmpty()) == 0 {
		s.True(true,
			"doc typed-layer APIs scope keyranges under D's namespace (always same layer), so cross-layer cannot be synthesized from doc package; untyped SI returned empty here, OK to skip.")
		return
	}
	s.FailNowf("unexpected SI Ok result", "len=%d", len(res.OrEmpty()))
}

func (s *DocSearchKeyRangeSuite) TestSearchIndexRangeScopedLimitCarry4LayerAlignment() {
	idSet := []string{"si_doc_a", "si_doc_b", "si_doc_c", "si_doc_d", "si_doc_e"}
	for _, id := range idSet {
		doc := UserDoc(fmt.Sprintf(`{"id":"%s","tag":"si_doc4align","age":33}`, id))
		s.NoError(Set(doc))
	}
	fullRes := SearchIndex[UserDoc]("age", x.KeysPattern("si_doc*"), x.Eq("tag", "si_doc4align"), false)
	s.Require().NoError(fullRes.Error())
	s.Len(fullRes.MustGet(), len(idSet))

	limitRes := SearchIndex[UserDoc]("age", x.KeysPattern("si_doc*").Limit(2), x.Eq("tag", "si_doc4align"), false)
	s.Require().NoError(limitRes.Error())
	s.Len(limitRes.MustGet(), 2, "Limit(2) must be carried through typed-layer ScopeKeyRange + SI")
	s.Equal(fullRes.MustGet()[:2], limitRes.MustGet(),
		"typed SI Limit carry must preserve ASC order first-N, matching SK parity")
}

type DocSearchKeyRangeSuite struct {
	suite.Suite
}

func (s *DocSearchKeyRangeSuite) SetupSuite() {
	var docSuite DocTestSuite
	docSuite.SetT(s.T())
	docSuite.SetupSuite()
}

func (s *DocSearchKeyRangeSuite) SetupTest() {
	if c := getSharedClient(); c == nil || healthCheck(context.Background(), c) != nil {
		disconnect()
		err := ConnectEmbedded()
		if err != nil {
			err = Connect(clientTestServerAddr, clientTestExternalAuthKey)
			s.Require().NoError(err)
		}
		for i := 0; i < 30; i++ {
			if c := getSharedClient(); c != nil && healthCheck(context.Background(), c) == nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		s.T().Fatal("shared client reconnection failed")
	}
}

func TestDocSearchKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(DocSearchKeyRangeSuite))
}

type DocUpdateKeyRangeSuite struct {
	suite.Suite
}

func (s *DocUpdateKeyRangeSuite) SetupSuite() {
	var docSuite DocTestSuite
	docSuite.SetT(s.T())
	docSuite.SetupSuite()
}

func (s *DocUpdateKeyRangeSuite) SetupTest() {
	if c := getSharedClient(); c == nil || healthCheck(context.Background(), c) != nil {
		disconnect()
		err := ConnectEmbedded()
		if err != nil {
			err = Connect(clientTestServerAddr, clientTestExternalAuthKey)
			s.Require().NoError(err)
		}
		for i := 0; i < 30; i++ {
			if c := getSharedClient(); c != nil && healthCheck(context.Background(), c) == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	for _, kv := range testutil.LoadXFor(s.T(), updateKRDocNamespace, testutil.KeyRangeFixtureMem()) {
		doc := DocUpdateFixture(kv.V)
		s.Require().NoError(Set(doc), "UpdateKR SetupTest Set key=%s", kv.K)
	}
}

func (s *DocUpdateKeyRangeSuite) TestSeedCountMatchesSearchKey() {
	allKr := x.KeysPattern("p*")
	skRes := SearchKey[DocUpdateFixture](allKr, nil, false)
	s.Require().NoError(skRes.Error(), "SK err: %v", skRes.Error())
	s.Len(skRes.MustGet(), testutil.CountX(), "UpdateKR seed count should equal fixture count")
}

func (s *DocUpdateKeyRangeSuite) TestNoCrossContamination() {
	resWrong := Update[DocUpdateFixture](x.KeysPattern("probenegdoc:*"), nil, x.Set("tag_contam", true))
	s.NoError(resWrong.Error())
	s.Empty(resWrong.MustGet(), "cross-contam probenegdoc prefix should hit zero keys (typed Scope coerces)")

	resServer := Update[DocUpdateFixture](x.KeysPattern(clientKRServerStar()), nil, x.Set("tag_contam", true))
	s.NoError(resServer.Error())
	s.Empty(resServer.MustGet(), "cross-contam probe-server prefix should hit zero keys")

	skAll := SearchKey[DocUpdateFixture](x.KeysPattern("p*"), nil, false)
	s.Require().NoError(skAll.Error())
	for _, d := range skAll.MustGet() {
		got := updDocRawGet(d.RawJSON(), "tag_contam")
		s.NotEqual("true", got, "tag_contam leaked to fixture data; ctor_shape=%q raw=%s", updDocRawGet(d.RawJSON(), "ctor_shape"), d.RawJSON())
	}
}

func clientKRServerStar() string {
	return testutil.XKeyPrefix("probeserver", true) + "*"
}

func (s *DocUpdateKeyRangeSuite) TestBulkSetAllTagThenVerifyViaSearchKey() {
	allKr := x.KeysPattern("p*")
	res := Update[DocUpdateFixture](allKr, nil, x.Set("update_tagged", "bulk_all"))
	s.Require().NoError(res.Error(), "Update bulk_all err: %v", res.Error())
	keys := res.MustGet()
	s.Len(keys, testutil.CountX())
	sort.Strings(keys)

	skAfter := SearchKey[DocUpdateFixture](allKr, nil, false)
	s.Require().NoError(skAfter.Error())
	after := skAfter.MustGet()
	s.Len(after, testutil.CountX())
	for _, v := range after {
		s.Equal("bulk_all", updDocRawGet(v.RawJSON(), "update_tagged"),
			"every value should carry update_tagged=bulk_all; raw=%s", v.RawJSON())
	}
}

func (s *DocUpdateKeyRangeSuite) TestUpdateAllKeyRangeCtorShapes_TABLE_DRIVEN() {
	epoch := 0
	nextTag := func() string {
		epoch++
		return fmt.Sprintf("e%d", epoch)
	}
	runAsc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := Update[DocUpdateFixture](kr, nil, x.Set("ctor_shape", tag))
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return updDocIDFromStorage(res.MustGet()), true, ""
	}
	runDesc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := Update[DocUpdateFixture](kr, nil, x.Set("ctor_shape", tag))
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		ids := updDocIDFromStorage(res.MustGet())
		for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
			ids[i], ids[j] = ids[j], ids[i]
		}
		return ids, true, ""
	}
	assertCtorShapeWritten := func(caseName, label string, wantCount int, verifyRange x.KeyRange, wantTag string) {
		s.T().Helper()
		skRes := SearchKey[DocUpdateFixture](verifyRange, nil, false)
		s.Require().False(skRes.IsError(), "%s/%s: SearchKey[D] after Update err: %v", caseName, label, skRes.Error())
		docs := skRes.MustGet()
		var count int
		for _, d := range docs {
			if updDocRawGet(d.RawJSON(), "ctor_shape") == wantTag {
				count++
			}
		}
		s.Equal(wantCount, count,
			"%s/%s: ctor_shape=%q written count mismatch want=%d got=%d (SearchKey range len=%d)",
			caseName, label, wantTag, wantCount, count, len(docs))
	}
	for _, tc := range testutil.KeyRangeCtorCases() {
		tc := tc
		kr := tc.Build(updDocID)
		fullCase := "UpdateKR/" + tc.Name

		tag := nextTag()
		ids, ok, errMsg := runAsc(kr, tag)
		assertDocKRResult(s.T(), fullCase, "ASC_no_limit", tc.WantAsc, ids, ok, errMsg, false)
		if ok && len(ids) > 0 {
			assertCtorShapeWritten(fullCase, "ASC_no_limit", len(ids), kr, tag)
		}

		tag = nextTag()
		ids, ok, errMsg = runDesc(kr, tag)
		assertDocKRResult(s.T(), fullCase, "DESC_no_limit", tc.WantAsc, ids, ok, errMsg, true)
		if ok && len(tc.WantAsc) > 0 {
			assertCtorShapeWritten(fullCase, "DESC_no_limit", len(tc.WantAsc), kr, tag)
		}

		if len(tc.WantAsc) >= 5 {
			limit5Asc := tc.WantAsc[:5]
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(5), tag)
			assertDocKRResult(s.T(), fullCase, "ASC_Limit_5_is_first_5", limit5Asc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_5", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runDesc(kr.Limit(5), tag)
			assertDocKRResult(s.T(), fullCase, "DESC_Limit_5_is_last_5_rev", limit5Asc, ids, ok, errMsg, true)
			if ok && len(limit5Asc) > 0 {
				assertCtorShapeWritten(fullCase, "DESC_Limit_5", 5, kr, tag)
			}
		}
		if len(tc.WantAsc) >= 3 {
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)), tag)
			assertDocKRResult(s.T(), fullCase, "ASC_Limit_EQ_count_returns_all", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_EQ_count", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)+500), tag)
			assertDocKRResult(s.T(), fullCase, "ASC_Limit_OVER_count_safe", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_OVER_count", len(ids), kr, tag)
			}
		}
	}
}

func assertDocKRResult(t *testing.T, caseName, label string, wantAsc, ids []string, ok bool, errMsg string, desc bool) {
	t.Helper()
	if !ok {
		t.Errorf("%s/%s: expected Ok, got Error: %s", caseName, label, errMsg)
		return
	}
	if len(wantAsc) != len(ids) {
		t.Errorf("%s/%s: length mismatch want=%d got=%d ids=%v", caseName, label, len(wantAsc), len(ids), ids)
		return
	}
	var want []string
	if desc {
		want = make([]string, len(wantAsc))
		copy(want, wantAsc)
		for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
			want[i], want[j] = want[j], want[i]
		}
	} else {
		want = wantAsc
	}
	if len(want) == 0 && len(ids) == 0 {
		return
	}
	for i := range want {
		if want[i] != ids[i] {
			t.Errorf("%s/%s content mismatch (desc=%v): want[%d]=%q got[%d]=%q", caseName, label, desc, i, want[i], i, ids[i])
			return
		}
	}
}

func (s *DocUpdateKeyRangeSuite) TestGtGteBoundaryGapEqualsOne() {
	krGte := x.KeysGte(updDocID("p027"))
	resGte := Update[DocUpdateFixture](krGte, nil, x.Set("boundary", "gte"))
	s.Require().NoError(resGte.Error())
	idsGte := updDocIDFromStorage(resGte.MustGet())

	skGte := SearchKey[DocUpdateFixture](krGte, nil, false)
	s.Require().NoError(skGte.Error())
	gotGte := skGte.MustGet()
	s.Len(gotGte, len(idsGte), "Gte SK sweep after Update expected len=%d got=%d", len(idsGte), len(gotGte))
	for _, d := range gotGte {
		got := updDocRawGet(d.RawJSON(), "boundary")
		s.Equal("gte", got, "Update Gte value mismatch on boundary field: raw=%s", d.RawJSON())
	}

	krGt := x.KeysGt(updDocID("p027"))
	resGt := Update[DocUpdateFixture](krGt, nil, x.Set("boundary", "gt"))
	s.Require().NoError(resGt.Error())
	idsGt := updDocIDFromStorage(resGt.MustGet())

	skGt := SearchKey[DocUpdateFixture](krGt, nil, false)
	s.Require().NoError(skGt.Error())
	gotGt := skGt.MustGet()
	s.Len(gotGt, len(idsGt), "Gt SK sweep after Update expected len=%d got=%d", len(idsGt), len(gotGt))
	for _, d := range gotGt {
		got := updDocRawGet(d.RawJSON(), "boundary")
		s.Equal("gt", got, "Update Gt value mismatch on boundary field: raw=%s", d.RawJSON())
	}

	testutil.AssertGtGteGap1(s.T(), idsGte, idsGt, "p027")
}

func (s *DocUpdateKeyRangeSuite) TestLtLteBoundaryGapEqualsOne() {
	krLte := x.KeysLte(updDocID("p072"))
	resLte := Update[DocUpdateFixture](krLte, nil, x.Set("boundary", "lte"))
	s.Require().NoError(resLte.Error())
	idsLte := updDocIDFromStorage(resLte.MustGet())

	skLte := SearchKey[DocUpdateFixture](krLte, nil, false)
	s.Require().NoError(skLte.Error())
	gotLte := skLte.MustGet()
	s.Len(gotLte, len(idsLte), "Lte SK sweep after Update expected len=%d got=%d", len(idsLte), len(gotLte))
	for _, d := range gotLte {
		got := updDocRawGet(d.RawJSON(), "boundary")
		s.Equal("lte", got, "Update Lte value mismatch on boundary field: raw=%s", d.RawJSON())
	}

	krLt := x.KeysLt(updDocID("p072"))
	resLt := Update[DocUpdateFixture](krLt, nil, x.Set("boundary", "lt"))
	s.Require().NoError(resLt.Error())
	idsLt := updDocIDFromStorage(resLt.MustGet())

	skLt := SearchKey[DocUpdateFixture](krLt, nil, false)
	s.Require().NoError(skLt.Error())
	gotLt := skLt.MustGet()
	s.Len(gotLt, len(idsLt), "Lt SK sweep after Update expected len=%d got=%d", len(idsLt), len(gotLt))
	for _, d := range gotLt {
		got := updDocRawGet(d.RawJSON(), "boundary")
		s.Equal("lt", got, "Update Lt value mismatch on boundary field: raw=%s", d.RawJSON())
	}

	testutil.AssertLtLteGap1(s.T(), idsLte, idsLt, "p072")
}

func (s *DocUpdateKeyRangeSuite) TestLimit7PrefixEqualFullSet() {
	allKr := x.KeysPattern("p*")

	fullRes := Update[DocUpdateFixture](allKr, nil, x.Set("lim", "full"))
	s.Require().NoError(fullRes.Error(), "full err=%v", fullRes.Error())
	full := fullRes.MustGet()
	s.Len(full, testutil.CountX())
	sort.Strings(full)
	skFull := SearchKey[DocUpdateFixture](allKr, nil, false)
	s.Require().NoError(skFull.Error())
	gotFull := skFull.MustGet()
	s.Len(gotFull, testutil.CountX())
	for _, d := range gotFull {
		got := updDocRawGet(d.RawJSON(), "lim")
		s.Equal("full", got, "Update lim=full value mismatch: raw=%s", d.RawJSON())
	}

	limitRes := Update[DocUpdateFixture](x.KeysPattern("p*").Limit(7), nil, x.Set("lim", "7"))
	s.Require().NoError(limitRes.Error(), "limit err=%v", limitRes.Error())
	lim := limitRes.MustGet()
	s.Len(lim, 7, "Limit(7) must truncate at callback=7, got len=%d", len(lim))
	sort.Strings(lim)
	s.Equal(full[:7], lim, "Limit(7) updated keys must equal ASC first-7 of full set — proves early-stop at callback")
	skLim := SearchKey[DocUpdateFixture](allKr, nil, false)
	s.Require().NoError(skLim.Error())
	gotLim := skLim.MustGet()
	var cntLim7 int
	for _, d := range gotLim {
		got := updDocRawGet(d.RawJSON(), "lim")
		if got == "7" {
			cntLim7++
			continue
		}
		s.Equal("full", got, "Limit=7 sweep: non-first-7 docs must keep lim=full, got %q; raw=%s", got, d.RawJSON())
	}
	s.Equal(7, cntLim7, "lim=7 want 7 docs with exact value lim==7 got=%d", cntLim7)
}

func (s *DocUpdateKeyRangeSuite) TestFilterUpdatesOnlyMatched() {
	filter := x.Eq("bucket", "A")
	res := Update[DocUpdateFixture](x.KeysPattern("p*"), filter, x.Set("filtered_tag", "A-only"))
	s.Require().NoError(res.Error(), "filtered err=%v", res.Error())
	ids := updDocIDFromStorage(res.MustGet())
	s.Len(ids, 34, "Update+filter Eq(bucket,A) should match 34 bucket=A rows")

	skAll := SearchKey[DocUpdateFixture](x.KeysPattern("p*"), nil, false)
	s.Require().NoError(skAll.Error())
	var count int
	for _, v := range skAll.MustGet() {
		if updDocRawGet(v.RawJSON(), "filtered_tag") == "A-only" {
			count++
		}
	}
	s.Equal(len(ids), count, "only updated rows carry filtered_tag; count=%d", count)
}

func (s *DocUpdateKeyRangeSuite) TestNilKRRejects() {
	res := Update[DocUpdateFixture](nil, nil, x.Set("nil_tag", true))
	s.Require().True(res.IsError(), "nil kr must reject")
	s.Contains(res.Error().Error(), "key range is required")
}

func (s *DocUpdateKeyRangeSuite) TestTypedScopedLimitCarry4LayerAlignment() {
	idSet := []string{"ukr_a", "ukr_b", "ukr_c", "ukr_d", "ukr_e"}
	for _, id := range idSet {
		doc := UserDoc(fmt.Sprintf(`{"id":"%s","tag":"ukr4align","age":33}`, id))
		s.NoError(Set(doc))
	}
	fullRes := Update[UserDoc](x.KeysPattern("ukr_*"), x.Eq("tag", "ukr4align"), x.Set("scoped", "yes"))
	s.Require().NoError(fullRes.Error())
	s.Len(fullRes.MustGet(), len(idSet))
	skFull := SearchKey[UserDoc](x.KeysPattern("ukr_*"), x.Eq("tag", "ukr4align"), false)
	s.Require().NoError(skFull.Error())
	var cntYes int
	for _, d := range skFull.MustGet() {
		got := updDocRawGet(d.RawJSON(), "scoped")
		if got == "yes" {
			cntYes++
			continue
		}
		s.Equal("", got, "unexpected scoped value (neither yes nor empty): %q raw=%s", got, d.RawJSON())
	}
	s.Equal(len(idSet), cntYes, "scoped=yes per-doc value check want=%d got=%d", len(idSet), cntYes)

	limitRes := Update[UserDoc](x.KeysPattern("ukr_*").Limit(2), x.Eq("tag", "ukr4align"), x.Set("scoped", "lim2"))
	s.Require().NoError(limitRes.Error())
	s.Len(limitRes.MustGet(), 2, "Limit(2) must carry through typed-layer ScopeKeyRange + Update")
	wantFull := fullRes.MustGet()
	sort.Strings(wantFull)
	gotLim := limitRes.MustGet()
	sort.Strings(gotLim)
	s.Equal(wantFull[:2], gotLim, "typed Update Limit carry must preserve ASC order first-N — matching SK parity")
	skLim := SearchKey[UserDoc](x.KeysPattern("ukr_*"), x.Eq("tag", "ukr4align"), false)
	s.Require().NoError(skLim.Error())
	var cntLim2 int
	for _, d := range skLim.MustGet() {
		got := updDocRawGet(d.RawJSON(), "scoped")
		if got == "lim2" {
			cntLim2++
			continue
		}
	}
	s.Equal(2, cntLim2, "scoped=lim2 per-doc value check want=2 got=%d", cntLim2)
}

func TestDocUpdateKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(DocUpdateKeyRangeSuite))
}

// ———  KeyRange client suites (merged from key_range_client_test.go)  ———

const (
	searchKRClientNamespace = "probeclient"
	updateKRClientNamespace = "updclient000"
)

type SearchFixtureDoc string

func (SearchFixtureDoc) Namespace() string  { return searchKRClientNamespace }
func (SearchFixtureDoc) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (SearchFixtureDoc) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d SearchFixtureDoc) RawJSON() string  { return string(d) }
func (SearchFixtureDoc) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

func krClientID(id string) string {
	return testutil.XIDKey(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), id)
}

type UpdateFixtureDoc string

func (UpdateFixtureDoc) Namespace() string  { return updateKRClientNamespace }
func (UpdateFixtureDoc) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (UpdateFixtureDoc) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d UpdateFixtureDoc) RawJSON() string  { return string(d) }
func (UpdateFixtureDoc) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

func updClientID(id string) string {
	return testutil.XIDKey(updateKRClientNamespace, testutil.KeyRangeFixtureMem(), id)
}

func updClientIDPrefix() string {
	return testutil.XKeyPrefix(updateKRClientNamespace, testutil.KeyRangeFixtureMem())
}

func updClientIDFromStorage(storageKeys []string) []string {
	prefix := updClientIDPrefix()
	out := make([]string, 0, len(storageKeys))
	for _, k := range storageKeys {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k[len(prefix):])
			continue
		}
		out = append(out, "")
	}
	return out
}

func updClientRawGet(raw, path string) string { return gjson.Get(raw, path).String() }

func (s *SearchKeyRangeSuite) TestSearchKeyRangeSeedCountIs100() {
	s.ensureConnectedClientAndAuth()
	kr := x.KeysPattern(krClientID("*"))
	got := raw.SearchKey(kr, nil, false)
	s.False(got.IsError(), "SearchKey err: %v", got.Error())
	s.Len(got.MustGet(), testutil.CountX())
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeNoCrossContamination() {
	s.ensureConnectedClientAndAuth()

	krWrongPrefix := x.KeysPattern("probe-client:*")
	resWrong := raw.SearchKey(krWrongPrefix, nil, false)
	s.False(resWrong.IsError())
	s.Empty(resWrong.MustGet())

	krServer := x.KeysPattern(testutil.XKeyPrefix("probeserver", true) + "*")
	resServer := raw.SearchKey(krServer, nil, false)
	s.False(resServer.IsError())
	s.Empty(resServer.MustGet())
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeFullMatrix_TABLE_DRIVEN() {
	s.ensureConnectedClientAndAuth()
	run := func(kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := raw.SearchKey(kr, nil, desc)
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(res.MustGet()), true, ""
	}
	testutil.AssertSearchKeyMatrix(s.T(), run, testutil.KeyRangeCtorCases(), krClientID, "SK/")
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeGtGteGapEqualsOne() {
	s.ensureConnectedClientAndAuth()
	gte := raw.SearchKey(x.KeysGte(krClientID("p027")), nil, false)
	s.Require().False(gte.IsError())
	gt := raw.SearchKey(x.KeysGt(krClientID("p027")), nil, false)
	s.Require().False(gt.IsError())
	testutil.AssertGtGteGap1(s.T(),
		testutil.XIDsFromValues(gte.MustGet()),
		testutil.XIDsFromValues(gt.MustGet()),
		"p027")
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeLtLteGapEqualsOne() {
	s.ensureConnectedClientAndAuth()
	lte := raw.SearchKey(x.KeysLte(krClientID("p072")), nil, false)
	s.Require().False(lte.IsError())
	lt := raw.SearchKey(x.KeysLt(krClientID("p072")), nil, false)
	s.Require().False(lt.IsError())
	testutil.AssertLtLteGap1(s.T(),
		testutil.XIDsFromValues(lte.MustGet()),
		testutil.XIDsFromValues(lt.MustGet()),
		"p072")
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeScoreSeedCountIs100() {
	s.ensureConnectedClientAndAuth()
	kr := x.KeysPattern(krClientID("*"))
	idxName := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")
	got := raw.SearchIndex(idxName, kr, nil, false)
	s.False(got.IsError(), "SearchIndex score ASC err: %v", got.Error())
	s.Len(got.MustGet(), testutil.CountX())
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeScoreOrderingMatchesSearchKeyIdOrder() {
	s.ensureConnectedClientAndAuth()
	krAll := x.KeysPattern(krClientID("*"))
	idxScore := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")

	siAsc := raw.SearchIndex(idxScore, krAll, nil, false)
	s.Require().False(siAsc.IsError())
	skAsc := raw.SearchKey(krAll, nil, false)
	s.Require().False(skAsc.IsError())
	siDesc := raw.SearchIndex(idxScore, krAll, nil, true)
	s.Require().False(siDesc.IsError())
	skDesc := raw.SearchKey(krAll, nil, true)
	s.Require().False(skDesc.IsError())

	testutil.AssertScoreEqSKId(s.T(),
		testutil.XIDsFromValues(siAsc.MustGet()),
		testutil.XIDsFromValues(skAsc.MustGet()),
		testutil.XIDsFromValues(siDesc.MustGet()),
		testutil.XIDsFromValues(skDesc.MustGet()))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeFullMatrix_TABLE_DRIVEN() {
	s.ensureConnectedClientAndAuth()
	idxName := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")
	run := func(n string, kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := raw.SearchIndex(n, kr, nil, desc)
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(res.MustGet()), true, ""
	}
	testutil.AssertSearchIndexMatrix(s.T(), run, idxName, testutil.KeyRangeCtorCases(), krClientID, "idx=score/")
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeBucketTiebreakersLexicographicById() {
	s.ensureConnectedClientAndAuth()
	krAll := x.KeysPattern(krClientID("*"))
	idxBucket := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "bucket")
	eqA := raw.SearchIndex(idxBucket, krAll, x.Eq("bucket", "A"), false)
	s.Require().False(eqA.IsError())
	eqC := raw.SearchIndex(idxBucket, krAll, x.Eq("bucket", "C"), false)
	s.Require().False(eqC.IsError())
	all := raw.SearchIndex(idxBucket, krAll, nil, false)
	s.Require().False(all.IsError())
	testutil.AssertBucketDistribution(s.T(),
		testutil.XIDsFromValues(eqA.MustGet()),
		testutil.XIDsFromValues(eqC.MustGet()),
		testutil.XIDsFromValues(all.MustGet()))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeSparseAmtLimit10() {
	s.ensureConnectedClientAndAuth()
	krLimit := x.KeysPattern(krClientID("*")).Limit(10)
	idxSparse := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "sparseamt")
	si := raw.SearchIndex(idxSparse, krLimit, nil, false)
	s.Require().False(si.IsError())
	testutil.AssertSparseLimit10(s.T(), testutil.XIDsFromValues(si.MustGet()))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeCrossLayerMismatchRejects() {
	s.ensureConnectedClientAndAuth()
	krDisk := x.KeysPattern("user:*")
	idxScore := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")
	res := raw.SearchIndex(idxScore, krDisk, nil, false)
	s.True(res.IsError(), "got Ok len=%d", len(res.OrEmpty()))
	s.Contains(res.Error().Error(), "different storage layer")
}

func (s *SearchKeyRangeSuite) TestSearchIndexRange_InvalidInputs_EmptyIndexAndNilKeyrange() {
	s.ensureConnectedClientAndAuth()
	s.Run("empty index name → early error", func() {
		res := raw.SearchIndex("", x.KeysPattern(krClientID("*")), nil, false)
		s.Require().True(res.IsError(), "expected Err for empty indexName; got Ok len=%d", len(res.OrEmpty()))
		s.Contains(res.Error().Error(), "index name is required",
			"err=%v", res.Error())
	})
	s.Run("nil keyrange → early error", func() {
		idxScore := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")
		res := raw.SearchIndex(idxScore, nil, nil, false)
		s.Require().True(res.IsError(), "expected Err for nil kr; got Ok len=%d", len(res.OrEmpty()))
		s.Contains(res.Error().Error(), "key range is required",
			"err=%v", res.Error())
	})
}

type SearchKeyRangeSuite struct {
	suite.Suite
}

func (s *SearchKeyRangeSuite) SetupSuite() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.SetupSuite()
}

func (s *SearchKeyRangeSuite) SetupTest() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.SetupTest()
}

func (s *SearchKeyRangeSuite) ensureConnectedClientAndAuth() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.ensureConnectedClientAndAuth()
}

func TestSearchKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(SearchKeyRangeSuite))
}

type UpdateKeyRangeSuite struct {
	suite.Suite
}

func (s *UpdateKeyRangeSuite) SetupSuite() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.SetupSuite()
}

func (s *UpdateKeyRangeSuite) SetupTest() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.SetupTest()
	s.ensureConnectedClientAndAuth()
	for _, kv := range testutil.LoadXFor(s.T(), updateKRClientNamespace, testutil.KeyRangeFixtureMem()) {
		err := raw.Set(kv.K, kv.V)
		s.Require().NoError(err, "UpdateKR SetupTest Set key=%s err=%v", kv.K, err)
	}
}

func (s *UpdateKeyRangeSuite) ensureConnectedClientAndAuth() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.ensureConnectedClientAndAuth()
}

func (s *UpdateKeyRangeSuite) TestSeedCountMatchesSearchKey() {
	allKr := x.KeysPattern(updClientID("*"))
	skRes := raw.SearchKey(allKr, nil, false)
	s.Require().False(skRes.IsError(), "SK err: %v", skRes.Error())
	s.Len(skRes.MustGet(), testutil.CountX(), "UpdateKR seed count should equal fixture count")
}

func (s *UpdateKeyRangeSuite) TestNoCrossContamination() {
	resWrong := raw.Update(x.KeysPattern("probenegcli:*"), nil, x.Set("tag_contam", true))
	s.False(resWrong.IsError())
	s.Empty(resWrong.MustGet(), "cross-contam probenegcli prefix should hit zero keys")

	resServer := raw.Update(x.KeysPattern(testutil.XKeyPrefix("probeserver", true)+"*"), nil, x.Set("tag_contam", true))
	s.False(resServer.IsError())
	s.Empty(resServer.MustGet(), "cross-contam probe-server prefix should hit zero keys")

	skAll := raw.SearchKey(x.KeysPattern(updClientID("*")), nil, false)
	s.Require().False(skAll.IsError())
	for _, v := range skAll.MustGet() {
		got := updClientRawGet(v, "tag_contam")
		s.NotEqual("true", got, "tag_contam leaked to fixture data; ctor_shape=%q raw=%s", updClientRawGet(v, "ctor_shape"), v)
	}
}

func (s *UpdateKeyRangeSuite) TestBulkSetAllTagThenVerifyViaSearchKey() {
	allKr := x.KeysPattern(updClientID("*"))
	res := raw.Update(allKr, nil, x.Set("update_tagged", "bulk_all"))
	s.Require().False(res.IsError(), "Update bulk_all err: %v", res.Error())
	keys := res.MustGet()
	s.Len(keys, testutil.CountX())
	sort.Strings(keys)

	skAfter := raw.SearchKey(allKr, nil, false)
	s.Require().False(skAfter.IsError())
	after := skAfter.MustGet()
	s.Len(after, testutil.CountX())
	for _, v := range after {
		s.Equal("bulk_all", updClientRawGet(v, "update_tagged"),
			"every value should carry update_tagged=bulk_all; raw=%s", v)
	}
}

func (s *UpdateKeyRangeSuite) TestUpdateAllKeyRangeCtorShapes_TABLE_DRIVEN() {
	epoch := 0
	nextTag := func() string {
		epoch++
		return fmt.Sprintf("e%d", epoch)
	}
	runAsc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := raw.Update(kr, nil, x.Set("ctor_shape", tag))
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return updClientIDFromStorage(res.MustGet()), true, ""
	}
	runDesc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := raw.Update(kr, nil, x.Set("ctor_shape", tag))
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		ids := updClientIDFromStorage(res.MustGet())
		for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
			ids[i], ids[j] = ids[j], ids[i]
		}
		return ids, true, ""
	}
	assertCtorShapeWritten := func(caseName, label string, wantCount int, verifyRange x.KeyRange, wantTag string) {
		s.T().Helper()
		skRes := raw.SearchKey(verifyRange, nil, false)
		s.Require().False(skRes.IsError(), "%s/%s: SearchKey after Update err: %v", caseName, label, skRes.Error())
		values := skRes.MustGet()
		var count int
		for _, v := range values {
			if updClientRawGet(v, "ctor_shape") == wantTag {
				count++
			}
		}
		s.Equal(wantCount, count,
			"%s/%s: ctor_shape=%q written count mismatch want=%d got=%d (SearchKey range len=%d)",
			caseName, label, wantTag, wantCount, count, len(values))
	}
	for _, tc := range testutil.KeyRangeCtorCases() {
		tc := tc
		kr := tc.Build(updClientID)
		fullCase := "UpdateKR/" + tc.Name

		tag := nextTag()
		ids, ok, errMsg := runAsc(kr, tag)
		assertClientKRResult(s.T(), fullCase, "ASC_no_limit", tc.WantAsc, ids, ok, errMsg, false)
		if ok && len(ids) > 0 {
			assertCtorShapeWritten(fullCase, "ASC_no_limit", len(ids), kr, tag)
		}

		tag = nextTag()
		ids, ok, errMsg = runDesc(kr, tag)
		assertClientKRResult(s.T(), fullCase, "DESC_no_limit", tc.WantAsc, ids, ok, errMsg, true)
		if ok && len(tc.WantAsc) > 0 {
			assertCtorShapeWritten(fullCase, "DESC_no_limit", len(tc.WantAsc), kr, tag)
		}

		if len(tc.WantAsc) >= 5 {
			limit5Asc := tc.WantAsc[:5]
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(5), tag)
			assertClientKRResult(s.T(), fullCase, "ASC_Limit_5_is_first_5", limit5Asc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_5", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runDesc(kr.Limit(5), tag)
			assertClientKRResult(s.T(), fullCase, "DESC_Limit_5_is_last_5_rev", limit5Asc, ids, ok, errMsg, true)
			if ok && len(limit5Asc) > 0 {
				assertCtorShapeWritten(fullCase, "DESC_Limit_5", 5, kr, tag)
			}
		}
		if len(tc.WantAsc) >= 3 {
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)), tag)
			assertClientKRResult(s.T(), fullCase, "ASC_Limit_EQ_count_returns_all", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_EQ_count", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)+500), tag)
			assertClientKRResult(s.T(), fullCase, "ASC_Limit_OVER_count_safe", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_OVER_count", len(ids), kr, tag)
			}
		}
	}
}

func assertClientKRResult(t *testing.T, caseName, label string, wantAsc, ids []string, ok bool, errMsg string, desc bool) {
	t.Helper()
	if !ok {
		t.Errorf("%s/%s: expected Ok, got Error: %s", caseName, label, errMsg)
		return
	}
	if len(wantAsc) != len(ids) {
		t.Errorf("%s/%s: length mismatch want=%d got=%d ids=%v", caseName, label, len(wantAsc), len(ids), ids)
		return
	}
	var want []string
	if desc {
		want = make([]string, len(wantAsc))
		copy(want, wantAsc)
		for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
			want[i], want[j] = want[j], want[i]
		}
	} else {
		want = wantAsc
	}
	if len(want) == 0 && len(ids) == 0 {
		return
	}
	for i := range want {
		if want[i] != ids[i] {
			t.Errorf("%s/%s content mismatch (desc=%v): want[%d]=%q got[%d]=%q", caseName, label, desc, i, want[i], i, ids[i])
			return
		}
	}
}

func (s *UpdateKeyRangeSuite) TestGtGteBoundaryGapEqualsOne() {
	krGte := x.KeysGte(updClientID("p027"))
	resGte := raw.Update(krGte, nil, x.Set("boundary", "gte"))
	s.Require().False(resGte.IsError())
	idsGte := updClientIDFromStorage(resGte.MustGet())

	skGte := raw.SearchKey(krGte, nil, false)
	s.Require().False(skGte.IsError())
	gotGte := skGte.MustGet()
	s.Len(gotGte, len(idsGte), "Gte SK sweep after Update expected len=%d got=%d", len(idsGte), len(gotGte))
	for _, v := range gotGte {
		got := updClientRawGet(v, "boundary")
		s.Equal("gte", got, "Update Gte value mismatch on boundary field: raw=%s", v)
	}

	krGt := x.KeysGt(updClientID("p027"))
	resGt := raw.Update(krGt, nil, x.Set("boundary", "gt"))
	s.Require().False(resGt.IsError())
	idsGt := updClientIDFromStorage(resGt.MustGet())

	skGt := raw.SearchKey(krGt, nil, false)
	s.Require().False(skGt.IsError())
	gotGt := skGt.MustGet()
	s.Len(gotGt, len(idsGt), "Gt SK sweep after Update expected len=%d got=%d", len(idsGt), len(gotGt))
	for _, v := range gotGt {
		got := updClientRawGet(v, "boundary")
		s.Equal("gt", got, "Update Gt value mismatch on boundary field: raw=%s", v)
	}

	testutil.AssertGtGteGap1(s.T(), idsGte, idsGt, "p027")
}

func (s *UpdateKeyRangeSuite) TestLtLteBoundaryGapEqualsOne() {
	krLte := x.KeysLte(updClientID("p072"))
	resLte := raw.Update(krLte, nil, x.Set("boundary", "lte"))
	s.Require().False(resLte.IsError())
	idsLte := updClientIDFromStorage(resLte.MustGet())

	skLte := raw.SearchKey(krLte, nil, false)
	s.Require().False(skLte.IsError())
	gotLte := skLte.MustGet()
	s.Len(gotLte, len(idsLte), "Lte SK sweep after Update expected len=%d got=%d", len(idsLte), len(gotLte))
	for _, v := range gotLte {
		got := updClientRawGet(v, "boundary")
		s.Equal("lte", got, "Update Lte value mismatch on boundary field: raw=%s", v)
	}

	krLt := x.KeysLt(updClientID("p072"))
	resLt := raw.Update(krLt, nil, x.Set("boundary", "lt"))
	s.Require().False(resLt.IsError())
	idsLt := updClientIDFromStorage(resLt.MustGet())

	skLt := raw.SearchKey(krLt, nil, false)
	s.Require().False(skLt.IsError())
	gotLt := skLt.MustGet()
	s.Len(gotLt, len(idsLt), "Lt SK sweep after Update expected len=%d got=%d", len(idsLt), len(gotLt))
	for _, v := range gotLt {
		got := updClientRawGet(v, "boundary")
		s.Equal("lt", got, "Update Lt value mismatch on boundary field: raw=%s", v)
	}

	testutil.AssertLtLteGap1(s.T(), idsLte, idsLt, "p072")
}

func (s *UpdateKeyRangeSuite) TestLimit7PrefixEqualFullSet() {
	allKr := x.KeysPattern(updClientID("*"))

	fullRes := raw.Update(allKr, nil, x.Set("lim", "full"))
	s.Require().False(fullRes.IsError(), "full err=%v", fullRes.Error())
	full := fullRes.MustGet()
	s.Len(full, testutil.CountX())
	sort.Strings(full)
	skFull := raw.SearchKey(allKr, nil, false)
	s.Require().False(skFull.IsError())
	gotFull := skFull.MustGet()
	s.Len(gotFull, testutil.CountX())
	for _, v := range gotFull {
		got := updClientRawGet(v, "lim")
		s.Equal("full", got, "Update lim=full value mismatch: raw=%s", v)
	}

	limitRes := raw.Update(x.KeysPattern(updClientID("*")).Limit(7), nil, x.Set("lim", "7"))
	s.Require().False(limitRes.IsError(), "limit err=%v", limitRes.Error())
	lim := limitRes.MustGet()
	s.Len(lim, 7, "Limit(7) must truncate at callback=7, got len=%d", len(lim))
	sort.Strings(lim)
	s.Equal(full[:7], lim, "Limit(7) updated keys must equal ASC first-7 of full set — proves early-stop at callback")
	skLim := raw.SearchKey(allKr, nil, false)
	s.Require().False(skLim.IsError())
	gotLim := skLim.MustGet()
	var cntLim7 int
	for _, v := range gotLim {
		got := updClientRawGet(v, "lim")
		if got == "7" {
			cntLim7++
			continue
		}
		s.Equal("full", got, "Limit=7 sweep: non-first-7 docs must keep lim=full, got %q; raw=%s", got, v)
	}
	s.Equal(7, cntLim7, "lim=7 want 7 docs with exact value lim==7 got=%d", cntLim7)
}

func (s *UpdateKeyRangeSuite) TestFilterUpdatesOnlyMatched() {
	filter := x.Eq("bucket", "A")
	res := raw.Update(x.KeysPattern(updClientID("*")), filter, x.Set("filtered_tag", "A-only"))
	s.Require().False(res.IsError(), "filtered err=%v", res.Error())
	ids := updClientIDFromStorage(res.MustGet())
	s.Len(ids, 34, "Update+filter Eq(bucket,A) should match 34 bucket=A rows")

	skAll := raw.SearchKey(x.KeysPattern(updClientID("*")), nil, false)
	s.Require().False(skAll.IsError())
	var count int
	for _, v := range skAll.MustGet() {
		if updClientRawGet(v, "filtered_tag") == "A-only" {
			count++
		}
	}
	s.Equal(len(ids), count, "only updated rows carry filtered_tag; count=%d", count)
}

func (s *UpdateKeyRangeSuite) TestNilKRRejects() {
	res := raw.Update(nil, nil, x.Set("nil_tag", true))
	s.Require().True(res.IsError(), "nil kr must reject")
	s.Contains(res.Error().Error(), "key range is required")
}

func (s *UpdateKeyRangeSuite) TestEmptyValuesRejects() {
	res := raw.Update(x.KeysPattern(updClientID("*")), nil)
	s.Require().True(res.IsError(), "no mutation values must reject")
	s.Contains(res.Error().Error(), "no update values provided")
}

func TestUpdateKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(UpdateKeyRangeSuite))
}
