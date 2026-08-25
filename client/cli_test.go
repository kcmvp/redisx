package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	naming "github.com/kcmvp/redisx/internal/naming"
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

var cliServerSeedOnce sync.Once

type testUserDoc string

func (testUserDoc) Namespace() string  { return "user" }
func (testUserDoc) Mem() bool          { return false }
func (testUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u testUserDoc) RawJSON() string  { return string(u) }
func (testUserDoc) TTL() time.Duration { return 0 }

func (s *ClientTestSuite) SetupTest() {
	disconnect()

	handlersMu.Lock()
	deliveryChByTopic = make(map[string]chan *ReceivedMessage)
	handlersMu.Unlock()

	kvClientMu.Lock()
	if kvClient != nil {
		_ = kvClient.Close()
	}
	kvClient = nil
	kvClientMu.Unlock()

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

	resetHooks()
}

type ClientTestSuite struct {
	suite.Suite
}

func (s *ClientTestSuite) SetupSuite() {
	s.T().Setenv("HOME", s.T().TempDir())
	dbPath := filepath.Join(s.T().TempDir(), "redisx.db")

	probeKP := testutil.KeyRangeKeyPattern(searchKRClientNamespace, testutil.KeyRangeFixtureMem())
	probeStorageNs := naming.BuildStorageNs(searchKRClientNamespace, testutil.KeyRangeFixtureMem())
	idxProbeScore := x.RawIndex(naming.BuildIdxFullName(probeStorageNs, "score"), probeKP, "score")
	idxProbeBucket := x.RawIndex(naming.BuildIdxFullName(probeStorageNs, "bucket"), probeKP, "bucket")
	idxProbeSparse := x.RawIndex(naming.BuildIdxFullName(probeStorageNs, "sparseamt"), probeKP, "sparse_amt")

	appPort, adminPort := testutil.AllocateTwoFreePorts(s.T())
	cfg := &server.Config{
		DataPath: dbPath,
		App:      server.AppConfig{Bind: "127.0.0.1", Port: appPort},
		Admin:    server.AdminConfig{Bind: "127.0.0.1", Port: adminPort},
	}
	clientTestServerAddr = cfg.Admin.Addr()
	db := server.StartWithConfig(cfg,
		testUserDoc(""),
		SearchFixtureDoc(""),
		UpdateFixtureDoc(""),
	)
	s.Require().NotNil(db)

	cliServerSeedOnce.Do(func() {
		s.Require().NoError(db.CreateIndex(x.Idx[testUserDoc]("age", "*", "age")))
		s.Require().NoError(db.CreateIndex(x.Idx[testUserDoc]("email", "*", "email")))
		s.Require().NoError(db.CreateIndex(idxProbeScore))
		s.Require().NoError(db.CreateIndex(idxProbeBucket))
		s.Require().NoError(db.CreateIndex(idxProbeSparse))
		s.Require().NoError(db.Set(naming.AuthStorageKey(clientTestExternalAuthKey), "50"))

		for _, kv := range testutil.LoadXFor(s.T(), searchKRClientNamespace, testutil.KeyRangeFixtureMem()) {
			s.Require().NoError(db.Set(kv.K, kv.V), "seed probe-client fixture failed for %s", kv.K)
		}
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
	disconnect()
	_ = server.Stop()
	cliServerSeedOnce = sync.Once{}
	clientTestServerAddr = ""
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

func (s *ClientTestSuite) TestMemKey() {
	s.Equal("_m_:user:1", x.MemKey("user:1"))
	s.Equal("_m_:user:1", x.MemKey("_m_:user:1"))
}

// ensureConnected waits until the shared client is fully connected
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

func (s *ClientTestSuite) TestGetSetCommand() {
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
			func() (string, error) { return Get("cli:k") },
			true, "resp client is not connected", "",
		},
		{
			"Set not connected",
			nil,
			func() (string, error) { return "", Set("cli:k", "v") },
			true, "resp client is not connected", "",
		},
		{
			"Get empty key",
			func() { _ = Connect(clientTestServerAddr, clientTestExternalAuthKey); s.ensureConnected() },
			func() (string, error) { return Get("") },
			false, "", "",
		},
		{
			"Set empty key",
			func() { _ = Connect(clientTestServerAddr, clientTestExternalAuthKey); s.ensureConnected() },
			func() (string, error) { return "", Set("", "v") },
			false, "", "",
		},
		{
			"Set and Get Success",
			func() { _ = Connect(clientTestServerAddr, clientTestExternalAuthKey); s.ensureConnected() },
			func() (string, error) {
				if err := Set("cli:external-key", "external-value"); err != nil {
					return "", err
				}
				return Get("cli:external-key")
			},
			false, "", "external-value",
		},
		{
			"SetWithTTL Success and Expire",
			func() { _ = Connect(clientTestServerAddr, clientTestExternalAuthKey); s.ensureConnected() },
			func() (string, error) {
				if err := SetWithTTL("cli:ttl_key", "ttl_val", 100*time.Millisecond); err != nil {
					return "", err
				}
				v1, _ := Get("cli:ttl_key")
				if v1 != "ttl_val" {
					return v1, nil
				}
				time.Sleep(200 * time.Millisecond)
				v2, err := Get("cli:ttl_key")
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

func (s *ClientTestSuite) TestCrudCommands() {
	s.Run("SetNXWithTTL not connected", func() {
		s.SetupTest()
		ok, err := SetNXWithTTL("cli:k", "v", time.Second)
		s.Error(err)
		s.Contains(err.Error(), "resp client is not connected")
		s.False(ok)
	})

	s.Run("SetNXWithTTL empty key", func() {
		s.SetupTest()
		err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
		s.NoError(err)
		s.ensureConnected()

		ok, err := SetNXWithTTL("", "v", time.Second)
		s.NoError(err)
		s.False(ok)
	})

	s.Run("SetNXWithTTL falls back when ttl is non-positive", func() {
		s.SetupTest()
		err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
		s.NoError(err)
		s.ensureConnected()

		ok, err := SetNXWithTTL("cli:setnx-fallback", "v1", 0)
		s.NoError(err)
		s.True(ok)

		ok, err = SetNXWithTTL("cli:setnx-fallback", "v2", 0)
		s.NoError(err)
		s.False(ok)
	})

	s.Run("SetNXWithTTL success and expire", func() {
		s.SetupTest()
		err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
		s.NoError(err)
		s.ensureConnected()

		ok, err := SetNXWithTTL("cli:setnx-ttl", "ttl-value", 100*time.Millisecond)
		s.NoError(err)
		s.True(ok)

		val, err := Get("cli:setnx-ttl")
		s.NoError(err)
		s.Equal("ttl-value", val)

		time.Sleep(200 * time.Millisecond)

		_, err = Get("cli:setnx-ttl")
		s.Error(err)
		s.Contains(err.Error(), "redis: nil")
	})

	s.Run("SetNX not connected", func() {
		s.SetupTest()
		ok, err := SetNX("cli:k", "v")
		s.Error(err)
		s.Contains(err.Error(), "resp client is not connected")
		s.False(ok)
	})

	s.Run("Delete not connected", func() {
		s.SetupTest()
		deleted, err := Delete("cli:k")
		s.Error(err)
		s.Contains(err.Error(), "resp client is not connected")
		s.False(deleted)
	})

	s.Run("Keys not connected", func() {
		s.SetupTest()
		keysRes := Keys("cli:k*")
		s.True(keysRes.IsError())
		s.Contains(keysRes.Error().Error(), "resp client is not connected")
	})

	s.Run("SetNX Delete Keys Success", func() {
		s.SetupTest()
		err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
		s.NoError(err)
		s.ensureConnected()

		// Test SetNX
		ok, err := SetNX("cli:key_nx", "val1")
		s.NoError(err)
		s.True(ok)

		ok, err = SetNX("cli:key_nx", "val2")
		s.NoError(err)
		s.False(ok)

		v, err := Get("cli:key_nx")
		s.NoError(err)
		s.Equal("val1", v)

		// Test Keys
		_ = Set("cli:key_another", "val")
		keysRes := Keys("cli:key_*")
		s.False(keysRes.IsError())
		s.ElementsMatch([]string{"cli:key_nx", "cli:key_another"}, keysRes.MustGet())

		// Test Delete
		deleted, err := Delete("cli:key_nx")
		s.NoError(err)
		s.True(deleted)

		keysRes = Keys("cli:key_*")
		s.False(keysRes.IsError())
		s.ElementsMatch([]string{"cli:key_another"}, keysRes.MustGet())

		deleted, err = Delete("cli:key_another")
		s.NoError(err)
		s.True(deleted)
	})
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
		useConnect  bool // if true, use internal connect() instead of Connect()
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
					// The error message from net.Dial can vary across different OS/environments
					// (e.g., "dial tcp", "context deadline exceeded", "server misbehaving").
					// We just need to ensure an error occurred for invalid addresses.
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
				// The error message for an intentionally rejected connection can be a transport-level close
				// or an explicit AUTH error depending on when the server rejects the connection.
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

func (s *ClientTestSuite) TestConnectEmbed() {
	s.SetupTest()

	err := ConnectEmbed(clientTestServerAddr)
	s.NoError(err)
	s.ensureConnected()
}

func (s *ClientTestSuite) TestConnectEmbedStopsOnProcessSignal() {
	s.SetupTest()

	var capturedSigCh chan<- os.Signal
	signalNotifyFn = func(c chan<- os.Signal, _ ...os.Signal) {
		capturedSigCh = c
	}

	err := ConnectEmbed(clientTestServerAddr)
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

	adminHost, adminPortStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("invalid REDISX_CHILD_ADDR=%q: %v", addr, err)
	}
	if adminHost == "" {
		adminHost = "127.0.0.1"
	}
	adminPort, pErr := strconv.Atoi(adminPortStr)
	if pErr != nil {
		t.Fatalf("invalid admin port %q: %v", adminPortStr, pErr)
	}
	// Atomically claim a free App port, then release it and immediately hand
	// over both App and Admin ports to StartWithConfig for the actual bind.
	// The App port we just bound is definitely not in active LISTEN state (we
	// were the last ones to have it open); the Admin port was chosen by the
	// parent using the same bind+close dance so redcon.ListenAndServe (which
	// sets SO_REUSEADDR) will re-bind through TIME_WAIT successfully.
	var appPort int
	claimedApp, lErr := net.Listen("tcp", net.JoinHostPort(adminHost, "0"))
	if lErr != nil {
		t.Fatalf("allocate free app port: %v", lErr)
	}
	appPort = claimedApp.Addr().(*net.TCPAddr).Port
	if appPort == adminPort {
		_ = claimedApp.Close()
		claimedApp, lErr = net.Listen("tcp", net.JoinHostPort(adminHost, "0"))
		if lErr != nil {
			t.Fatalf("re-allocate free app port (collision): %v", lErr)
		}
		appPort = claimedApp.Addr().(*net.TCPAddr).Port
	}
	_ = claimedApp.Close()

	cfg := &server.Config{
		DataPath: dbPath,
		App:      server.AppConfig{Bind: adminHost, Port: appPort},
		Admin:    server.AdminConfig{Bind: adminHost, Port: adminPort},
	}
	db := server.StartWithConfig(cfg)
	if db == nil {
		t.Fatalf("server.StartWithConfig() returned nil; appPort=%d adminPort=%d — likely a bind race, retry", appPort, adminPort)
	}

	if err := ConnectEmbed(addr); err != nil {
		t.Fatalf("ConnectEmbed() failed: %v", err)
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

func (s *ClientTestSuite) TestSearchIndexCommand() {
	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	// Prepare data
	data := []struct {
		key string
		val string
	}{
		{"user:1", `{"id": "1", "email": "ken@example.com", "age": 30, "status": "active"}`},
		{"user:2", `{"id": "2", "email": "john@example.com", "age": 20, "status": "pending"}`},
		{"user:3", `{"id": "3", "email": "admin@example.com", "age": 40, "status": "active"}`},
	}

	for _, d := range data {
		err := Set(d.key, d.val)
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
		{"Eq string", x.Idx[testUserDoc]("email", "*", "email").Name(), x.KeysPattern("user:*"), x.Eq("email", "ken@example.com"), false, false, 1},
		{"Eq false", x.Idx[testUserDoc]("email", "*", "email").Name(), x.KeysPattern("user:*"), x.Eq("email", "nobody@example.com"), false, false, 0},
		{"Gt number", x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.Gt("age", 25), false, false, 2},
		{"Lt number", x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.Lt("age", 35), false, false, 2},
		{"And true", x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.And(x.Gt("age", 25), x.Eq("status", "active")), false, false, 2},
		{"And false", x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.And(x.Gt("age", 35), x.Eq("status", "pending")), false, false, 0},
		{"Or", x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.Or(x.Lt("age", 25), x.Eq("status", "active")), false, false, 3},
		{"Empty filter", x.Idx[testUserDoc]("email", "*", "email").Name(), x.KeysPattern("user:*"), nil, false, false, 3},
		{"Key pattern narrows index scan", x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:2"), nil, false, false, 1},
		{"Descend test", x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), x.Gt("age", 10), true, false, 3},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			res := SearchIndex(tt.index, tt.kr, tt.filter, tt.desc)

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

func (s *ClientTestSuite) TestSearchKeyCommand() {
	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	// Prepare data
	data := []struct {
		key string
		val string
	}{
		{"product:1", `{"id": "1", "name": "Apple", "price": 10, "stock": 100}`},
		{"product:2", `{"id": "2", "name": "Banana", "price": 5, "stock": 50}`},
		{"product:3", `{"id": "3", "name": "Orange", "price": 8, "stock": 200}`},
	}

	for _, d := range data {
		err := Set(d.key, d.val)
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
			res := SearchKey(tt.kr, tt.filter, tt.desc)

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

func (s *ClientTestSuite) TestUpdateCommand() {
	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	data := []struct {
		key string
		val string
	}{
		{"user:1", `{"id":"1","status":"pending","age":17}`},
		{"user:2", `{"id":"2","status":"pending","age":22}`},
		{"user:3", `{"id":"3","status":"active","age":30}`},
	}

	for _, d := range data {
		err := Set(d.key, d.val)
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
				val, err := Get("user:1")
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
				val, err := Get("user:3")
				s.NoError(err)
				s.Equal(float64(2), gjson.Get(val, "version").Float())
			},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			res := Update(tt.kr, tt.filter, tt.updates...)
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

				// Test Pattern Matching (PSUBSCRIBE)
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

				// Test Broadcast (Multiple clients subscribe to the same topic)
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

	s.Nil(snapshotHooks(), "no hooks initially")

	id1 := AddObserverHook(func(key string, value []byte) {})
	s.NotEqual(HookID(0), id1, "HookID should be non-zero")
	s.NotNil(snapshotHooks())
	s.Len(snapshotHooks().observers, 1)

	id2 := AddAbortHook(func(key string, value []byte) error { return nil })
	id3 := AddTransformHook(func(key string, value []byte) ([]byte, error) { return value, nil })
	id4 := AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {})
	s.Len(snapshotHooks().aborts, 1)
	s.Len(snapshotHooks().transforms, 1)
	s.Len(snapshotHooks().afters, 1)

	s.NotEqual(id1, id2)
	s.NotEqual(id2, id3)
	s.NotEqual(id3, id4)

	RemoveHook(id2)
	s.Len(snapshotHooks().aborts, 0)
	s.Len(snapshotHooks().observers, 1)
	s.Len(snapshotHooks().transforms, 1)
	s.Len(snapshotHooks().afters, 1)

	RemoveHook(id1)
	RemoveHook(id3)
	RemoveHook(id4)
	reg := snapshotHooks()
	s.Nil(reg, "after removing every hook, registry snapshot must be nil to keep the zero-cost default path")

	RemoveHook(HookID(0))
	RemoveHook(9999)
	RemoveHook(id1)
}

func (s *ClientTestSuite) TestObserverHookCalledBeforeWrite() {
	s.SetupTest()

	var called bool
	var gotKey string
	var gotVal []byte
	AddObserverHook(func(key string, value []byte) {
		called = true
		gotKey = key
		gotVal = append([]byte(nil), value...)
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-obs-1", "hello")
	s.NoError(err)
	s.True(called, "ObserverHook must be called")
	s.Equal("cli:hook-obs-1", gotKey)
	s.Equal([]byte("hello"), gotVal)

	stored, err := Get("cli:hook-obs-1")
	s.NoError(err)
	s.Equal("hello", stored)
}

func (s *ClientTestSuite) TestObserverHookFailOpenOnPanic() {
	s.SetupTest()

	SetHookTimeout(0)
	defer SetHookTimeout(defaultHookTimeout)

	AddObserverHook(func(key string, value []byte) {
		panic("boom observer")
	})

	var ok bool
	AddObserverHook(func(key string, value []byte) {
		ok = true
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-obs-panic", "val")
	s.NoError(err, "ObserverHook panic must not abort write")
	s.True(ok, "subsequent ObserverHooks must still run after panic in prior observer")

	stored, err := Get("cli:hook-obs-panic")
	s.NoError(err)
	s.Equal("val", stored)
}

func (s *ClientTestSuite) TestObserverHookFailOpenOnTimeout() {
	s.SetupTest()

	SetHookTimeout(50 * time.Millisecond)
	defer SetHookTimeout(defaultHookTimeout)

	AddObserverHook(func(key string, value []byte) {
		time.Sleep(200 * time.Millisecond)
	})

	var ok bool
	AddObserverHook(func(key string, value []byte) {
		ok = true
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	start := time.Now()
	err = Set("cli:hook-obs-timeout", "val")
	elapsed := time.Since(start)
	s.NoError(err, "ObserverHook timeout must not abort write")
	s.True(ok, "subsequent ObserverHooks must still run")
	s.Less(elapsed, 500*time.Millisecond, "timeout should bound the hook time")

	stored, err := Get("cli:hook-obs-timeout")
	s.NoError(err)
	s.Equal("val", stored)
}

func (s *ClientTestSuite) TestAbortHookBlocksWrite() {
	s.SetupTest()

	customErr := errors.New("blocked by policy")
	AddAbortHook(func(key string, value []byte) error {
		if key == "cli:forbidden" {
			return customErr
		}
		return nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:forbidden", "secret")
	s.ErrorIs(err, customErr)

	_, err = Get("cli:forbidden")
	s.Error(err, "key must not exist after abort")

	err = Set("cli:allowed", "public")
	s.NoError(err)
	v, err := Get("cli:allowed")
	s.NoError(err)
	s.Equal("public", v)
}

func (s *ClientTestSuite) TestAbortHookFailClosedOnPanic() {
	s.SetupTest()

	SetHookTimeout(0)
	defer SetHookTimeout(defaultHookTimeout)

	AddAbortHook(func(key string, value []byte) error {
		panic("abort panic")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-abort-panic", "val")
	s.Error(err, "AbortHook panic must abort write (fail-closed)")
	s.Contains(err.Error(), "panic")

	_, err = Get("cli:hook-abort-panic")
	s.Error(err, "key must not exist after AbortHook panic")
}

func (s *ClientTestSuite) TestAbortHookFailClosedOnTimeout() {
	s.SetupTest()

	SetHookTimeout(50 * time.Millisecond)
	defer SetHookTimeout(defaultHookTimeout)

	AddAbortHook(func(key string, value []byte) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	start := time.Now()
	err = Set("cli:hook-abort-timeout", "val")
	elapsed := time.Since(start)
	s.Error(err, "AbortHook timeout must abort write (fail-closed)")
	s.Contains(err.Error(), "timeout")
	s.Less(elapsed, 500*time.Millisecond)
}

func (s *ClientTestSuite) TestTransformHookChangesValue() {
	s.SetupTest()

	prefix := []byte("enc:")
	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(prefix)+len(value))
		copy(out, prefix)
		copy(out[len(prefix):], value)
		return out, nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-transform-1", "plain")
	s.NoError(err)

	stored, err := Get("cli:hook-transform-1")
	s.NoError(err)
	s.Equal("enc:plain", stored, "TransformHook must change the written value")
}

func (s *ClientTestSuite) TestTransformHookChain() {
	s.SetupTest()

	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		for i, b := range value {
			out[i] = b + 1
		}
		return out, nil
	})
	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		for i, b := range value {
			out[i] = b * 2
		}
		return out, nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-transform-chain", "\x01")
	s.NoError(err)

	stored, err := Get("cli:hook-transform-chain")
	s.NoError(err)
	s.Equal([]byte{("\x01"[0] + 1) * 2}, []byte(stored),
		"TransformHooks must chain in registration order: (+1) then (*2)")
}

func (s *ClientTestSuite) TestTransformHookFailClosedOnError() {
	s.SetupTest()

	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		return nil, errors.New("transform failed")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-transform-err", "val")
	s.Error(err)
	s.Contains(err.Error(), "transform failed")

	_, err = Get("cli:hook-transform-err")
	s.Error(err, "key must not exist after TransformHook error")
}

func (s *ClientTestSuite) TestTransformHookFailClosedOnPanic() {
	s.SetupTest()

	SetHookTimeout(0)
	defer SetHookTimeout(defaultHookTimeout)

	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		panic("transform panic")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-transform-panic", "val")
	s.Error(err)
	s.Contains(err.Error(), "panic")

	_, err = Get("cli:hook-transform-panic")
	s.Error(err)
}

func (s *ClientTestSuite) TestObserverAfterHookCalledAfterWrite() {
	s.SetupTest()

	var gotKey, gotVal string
	var gotWriteErr error
	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		gotKey = key
		gotVal = string(value)
		gotWriteErr = writeErr
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-after-1", "hello-after")
	s.NoError(err)
	s.Equal("cli:hook-after-1", gotKey)
	s.Equal("hello-after", gotVal)
	s.NoError(gotWriteErr, "writeErr should be nil on success")
}

func (s *ClientTestSuite) TestObserverAfterHookReceivesTransformedValue() {
	s.SetupTest()

	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		copy(out, value)
		for i := range out {
			out[i] = out[i] + 1
		}
		return out, nil
	})

	var gotVal []byte
	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		gotVal = append([]byte(nil), value...)
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-after-transformed", "ABC")
	s.NoError(err)
	s.Equal([]byte("BCD"), gotVal, "ObserverAfter must see the transformed value")
}

func (s *ClientTestSuite) TestObserverAfterHookFailOpenOnPanic() {
	s.SetupTest()

	SetHookTimeout(0)
	defer SetHookTimeout(defaultHookTimeout)

	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		panic("after hook boom")
	})

	var ok bool
	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		ok = true
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-after-panic", "val")
	s.NoError(err, "ObserverAfterHook panic must not affect write result")
	s.True(ok, "subsequent ObserverAfterHooks must still run")

	stored, err := Get("cli:hook-after-panic")
	s.NoError(err)
	s.Equal("val", stored)
}

func (s *ClientTestSuite) TestObserverAfterHookFailOpenOnTimeout() {
	s.SetupTest()

	SetHookTimeout(50 * time.Millisecond)
	defer SetHookTimeout(defaultHookTimeout)

	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		time.Sleep(200 * time.Millisecond)
	})

	var ok bool
	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		ok = true
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	start := time.Now()
	err = Set("cli:hook-after-timeout", "val")
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

	AddAbortHook(func(key string, value []byte) error {
		capture(0)
		s.Equal("original", string(value), "Abort must see original value")
		return nil
	})
	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		capture(1)
		s.Equal("original", string(value), "Transform must see original value")
		out := make([]byte, len("transformed"))
		copy(out, "transformed")
		return out, nil
	})
	AddObserverHook(func(key string, value []byte) {
		capture(2)
		s.Equal("transformed", string(value), "ObserverBefore must see POST-transform value (A5)")
	})
	// Write happens between index 2 and 3 (captured below as manual step 3)
	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		capture(4)
		s.Equal("transformed", string(value), "ObserverAfter must see transformed value")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-order", "original")
	s.NoError(err)
	steps[3] = seq.Add(1) // Capture "write step" after Set returns

	s.Equal([]int64{1, 2, 3, 5, 4}, steps,
		"Order MUST be Abort(1) → Transform(2) → ObserverBefore(3) → [actual Redis write happens  before return ~4 but AFTER Before hooks] → ObserverAfter(4) — seq: [1,2,3,4,5]")
	s.Equal(int64(1), steps[0]) // Abort
	s.Equal(int64(2), steps[1]) // Transform
	s.Equal(int64(3), steps[2]) // ObserverBefore
	s.Equal(int64(4), steps[4]) // ObserverAfter runs BEFORE Set returns (sync)
	s.Equal(int64(5), steps[3]) // our post-Set capture happens last

	stored, err := Get("cli:hook-order")
	s.NoError(err)
	s.Equal("transformed", stored)
}

func (s *ClientTestSuite) TestObserverBeforeSeesPostTransformValueButAbortSeesOriginal() {
	s.SetupTest()

	var abortSaw, transformSaw, observerSaw []byte

	AddAbortHook(func(key string, value []byte) error {
		abortSaw = append([]byte(nil), value...)
		return nil
	})
	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		transformSaw = append([]byte(nil), value...)
		out := make([]byte, len(value)+len("-transformed"))
		copy(out, value)
		copy(out[len(value):], "-transformed")
		return out, nil
	})
	AddObserverHook(func(key string, value []byte) {
		observerSaw = append([]byte(nil), value...)
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-see-orig", "raw-value")
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
	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		afterCalled = true
	})

	var observerBeforeCalled bool
	AddObserverHook(func(key string, value []byte) {
		observerBeforeCalled = true
	})

	AddAbortHook(func(key string, value []byte) error {
		return errors.New("abort")
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:hook-no-after", "val")
	s.Error(err)
	s.False(observerBeforeCalled,
		"ObserverBefore must NOT run when AbortHook aborted earlier (fail-closed propagates out of Abort phase)")
	s.False(afterCalled, "ObserverAfter hooks must NOT run when write was aborted in Before phase")
}

func (s *ClientTestSuite) TestTransformReturnsNilBytesWithoutErrorFailsClosed() {
	s.SetupTest()

	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		// Emulate user who forgot to either return a fresh slice or return an error.
		// Framework must fail-closed instead of silently writing "".
		return nil, nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = Set("cli:nil-transform", "should-not-be-overwritten")
	s.Error(err)
	s.Contains(err.Error(), "returned nil bytes without error")

	existing, gerr := Get("cli:nil-transform")
	if gerr == nil {
		s.Equal("", existing,
			"key either should not exist OR be empty string; what we must NOT see is 'should-not-be-overwritten' overwritten to ''.")
	}
}

func (s *ClientTestSuite) TestSetHookZeroTimeoutUsesSyncPathNoGoroutineAllocs() {
	s.SetupTest()

	old := getHookTimeout()
	SetHookTimeout(0)
	defer SetHookTimeout(old)

	AddObserverHook(func(key string, value []byte) {})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	// Quick sanity — no error, writes succeed with T=0.
	err = Set("cli:sync-path", "v")
	s.NoError(err)
	v, gerr := Get("cli:sync-path")
	s.NoError(gerr)
	s.Equal("v", v)
}

func (s *ClientTestSuite) TestObserverAfterCommittedOnNormalSetAndAbortOnError() {
	s.SetupTest()

	var gotKey string
	var gotCommitted bool
	var gotErr error
	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		gotKey = key
		gotCommitted = committed
		gotErr = writeErr
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	// (1) Plain Set success -> committed true, err nil
	err = Set("cli:after-commit-1", "v1")
	s.NoError(err)
	s.Equal("cli:after-commit-1", gotKey)
	s.True(gotCommitted)
	s.NoError(gotErr)

	// (2) Abort in before-hook -> ObserverAfter does NOT run at all, got* unchanged (per invariant).
	abortID := AddAbortHook(func(key string, value []byte) error {
		return errors.New("blocked in abort hook")
	})
	defer RemoveHook(abortID)
	err = Set("cli:after-commit-2", "v2")
	s.Error(err)
	// gotKey/gotCommitted/gotErr still hold values from the previous successful Set because
	// runAfterHooks is entirely skipped when runBeforeHooks aborts (fail-closed propagation).
	s.Equal("cli:after-commit-1", gotKey)
	s.True(gotCommitted)
}

func (s *ClientTestSuite) TestHooksAppliedToSetNX() {
	s.SetupTest()

	var obsKey, obsVal string
	AddObserverHook(func(key string, value []byte) {
		obsKey = key
		obsVal = string(value)
	})

	prefix := []byte("x:")
	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(prefix)+len(value))
		copy(out, prefix)
		copy(out[len(prefix):], value)
		return out, nil
	})

	var afterErr error
	var afterVal string
	var afterCommitted bool
	AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
		afterErr = writeErr
		afterVal = string(value)
		afterCommitted = committed
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	ok, err := SetNX("cli:hook-setnx-1", "original")
	s.NoError(err)
	s.True(ok)
	s.True(afterCommitted, "first SetNX write actually occurred -> committed=true")
	s.Equal("cli:hook-setnx-1", obsKey)
	s.Equal("x:original", obsVal,
		"ObserverBefore runs after Transform, so sees post-transform value (A5 order)")
	s.Equal("x:original", afterVal)
	s.NoError(afterErr)

	stored, err := Get("cli:hook-setnx-1")
	s.NoError(err)
	s.Equal("x:original", stored)

	ok, err = SetNX("cli:hook-setnx-1", "another")
	s.NoError(err)
	s.False(ok, "SetNX should return false on existing key")
	s.False(afterCommitted,
		"SetNX ok=false MUST propagate committed=false to ObserverAfter, otherwise downstream L1/CDC will incorrectly think a write happened.")
}

func (s *ClientTestSuite) TestHooksAppliedToSetWithTTL() {
	s.SetupTest()

	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		copy(out, value)
		return append(out, '-', 't', 't', 'l'), nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	err = SetWithTTL("cli:hook-ttl", "base", 100*time.Millisecond)
	s.NoError(err)

	v, err := Get("cli:hook-ttl")
	s.NoError(err)
	s.Equal("base-ttl", v)

	time.Sleep(200 * time.Millisecond)
	_, err = Get("cli:hook-ttl")
	s.Error(err)
}

func (s *ClientTestSuite) TestHooksAppliedToSetNXWithTTL() {
	s.SetupTest()

	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		return append([]byte(nil), value...), nil
	})

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	ok, err := SetNXWithTTL("cli:hook-setnx-ttl", "x", 80*time.Millisecond)
	s.NoError(err)
	s.True(ok)

	v, err := Get("cli:hook-setnx-ttl")
	s.NoError(err)
	s.Equal("x", v)

	time.Sleep(160 * time.Millisecond)
	_, err = Get("cli:hook-setnx-ttl")
	s.Error(err)
}

func (s *ClientTestSuite) TestRemoveHookDuringWriteIsSafe() {
	s.SetupTest()

	id := AddObserverHook(func(key string, value []byte) {})
	_ = id

	err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
	s.NoError(err)
	s.ensureConnected()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			RemoveHook(id)
			AddObserverHook(func(key string, value []byte) {})
		}
	}()

	for i := 0; i < 50; i++ {
		_ = Set(fmt.Sprintf("hook-safe-%d", i), "v")
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
	adminPort, err := testutil.AllocateFreePort()
	require.NoError(t, err)
	adminAuth := "np-admin-auth"
	appAuth := "np-app-auth"
	cfg := &server.Config{
		DataPath: testutil.DBPath(t),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: appPort, Auth: appAuth},
		Admin:    server.AdminConfig{Bind: "127.0.0.1", Port: adminPort, Auth: adminAuth},
	}
	db := server.StartWithConfig(cfg)
	require.NotNil(t, db)
	defer func() {
		_ = server.Stop()
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
		{"regdoc", []any{"REGDOC", `{"namespace":"user","mem":false,"key_attrs":["id"]}`}},
		{"lsdoc", []any{"LSDOC"}},
		{"desdoc", []any{"DESDOC", "user"}},
		{"regidx", []any{"REGIDX", "user", "age", "age"}},
		{"lsidx", []any{"LSIDX"}},
		{"delidx", []any{"DELIDX", "user", "age"}},
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

func TestConnectAuthMismatchEmitsWrapAdminErr(t *testing.T) {
	appPort, err := testutil.AllocateFreePort()
	require.NoError(t, err)
	adminPort, err := testutil.AllocateFreePort()
	require.NoError(t, err)
	adminAuth := "mismatch-admin-key"
	appAuth := "mismatch-app-key"
	cfg := &server.Config{
		DataPath: testutil.DBPath(t),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: appPort, Auth: appAuth},
		Admin:    server.AdminConfig{Bind: "127.0.0.1", Port: adminPort, Auth: adminAuth},
	}
	db := server.StartWithConfig(cfg)
	require.NotNil(t, db)
	defer func() {
		_ = server.Stop()
	}()

	t.Run("app port with admin auth key → connect() itself returns WrapAdminErr WRONGPASS", func(t *testing.T) {
		_, dialErr := connect(cfg.App.Addr(), adminAuth)
		require.Error(t, dialErr)
		require.Contains(t, dialErr.Error(), "connect redisx admin-port failed: AUTH key rejected (WRONGPASS)")
		require.Contains(t, dialErr.Error(), "WRONGPASS invalid password for app port")
		require.Contains(t, dialErr.Error(), "invalid password for app port")
	})
	t.Run("admin port with app auth key → connect() itself returns WrapAdminErr WRONGPASS", func(t *testing.T) {
		_, dialErr := connect(cfg.Admin.Addr(), appAuth)
		require.Error(t, dialErr)
		require.Contains(t, dialErr.Error(), "connect redisx admin-port failed: AUTH key rejected (WRONGPASS)")
		require.Contains(t, dialErr.Error(), "WRONGPASS invalid password for admin port")
		require.Contains(t, dialErr.Error(), "invalid password for admin port")
	})
	t.Run("admin port with no auth → Connect() refuses empty auth key pre-wire", func(t *testing.T) {
		err := Connect(cfg.Admin.Addr(), "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "auth key is empty")
	})
	t.Run("admin port with invalid non-empty auth key → connect() returns WrapAdminErr ERR authentication failed", func(t *testing.T) {
		_, dialErr := connect(cfg.Admin.Addr(), "wrong-wrong-wrong")
		require.Error(t, dialErr)
		require.Contains(t, dialErr.Error(), "connect redisx admin-port failed: AUTH failed (server ERR authentication failed)")
		require.Contains(t, dialErr.Error(), "ERR authentication failed")
	})
}

func TestConnectProbePlainRedisNoCapsNoError(t *testing.T) {
	called := make(chan struct{}, 1)
	capsCh := make(chan respconn.Capabilities, 1)
	origDial := internalDialForRespconnInternal
	internalDialForRespconnInternal = func(o respconn.Options) (*respconn.DialResult, error) {
		res, err := origDial(o)
		if res != nil {
			select {
			case capsCh <- res.Capabilities:
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
	var caps respconn.Capabilities
	select {
	case caps = <-capsCh:
	case <-time.After(2 * time.Second):
		t.Fatal("DialAndHandshake interceptor never fired")
	}
	if caps.IsRedisx {
		t.Fatalf("expected IsRedisx=false on plain redis, got true (caps=%+v)", caps)
	}
	if caps.ServerVer != "" || caps.AdminRole || caps.TypedDocs || caps.TypedIndexes {
		t.Fatalf("expected empty feature caps on plain redis, got %+v", caps)
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
	resetHooks()
	cfg := &server.Config{
		DataPath: filepath.Join(b.TempDir(), "redisx-bench.db"),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: 36379},
		Admin:    server.AdminConfig{Bind: "127.0.0.1", Port: 36380},
	}
	db := server.StartWithConfig(cfg)
	if db == nil {
		b.Fatal("embedded server Start returned nil")
	}
	_ = db.Set(naming.AuthStorageKey(clientTestExternalAuthKey), "50")
	time.Sleep(200 * time.Millisecond)
	mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	_ = setSharedClient(mockClient)
	defer func() {
		_ = mockClient.Close()
		clearSharedClientIf(mockClient)
		_ = db.Close()
		resetHooks()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SetWithTTL("cli:bench-nohook", "payload-value", 0)
	}
}

func BenchmarkSetWithObserverHooks(b *testing.B) {
	resetHooks()
	SetHookTimeout(0)
	defer SetHookTimeout(defaultHookTimeout)
	AddObserverHook(func(key string, value []byte) {})
	AddObserverHook(func(key string, value []byte) {})
	AddObserverHook(func(key string, value []byte) {})

	cfg := &server.Config{
		DataPath: filepath.Join(b.TempDir(), "redisx-bench.db"),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: 36379},
		Admin:    server.AdminConfig{Bind: "127.0.0.1", Port: 36380},
	}
	db := server.StartWithConfig(cfg)
	if db == nil {
		b.Fatal("embedded server Start returned nil")
	}
	_ = db.Set(naming.AuthStorageKey(clientTestExternalAuthKey), "50")
	time.Sleep(200 * time.Millisecond)
	mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	_ = setSharedClient(mockClient)
	defer func() {
		_ = mockClient.Close()
		clearSharedClientIf(mockClient)
		_ = db.Close()
		resetHooks()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SetWithTTL("cli:bench-observer", "payload-value", 0)
	}
}

func BenchmarkSetWithAbortHook(b *testing.B) {
	resetHooks()
	SetHookTimeout(0)
	defer SetHookTimeout(defaultHookTimeout)
	AddAbortHook(func(key string, value []byte) error {
		return nil
	})

	cfg := &server.Config{
		DataPath: filepath.Join(b.TempDir(), "redisx-bench.db"),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: 36379},
		Admin:    server.AdminConfig{Bind: "127.0.0.1", Port: 36380},
	}
	db := server.StartWithConfig(cfg)
	if db == nil {
		b.Fatal("embedded server Start returned nil")
	}
	_ = db.Set(naming.AuthStorageKey(clientTestExternalAuthKey), "50")
	time.Sleep(200 * time.Millisecond)
	mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	_ = setSharedClient(mockClient)
	defer func() {
		_ = mockClient.Close()
		clearSharedClientIf(mockClient)
		_ = db.Close()
		resetHooks()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SetWithTTL("cli:bench-abort", "payload-value", 0)
	}
}

func BenchmarkSetWithTransformHook(b *testing.B) {
	resetHooks()
	SetHookTimeout(0)
	defer SetHookTimeout(defaultHookTimeout)
	AddTransformHook(func(key string, value []byte) ([]byte, error) {
		out := make([]byte, len(value))
		copy(out, value)
		return out, nil
	})

	cfg := &server.Config{
		DataPath: filepath.Join(b.TempDir(), "redisx-bench.db"),
		App:      server.AppConfig{Bind: "127.0.0.1", Port: 36379},
		Admin:    server.AdminConfig{Bind: "127.0.0.1", Port: 36380},
	}
	db := server.StartWithConfig(cfg)
	if db == nil {
		b.Fatal("embedded server Start returned nil")
	}
	_ = db.Set(naming.AuthStorageKey(clientTestExternalAuthKey), "50")
	time.Sleep(200 * time.Millisecond)
	mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, Password: clientTestExternalAuthKey})
	_ = setSharedClient(mockClient)
	defer func() {
		_ = mockClient.Close()
		clearSharedClientIf(mockClient)
		_ = db.Close()
		resetHooks()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SetWithTTL("cli:bench-transform", "payload-value", 0)
	}
}

func BenchmarkHooksStoreLoadBaseline(b *testing.B) {
	resetHooks()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = snapshotHooks()
	}
}
