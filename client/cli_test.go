package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/redisx/server"
	"github.com/kcmvp/redisx/x"
	"github.com/kcmvp/redisx/x/contract"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
)

const clientTestServerAddr = "127.0.0.1:36380"
const clientTestExternalAuthKey = "client-test-external-key"
const clientTestAuthLimit = 50

type testUserDoc string

func (testUserDoc) Namespace() string  { return "user" }
func (testUserDoc) Mem() bool          { return false }
func (testUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u testUserDoc) RawJSON() string  { return string(u) }
func (testUserDoc) TTL() time.Duration { return 0 }

func (s *ClientTestSuite) SetupTest() {
	Disconnect()

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
	signalNotifyContextFn = signal.NotifyContext

	for len(pubChan) > 0 {
		<-pubChan
	}
	for len(subscribeReqCh) > 0 {
		<-subscribeReqCh
	}
	atomic.StoreUint64(&pipeDrops, 0)
}

type ClientTestSuite struct {
	suite.Suite
}

func (s *ClientTestSuite) SetupSuite() {
	s.T().Setenv("HOME", s.T().TempDir())
	dbPath := filepath.Join(s.T().TempDir(), "redisx.db")
	db := server.Start(
		clientTestServerAddr,
		dbPath,
		x.Idx[testUserDoc]("age", "*", "age"),
		x.Idx[testUserDoc]("email", "*", "email"),
	)
	s.Require().NotNil(db)
	s.Require().NoError(db.Set(contract.AuthKeyPrefix+contract.StorageKeySeparator+clientTestExternalAuthKey, "50"))

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

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

func (s *ClientTestSuite) TestMemKey() {
	s.Equal("_m_user:1", x.MemKey("user:1"))
	s.Equal("_m_user:1", x.MemKey("_m_user:1"))
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
			func() (string, error) { return Get("k") },
			true, "resp client is not connected", "",
		},
		{
			"Set not connected",
			nil,
			func() (string, error) { return "", Set("k", "v") },
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
				if err := Set("external-key", "external-value"); err != nil {
					return "", err
				}
				return Get("external-key")
			},
			false, "", "external-value",
		},
		{
			"SetWithTTL Success and Expire",
			func() { _ = Connect(clientTestServerAddr, clientTestExternalAuthKey); s.ensureConnected() },
			func() (string, error) {
				if err := SetWithTTL("ttl_key", "ttl_val", 100*time.Millisecond); err != nil {
					return "", err
				}
				v1, _ := Get("ttl_key")
				if v1 != "ttl_val" {
					return v1, nil
				}
				time.Sleep(200 * time.Millisecond)
				v2, err := Get("ttl_key")
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
		ok, err := SetNXWithTTL("k", "v", time.Second)
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

		ok, err := SetNXWithTTL("setnx-fallback", "v1", 0)
		s.NoError(err)
		s.True(ok)

		ok, err = SetNXWithTTL("setnx-fallback", "v2", 0)
		s.NoError(err)
		s.False(ok)
	})

	s.Run("SetNXWithTTL success and expire", func() {
		s.SetupTest()
		err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
		s.NoError(err)
		s.ensureConnected()

		ok, err := SetNXWithTTL("setnx-ttl", "ttl-value", 100*time.Millisecond)
		s.NoError(err)
		s.True(ok)

		val, err := Get("setnx-ttl")
		s.NoError(err)
		s.Equal("ttl-value", val)

		time.Sleep(200 * time.Millisecond)

		_, err = Get("setnx-ttl")
		s.Error(err)
		s.Contains(err.Error(), "redis: nil")
	})

	s.Run("SetNX not connected", func() {
		s.SetupTest()
		ok, err := SetNX("k", "v")
		s.Error(err)
		s.Contains(err.Error(), "resp client is not connected")
		s.False(ok)
	})

	s.Run("Delete not connected", func() {
		s.SetupTest()
		deleted, err := Delete("k")
		s.Error(err)
		s.Contains(err.Error(), "resp client is not connected")
		s.False(deleted)
	})

	s.Run("Keys not connected", func() {
		s.SetupTest()
		keysRes := Keys("k*")
		s.True(keysRes.IsError())
		s.Contains(keysRes.Error().Error(), "resp client is not connected")
	})

	s.Run("SetNX Delete Keys Success", func() {
		s.SetupTest()
		err := Connect(clientTestServerAddr, clientTestExternalAuthKey)
		s.NoError(err)
		s.ensureConnected()

		// Test SetNX
		ok, err := SetNX("key_nx", "val1")
		s.NoError(err)
		s.True(ok)

		ok, err = SetNX("key_nx", "val2")
		s.NoError(err)
		s.False(ok)

		v, err := Get("key_nx")
		s.NoError(err)
		s.Equal("val1", v)

		// Test Keys
		_ = Set("key_another", "val")
		keysRes := Keys("key_*")
		s.False(keysRes.IsError())
		s.ElementsMatch([]string{"key_nx", "key_another"}, keysRes.MustGet())

		// Test Delete
		deleted, err := Delete("key_nx")
		s.NoError(err)
		s.True(deleted)

		keysRes = Keys("key_*")
		s.False(keysRes.IsError())
		s.ElementsMatch([]string{"key_another"}, keysRes.MustGet())

		deleted, err = Delete("key_another")
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

	sigCtx, sigCancel := context.WithCancel(context.Background())
	signalNotifyContextFn = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return sigCtx, sigCancel
	}

	err := ConnectEmbed(clientTestServerAddr)
	s.NoError(err)
	s.ensureConnected()

	sigCancel()

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
	db := server.Start(addr, dbPath)
	if db == nil {
		t.Fatal("server.Start() returned nil")
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
		name       string
		index      string
		keyPattern string
		filter     x.Filter
		desc       bool
		expectErr  bool
		expectLen  int
	}{
		{"Missing index", "", "user:*", x.Eq("email", "ken@example.com"), false, true, 0},
		{"Unknown index", "unknown", "user:*", x.Eq("email", "ken@example.com"), false, true, 0},
		{"Eq string", x.Idx[testUserDoc]("email", "*", "email").Name(), "user:*", x.Eq("email", "ken@example.com"), false, false, 1},
		{"Eq false", x.Idx[testUserDoc]("email", "*", "email").Name(), "user:*", x.Eq("email", "nobody@example.com"), false, false, 0},
		{"Gt number", x.Idx[testUserDoc]("age", "*", "age").Name(), "user:*", x.Gt("age", 25), false, false, 2},
		{"Lt number", x.Idx[testUserDoc]("age", "*", "age").Name(), "user:*", x.Lt("age", 35), false, false, 2},
		{"And true", x.Idx[testUserDoc]("age", "*", "age").Name(), "user:*", x.And(x.Gt("age", 25), x.Eq("status", "active")), false, false, 2},
		{"And false", x.Idx[testUserDoc]("age", "*", "age").Name(), "user:*", x.And(x.Gt("age", 35), x.Eq("status", "pending")), false, false, 0},
		{"Or", x.Idx[testUserDoc]("age", "*", "age").Name(), "user:*", x.Or(x.Lt("age", 25), x.Eq("status", "active")), false, false, 3},
		{"Empty filter", x.Idx[testUserDoc]("email", "*", "email").Name(), "user:*", nil, false, false, 3},
		{"Key pattern narrows index scan", x.Idx[testUserDoc]("age", "*", "age").Name(), "user:2", nil, false, false, 1},
		{"Descend test", x.Idx[testUserDoc]("age", "*", "age").Name(), "user:*", x.Gt("age", 10), true, false, 3},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			res := SearchIndex(tt.index, tt.keyPattern, tt.filter, tt.desc)

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
		pattern   string
		filter    x.Filter
		desc      bool
		expectErr bool
		expectLen int
	}{
		{"Missing pattern", "", x.Eq("name", "Apple"), false, true, 0},
		{"Match one", "product:*", x.Eq("name", "Apple"), false, false, 1},
		{"Match none by filter", "product:*", x.Eq("name", "Grape"), false, false, 0},
		{"Match none by pattern", "99*", x.Eq("name", "Apple"), false, false, 0},
		{"Gt number", "product:*", x.Gt("price", 6), false, false, 2},
		{"Lt number", "product:*", x.Lt("stock", 150), false, false, 2},
		{"And true", "product:*", x.And(x.Gt("price", 6), x.Lt("stock", 150)), false, false, 1},
		{"Empty filter", "product:*", nil, false, false, 3},
		{"Descend test", "product:*", x.Gt("price", 4), true, false, 3},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			res := SearchKey(tt.pattern, tt.filter, tt.desc)

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
		pattern   string
		filter    x.Filter
		updates   []x.Mutation
		expectErr bool
		expectLen int
		check     func()
	}{
		{
			name:      "missing pattern",
			pattern:   "",
			filter:    x.Eq("status", "pending"),
			updates:   []x.Mutation{x.Set("status", "active")},
			expectErr: true,
		},
		{
			name:      "missing update values",
			pattern:   "user:*",
			filter:    x.Eq("status", "pending"),
			expectErr: true,
		},
		{
			name:      "update filtered documents",
			pattern:   "user:*",
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
			pattern:   "user:*",
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
			res := Update(tt.pattern, tt.filter, tt.updates...)
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
