package client

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/respx/internal"
	"github.com/kcmvp/respx/server"
	"github.com/kcmvp/respx/storage"
	"github.com/kcmvp/respx/x"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
)

const clientTestServerAddr = "127.0.0.1:36380"
const clientTestExternalAuthKey = "client-test-external-key"
const clientTestMaxConns = 50

var testSchema = storage.JsonSchema("user", 0).PrefixAttr("id").Index("email").Index("age")
var productSchema = storage.JsonSchema("product", 0).PrefixAttr("id")

func (s *ClientTestSuite) SetupTest() {
	Disconnect()

	handlersMu.Lock()
	handlersByTopic = make(map[string]chan *ReceivedMessage)
	handlersMu.Unlock()

	kvClientMu.Lock()
	if kvClient != nil {
		_ = kvClient.Close()
	}
	kvClient = nil
	kvClientMu.Unlock()

	cliOnce = sync.Once{}

	for len(pubChan) > 0 {
		<-pubChan
	}
	for len(subReqCh) > 0 {
		<-subReqCh
	}
	atomic.StoreUint64(&pipeDrops, 0)
}

type ClientTestSuite struct {
	suite.Suite
}

func (s *ClientTestSuite) SetupSuite() {
	s.T().Setenv("HOME", s.T().TempDir())
	s.T().Setenv(internal.RespxAuthKeyEnv, clientTestExternalAuthKey)
	server.Start(clientTestServerAddr, clientTestMaxConns, false, testSchema, productSchema)

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
				stored := handlersByTopic[tc.topic]
				handlersMu.RUnlock()
				s.NotNil(stored)

				select {
				case topic := <-subReqCh:
					s.Equal(tc.topic, topic)
				default:
					s.FailNow("subReqCh should contain one subscription request")
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
				for i := 0; i < clientTestMaxConns+5; i++ {
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
				// The error message for an intentionally rejected connection can be "EOF" or "connection reset by peer"
				// depending on the OS and timing of when the socket is closed by the server.
				if overflowErr != nil && tc.wantErrMsg != "" {
					errMsg := overflowErr.Error()
					s.True(strings.Contains(errMsg, "EOF") || strings.Contains(errMsg, "connection reset by peer") || strings.Contains(errMsg, tc.wantErrMsg),
						"Expected error to contain EOF, connection reset by peer, or %q, but got: %s", tc.wantErrMsg, errMsg)
				}
			}
		})
	}
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

func (s *ClientTestSuite) TestQueryIndexCommand() {
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
		schema    string
		index     string
		filter    x.Filter
		desc      bool
		expectErr bool
		expectLen int
	}{
		{"Missing schema", "", "email", x.Eq("email", "ken@example.com"), false, true, 0},
		{"Missing index", "user", "", x.Eq("email", "ken@example.com"), false, true, 0},
		{"Index not exists", "user", "unknown", x.Eq("email", "ken@example.com"), false, true, 0},
		{"Eq string", "user", "email", x.Eq("email", "ken@example.com"), false, false, 1},
		{"Eq false", "user", "email", x.Eq("email", "nobody@example.com"), false, false, 0},
		{"Gt number", "user", "age", x.Gt("age", 25), false, false, 2},
		{"Lt number", "user", "age", x.Lt("age", 35), false, false, 2},
		{"And true", "user", "age", x.And(x.Gt("age", 25), x.Eq("status", "active")), false, false, 2},
		{"And false", "user", "age", x.And(x.Gt("age", 35), x.Eq("status", "pending")), false, false, 0},
		{"Or", "user", "age", x.Or(x.Lt("age", 25), x.Eq("status", "active")), false, false, 3},
		{"Empty filter", "user", "email", nil, false, false, 3},
		{"Descend test", "user", "age", x.Gt("age", 10), true, false, 3},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			res := QueryIndex(tt.schema, tt.index, tt.filter, tt.desc)

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

func (s *ClientTestSuite) TestQueryKeyCommand() {
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
		schema    string
		pattern   string
		filter    x.Filter
		desc      bool
		expectErr bool
		expectLen int
	}{
		{"Missing schema", "", "*", x.Eq("name", "Apple"), false, true, 0},
		{"Missing pattern", "product", "", x.Eq("name", "Apple"), false, true, 0},
		{"Match one", "product", "*", x.Eq("name", "Apple"), false, false, 1},
		{"Match none by filter", "product", "*", x.Eq("name", "Grape"), false, false, 0},
		{"Match none by pattern", "product", "99*", x.Eq("name", "Apple"), false, false, 0},
		{"Gt number", "product", "*", x.Gt("price", 6), false, false, 2},
		{"Lt number", "product", "*", x.Lt("stock", 150), false, false, 2},
		{"And true", "product", "*", x.And(x.Gt("price", 6), x.Lt("stock", 150)), false, false, 1},
		{"Empty filter", "product", "*", nil, false, false, 3},
		{"Descend test", "product", "*", x.Gt("price", 4), true, false, 3},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			res := QueryKey(tt.schema, tt.pattern, tt.filter, tt.desc)

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
				handlersByTopic["initial-topic"] = initialHandler
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
				handlersByTopic["runtime-topic"] = runtimeHandler
				handlersMu.Unlock()
				subReqCh <- "runtime-topic"

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
