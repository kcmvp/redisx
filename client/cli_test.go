package client

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/respx/internal"
	"github.com/kcmvp/respx/server"
	"github.com/kcmvp/respx/storage"
	"github.com/kcmvp/respx/x"
	"github.com/redis/go-redis/v9"
)

const clientTestServerAddr = "127.0.0.1:36380"
const clientTestExternalAuthKey = "client-test-external-key"
const clientTestMaxConns = 50

var testSchema = storage.JsonSchema("user", 0).PrefixAttr("id").Index("email").Index("age")

func ensureClientTestServer(t *testing.T) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv(internal.RespxAuthKeyEnv, clientTestExternalAuthKey)

	server.Start(clientTestServerAddr, clientTestMaxConns, false, testSchema)

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

func newAuthedClientForTest(t *testing.T) *redis.Client {
	t.Helper()

	ensureClientTestServer(t)
	client, err := connect(clientTestServerAddr, clientTestExternalAuthKey)
	if err != nil {
		t.Fatalf("connect() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func resetHandlerStateForTest() {
	Disconnect()

	handlersMu.Lock()
	handlersByTopic = make(map[string]MessageHandler)
	handlersMu.Unlock()

	kvClientMu.Lock()
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

func TestSubscribeRejectsNilHandler(t *testing.T) {
	resetHandlerStateForTest()

	err := Subscribe("topic-a", nil)
	if err == nil {
		t.Fatal("Subscribe() error = nil, want non-nil")
	}
}

func TestSubscribeRejectsEmptyTopic(t *testing.T) {
	resetHandlerStateForTest()

	err := Subscribe("", make(chan *ReceivedMessage, 1))
	if err == nil {
		t.Fatal("Subscribe() empty topic error = nil, want non-nil")
	}
}

func TestSubscribeRejectsDuplicateHandlerOnSameTopic(t *testing.T) {
	resetHandlerStateForTest()

	h := make(chan *ReceivedMessage, 2)
	h2 := make(chan *ReceivedMessage, 2)
	if err := Subscribe("orders", h); err != nil {
		t.Fatalf("first Subscribe() error = %v, want nil", err)
	}
	if err := Subscribe("orders", h2); err == nil {
		t.Fatal("duplicate Subscribe() error = nil, want non-nil")
	}
}

func TestSubscribeStoresHandlerAndSignalsSubscription(t *testing.T) {
	resetHandlerStateForTest()

	h := make(chan *ReceivedMessage, 1)
	if err := Subscribe("orders", h); err != nil {
		t.Fatalf("Subscribe() error = %v, want nil", err)
	}

	handlersMu.RLock()
	stored := handlersByTopic["orders"]
	handlersMu.RUnlock()
	if stored == nil {
		t.Fatal("handlersByTopic[orders] = nil, want non-nil")
	}

	select {
	case topic := <-subReqCh:
		if topic != "orders" {
			t.Fatalf("subReqCh topic = %q, want %q", topic, "orders")
		}
	default:
		t.Fatal("subReqCh should contain one subscription request")
	}
}

func TestGetReturnsNotConnectedWhenNoSharedClient(t *testing.T) {
	resetHandlerStateForTest()

	_, err := Get("k")
	if err == nil {
		t.Fatal("Get() error = nil, want non-nil")
	}
	if err.Error() != "resp client is not connected" {
		t.Fatalf("Get() error = %q, want %q", err.Error(), "resp client is not connected")
	}
}

func TestSetReturnsNotConnectedWhenNoSharedClient(t *testing.T) {
	resetHandlerStateForTest()

	err := Set("k", "v")
	if err == nil {
		t.Fatal("Set() error = nil, want non-nil")
	}
	if err.Error() != "resp client is not connected" {
		t.Fatalf("Set() error = %q, want %q", err.Error(), "resp client is not connected")
	}
}

func TestGetReturnsEmptyForEmptyKey(t *testing.T) {
	resetHandlerStateForTest()

	v, err := Get("")
	if err != nil {
		t.Fatalf("Get(\"\") error = %v, want nil", err)
	}
	if v != "" {
		t.Fatalf("Get(\"\") value = %q, want empty", v)
	}
}

func TestSetReturnsNilForEmptyKey(t *testing.T) {
	resetHandlerStateForTest()

	if err := Set("", "v"); err != nil {
		t.Fatalf("Set(\"\") error = %v, want nil", err)
	}
}

func TestAuthRejectsEmptyAuthKey(t *testing.T) {
	resetHandlerStateForTest()

	err := Auth("")
	if err == nil {
		t.Fatal("Auth() error = nil, want non-nil")
	}
	if err.Error() != "auth key is empty" {
		t.Fatalf("Auth() error = %q, want %q", err.Error(), "auth key is empty")
	}
}

func TestAuthReturnsNotConnectedWhenNoSharedClient(t *testing.T) {
	resetHandlerStateForTest()

	err := Auth("token")
	if err == nil {
		t.Fatal("Auth() error = nil, want non-nil")
	}
	if err.Error() != "resp client is not connected" {
		t.Fatalf("Auth() error = %q, want %q", err.Error(), "resp client is not connected")
	}
}

func TestAuthWithServerSuccess(t *testing.T) {
	resetHandlerStateForTest()

	mockClient := newAuthedClientForTest(t)
	setSharedClient(mockClient)

	if err := Auth(clientTestExternalAuthKey); err != nil {
		t.Fatalf("Auth() error = %v, want nil", err)
	}
}

func TestAuthWithServerInvalidPassword(t *testing.T) {
	resetHandlerStateForTest()
	ensureClientTestServer(t)

	mockClient := redis.NewClient(&redis.Options{Addr: clientTestServerAddr, PoolSize: 1, Protocol: 2})
	t.Cleanup(func() { _ = mockClient.Close() })
	setSharedClient(mockClient)

	if err := Auth("wrong"); err == nil {
		t.Fatal("Auth() error = nil, want non-nil for invalid password")
	}
}

func TestSetAndClearSharedClientHelpers(t *testing.T) {
	resetHandlerStateForTest()

	c1 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	c2 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})

	if prev := setSharedClient(c1); prev != nil {
		t.Fatal("first setSharedClient() prev != nil, want nil")
	}
	if got := getSharedClient(); got != c1 {
		t.Fatal("getSharedClient() should return c1")
	}
	if prev := setSharedClient(c2); prev != c1 {
		t.Fatal("second setSharedClient() prev should be c1")
	}

	clearSharedClientIf(c1)
	if got := getSharedClient(); got != c2 {
		t.Fatal("clearSharedClientIf(c1) should not clear current c2")
	}

	clearSharedClientIf(c2)
	if got := getSharedClient(); got != nil {
		t.Fatal("clearSharedClientIf(c2) should clear shared client")
	}
}

func TestQueryIndexCommand(t *testing.T) {
	resetHandlerStateForTest()
	ensureClientTestServer(t)

	if err := Connect(clientTestServerAddr, clientTestExternalAuthKey); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// wait for client connection
	var connected bool
	for i := 0; i < 30; i++ {
		client := getSharedClient()
		if client != nil {
			if err := healthCheck(context.Background(), client); err == nil {
				connected = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !connected {
		t.Fatal("Connect() failed to establish shared client in time")
	}

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
		if err := Set(d.key, d.val); err != nil {
			t.Fatalf("Failed to seed data: %v", err)
		}
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

		{"Gt number", "user", "age", x.Gt("age", 25), false, false, 2}, // 30, 40
		{"Lt number", "user", "age", x.Lt("age", 35), false, false, 2}, // 20, 30

		{"And true", "user", "age", x.And(x.Gt("age", 25), x.Eq("status", "active")), false, false, 2}, // 30, 40
		{"And false", "user", "age", x.And(x.Gt("age", 35), x.Eq("status", "pending")), false, false, 0},

		{"Or", "user", "age", x.Or(x.Lt("age", 25), x.Eq("status", "active")), false, false, 3}, // 20(lt), 30(active), 40(active)

		{"Empty filter", "user", "email", nil, false, false, 3},          // Matches all in index
		{"Descend test", "user", "age", x.Gt("age", 10), true, false, 3}, // Test DESC flag
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := QueryIndex(tt.schema, tt.index, tt.filter, tt.desc)

			if tt.expectErr {
				if !res.IsError() {
					t.Errorf("expected error, got success")
				}
				return
			}

			if res.IsError() {
				t.Fatalf("unexpected error: %v", res.Error())
			}

			results := res.MustGet()
			if len(results) != tt.expectLen {
				t.Errorf("expected %d results, got %d", tt.expectLen, len(results))
			}
		})
	}
}

func TestQueryKeyCommand(t *testing.T) {
	resetHandlerStateForTest()
	ensureClientTestServer(t)

	if err := Connect(clientTestServerAddr, clientTestExternalAuthKey); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// wait for client connection
	var connected bool
	for i := 0; i < 30; i++ {
		client := getSharedClient()
		if client != nil {
			if err := healthCheck(context.Background(), client); err == nil {
				connected = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !connected {
		t.Fatal("Connect() failed to establish shared client in time")
	}

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
		if err := Set(d.key, d.val); err != nil {
			t.Fatalf("Failed to seed data: %v", err)
		}
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

		{"Gt number", "product", "*", x.Gt("price", 6), false, false, 2},   // 10, 8
		{"Lt number", "product", "*", x.Lt("stock", 150), false, false, 2}, // 100, 50

		{"And true", "product", "*", x.And(x.Gt("price", 6), x.Lt("stock", 150)), false, false, 1}, // 10

		{"Empty filter", "product", "*", nil, false, false, 3},             // Matches all matching keys
		{"Descend test", "product", "*", x.Gt("price", 4), true, false, 3}, // Matches all
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := QueryKey(tt.schema, tt.pattern, tt.filter, tt.desc)

			if tt.expectErr {
				if !res.IsError() {
					t.Errorf("expected error, got success")
				}
				return
			}

			if res.IsError() {
				t.Fatalf("unexpected error: %v", res.Error())
			}

			results := res.MustGet()
			if len(results) != tt.expectLen {
				t.Errorf("expected %d results, got %d", tt.expectLen, len(results))
			}
		})
	}
}

func TestPublishReturnsFalseWhenNoSharedClient(t *testing.T) {
	resetHandlerStateForTest()

	if ok := Publish("topic-a", "hello"); ok {
		t.Fatal("Publish() = true, want false when client is not connected")
	}
	if got := len(pubChan); got != 0 {
		t.Fatalf("pubChan len = %d, want 0", got)
	}
}

func TestPublishReturnsFalseForEmptyTopic(t *testing.T) {
	resetHandlerStateForTest()

	if ok := Publish("", "hello"); ok {
		t.Fatal("Publish() = true, want false for empty topic")
	}
}

func TestPublishEnqueuesWhenSharedClientExists(t *testing.T) {
	resetHandlerStateForTest()

	mockClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = mockClient.Close() })
	setSharedClient(mockClient)

	if ok := Publish("topic-a", "hello"); !ok {
		t.Fatal("Publish() = false, want true")
	}

	select {
	case out := <-pubChan:
		if out.topic != "topic-a" || out.payload != "hello" {
			t.Fatalf("queued message = (%q, %q), want (%q, %q)", out.topic, out.payload, "topic-a", "hello")
		}
	default:
		t.Fatal("pubChan should have one queued message")
	}
}

func TestPublishDropsWhenQueueIsFull(t *testing.T) {
	resetHandlerStateForTest()

	mockClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = mockClient.Close() })
	setSharedClient(mockClient)

	for i := 0; i < cap(pubChan); i++ {
		pubChan <- outgoingMessage{topic: "filled", payload: "x"}
	}

	if ok := Publish("topic-a", "hello"); ok {
		t.Fatal("Publish() = true, want false when pubChan is full")
	}
	if drops := atomic.LoadUint64(&pipeDrops); drops != 1 {
		t.Fatalf("pipeDrops = %d, want 1", drops)
	}
}

func TestHealthCheckAndConnectWithServer(t *testing.T) {
	resetHandlerStateForTest()
	ensureClientTestServer(t)

	client, err := connect(clientTestServerAddr, clientTestExternalAuthKey)
	if err != nil {
		t.Fatalf("connect() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if err := healthCheck(context.Background(), client); err != nil {
		t.Fatalf("healthCheck() error = %v, want nil", err)
	}
}

func TestConnectStartsSharedClientWithPassword(t *testing.T) {
	resetHandlerStateForTest()
	ensureClientTestServer(t)

	if err := Connect(clientTestServerAddr, clientTestExternalAuthKey); err != nil {
		t.Fatalf("Connect() error = %v, want nil", err)
	}

	for i := 0; i < 30; i++ {
		client := getSharedClient()
		if client != nil {
			if err := healthCheck(context.Background(), client); err == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("Connect() should populate a healthy shared client")
}

func TestConnectFailsForInvalidAddress(t *testing.T) {
	resetHandlerStateForTest()

	client, err := connect("bad-addr", "token")
	if err == nil {
		if client != nil {
			_ = client.Close()
		}
		t.Fatal("connect() error = nil, want non-nil")
	}
	if client != nil {
		t.Fatal("connect() client != nil, want nil")
	}
}

func TestConnectRejectsEmptyAuthKey(t *testing.T) {
	resetHandlerStateForTest()

	if err := Connect(clientTestServerAddr, ""); err == nil {
		t.Fatal("Connect() error = nil, want non-nil for empty auth key")
	}
}

func TestConnectHelperRejectsEmptyAuthKey(t *testing.T) {
	resetHandlerStateForTest()

	client, err := connect(clientTestServerAddr, "")
	if err == nil {
		if client != nil {
			_ = client.Close()
		}
		t.Fatal("connect() error = nil, want non-nil for empty auth key")
	}
}

func TestExternalSetWithTTL(t *testing.T) {
	resetHandlerStateForTest()
	ensureClientTestServer(t)

	if err := Connect(clientTestServerAddr, clientTestExternalAuthKey); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// wait for client connection
	var connected bool
	for i := 0; i < 30; i++ {
		client := getSharedClient()
		if client != nil {
			if err := healthCheck(context.Background(), client); err == nil {
				connected = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !connected {
		t.Fatal("Connect() failed to establish shared client in time")
	}

	key := "ttl_key"
	val := "ttl_val"

	// set with 200ms TTL
	if err := SetWithTTL(key, val, 200*time.Millisecond); err != nil {
		t.Fatalf("SetWithTTL() error = %v", err)
	}

	// Immediate Get should succeed
	got, err := Get(key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != val {
		t.Errorf("Get() = %v, want %v", got, val)
	}

	// Wait for expiration
	time.Sleep(300 * time.Millisecond)

	// Get after expiration should be empty
	got, err = Get(key)
	if err != redis.Nil && got != "" {
		t.Errorf("Get() after TTL = %v, %v, want empty string and redis.Nil", got, err)
	}
}

func TestExternalConnectExceedsMaxConnections(t *testing.T) {
	resetHandlerStateForTest()
	ensureClientTestServer(t)

	var clients []*redis.Client
	t.Cleanup(func() {
		for _, c := range clients {
			_ = c.Close()
		}
	})

	var overflowErr error
	for i := 0; i < clientTestMaxConns+5; i++ {
		var c *redis.Client
		var err error
		for attempt := 0; attempt < 10; attempt++ {
			c, err = connect(clientTestServerAddr, clientTestExternalAuthKey)
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

	if overflowErr == nil {
		t.Fatal("connect() overflow error = nil, want non-nil when exceeding external max connections")
	}
}

func TestProducePublishesMessageAndStopsOnCancel(t *testing.T) {
	resetHandlerStateForTest()

	client := newAuthedClientForTest(t)

	ctxSub, cancelSub := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelSub()
	sub := client.Subscribe(ctxSub, "orders")
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Receive(ctxSub); err != nil {
		t.Fatalf("sub.Receive() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- produce(ctx, client)
	}()

	pubChan <- outgoingMessage{topic: "orders", payload: "hello"}

	msgCh := sub.Channel()
	select {
	case msg := <-msgCh:
		if msg == nil || msg.Payload != "hello" || msg.Channel != "orders" {
			t.Fatalf("received message = %#v, want topic=orders payload=hello", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting published message")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("produce() error = %v, want nil after cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("produce() did not exit after cancel")
	}
}

func TestConsumeDispatchesInitialAndRuntimeTopics(t *testing.T) {
	resetHandlerStateForTest()

	client := newAuthedClientForTest(t)

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

	// Give consume a brief moment to finish initial subscribe setup.
	time.Sleep(80 * time.Millisecond)

	if err := client.Publish(context.Background(), "initial-topic", "init-msg").Err(); err != nil {
		t.Fatalf("publish initial topic error = %v", err)
	}
	select {
	case got := <-initialHandler:
		if got == nil || got.Channel != "initial-topic" || got.Payload != "init-msg" {
			t.Fatalf("initial handler got = %#v, want channel=initial-topic payload=init-msg", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting initial-topic message")
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
			if got == nil || got.Channel != "runtime-topic" || got.Payload != "rt-msg" {
				t.Fatalf("runtime handler got = %#v, want channel=runtime-topic payload=rt-msg", got)
			}
			runtimeDelivered = true
		case <-tick.C:
			if err := client.Publish(context.Background(), "runtime-topic", "rt-msg").Err(); err != nil {
				t.Fatalf("publish runtime topic error = %v", err)
			}
		case <-deadline:
			t.Fatal("timeout waiting runtime-topic message")
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("consume() error = %v, want nil after cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consume() did not exit after cancel")
	}
}
