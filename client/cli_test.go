package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/respx/internal"
	"github.com/kcmvp/respx/server"
	"github.com/redis/go-redis/v9"
)

const clientTestServerAddr = "127.0.0.1:36380"
const clientTestExternalAuthKey = "client-test-external-key"
const clientTestMaxConns = 3

func ensureClientTestServer(t *testing.T) {
	t.Helper()
	t.Setenv("mresp.auth_key", clientTestExternalAuthKey)

	server.Start(clientTestServerAddr, clientTestMaxConns)

	for i := 0; i < 30; i++ {
		probe, err := connect(clientTestServerAddr, internal.AuthKey())
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
	client, err := connect(clientTestServerAddr, internal.AuthKey())
	if err != nil {
		t.Fatalf("connect() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func resetHandlerStateForTest() {
	handlersMu.Lock()
	handlersByTopic = make(map[string]MessageHandler)
	handlersMu.Unlock()

	kvClientMu.Lock()
	kvClient = nil
	kvClientMu.Unlock()

	for len(pubChan) > 0 {
		<-pubChan
	}
	for len(subReqCh) > 0 {
		<-subReqCh
	}
	atomic.StoreUint64(&pipeDrops, 0)
}

func TestRegisterHandlerRejectsNilHandler(t *testing.T) {
	resetHandlerStateForTest()

	err := RegisterHandler("topic-a", nil)
	if err == nil {
		t.Fatal("RegisterHandler() error = nil, want non-nil")
	}
}

func TestRegisterHandlerRejectsEmptyTopic(t *testing.T) {
	resetHandlerStateForTest()

	err := RegisterHandler("", make(chan *ReceivedMessage, 1))
	if err == nil {
		t.Fatal("RegisterHandler() empty topic error = nil, want non-nil")
	}
}

func TestRegisterHandlerRejectsDuplicateHandlerOnSameTopic(t *testing.T) {
	resetHandlerStateForTest()

	h := make(chan *ReceivedMessage, 2)
	h2 := make(chan *ReceivedMessage, 2)
	if err := RegisterHandler("orders", h); err != nil {
		t.Fatalf("first RegisterHandler() error = %v, want nil", err)
	}
	if err := RegisterHandler("orders", h2); err == nil {
		t.Fatal("duplicate RegisterHandler() error = nil, want non-nil")
	}
}

func TestRegisterHandlerStoresHandlerAndSignalsSubscription(t *testing.T) {
	resetHandlerStateForTest()

	h := make(chan *ReceivedMessage, 1)
	if err := RegisterHandler("orders", h); err != nil {
		t.Fatalf("RegisterHandler() error = %v, want nil", err)
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

	if err := Auth(internal.AuthKey()); err != nil {
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

func TestSendToReturnsFalseWhenNoSharedClient(t *testing.T) {
	resetHandlerStateForTest()

	if ok := SendTo("topic-a", "hello"); ok {
		t.Fatal("SendTo() = true, want false when client is not connected")
	}
	if got := len(pubChan); got != 0 {
		t.Fatalf("pubChan len = %d, want 0", got)
	}
}

func TestSendToReturnsFalseForEmptyTopic(t *testing.T) {
	resetHandlerStateForTest()

	if ok := SendTo("", "hello"); ok {
		t.Fatal("SendTo() = true, want false for empty topic")
	}
}

func TestSendToEnqueuesWhenSharedClientExists(t *testing.T) {
	resetHandlerStateForTest()

	mockClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = mockClient.Close() })
	setSharedClient(mockClient)

	if ok := SendTo("topic-a", "hello"); !ok {
		t.Fatal("SendTo() = false, want true")
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

func TestSendToDropsWhenQueueIsFull(t *testing.T) {
	resetHandlerStateForTest()

	mockClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = mockClient.Close() })
	setSharedClient(mockClient)

	for i := 0; i < cap(pubChan); i++ {
		pubChan <- outgoingMessage{topic: "filled", payload: "x"}
	}

	if ok := SendTo("topic-a", "hello"); ok {
		t.Fatal("SendTo() = true, want false when pubChan is full")
	}
	if drops := atomic.LoadUint64(&pipeDrops); drops != 1 {
		t.Fatalf("pipeDrops = %d, want 1", drops)
	}
}

func TestHealthCheckAndConnectWithServer(t *testing.T) {
	resetHandlerStateForTest()
	ensureClientTestServer(t)

	client, err := connect(clientTestServerAddr, internal.AuthKey())
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

	if err := Connect(clientTestServerAddr, internal.AuthKey()); err != nil {
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

func TestExternalConnectExceedsMaxConnections(t *testing.T) {
	resetHandlerStateForTest()
	ensureClientTestServer(t)

	clients := make([]*redis.Client, 0, clientTestMaxConns)
	t.Cleanup(func() {
		for _, c := range clients {
			_ = c.Close()
		}
	})

	for i := 0; i < clientTestMaxConns; i++ {
		c, err := connect(clientTestServerAddr, clientTestExternalAuthKey)
		if err != nil {
			t.Fatalf("connect() #%d error = %v, want nil", i+1, err)
		}
		clients = append(clients, c)
	}

	extra, err := connect(clientTestServerAddr, clientTestExternalAuthKey)
	if err == nil {
		_ = extra.Close()
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
