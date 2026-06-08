package client

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func ensureExternalClientTestServer(t *testing.T) {
	t.Helper()
	ensureClientTestServer(t)
}

func newExternalAuthedClientForTest(t *testing.T) *redis.Client {
	t.Helper()

	ensureExternalClientTestServer(t)
	client, err := connect(clientTestServerAddr, clientTestExternalAuthKey)
	if err != nil {
		t.Fatalf("connect() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestExternalHealthCheckAndConnectWithServer(t *testing.T) {
	resetHandlerStateForTest()
	ensureExternalClientTestServer(t)

	client, err := connect(clientTestServerAddr, clientTestExternalAuthKey)
	if err != nil {
		t.Fatalf("connect() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if err := healthCheck(context.Background(), client); err != nil {
		t.Fatalf("healthCheck() error = %v, want nil", err)
	}
}

func TestExternalConnectRejectsWrongPassword(t *testing.T) {
	resetHandlerStateForTest()
	ensureExternalClientTestServer(t)

	client, err := connect(clientTestServerAddr, "wrong-external-password")
	if err == nil {
		_ = client.Close()
		t.Fatal("connect() error = nil, want non-nil for wrong external password")
	}
}

func TestExternalAuthWithServerSuccess(t *testing.T) {
	resetHandlerStateForTest()

	mockClient := newExternalAuthedClientForTest(t)
	setSharedClient(mockClient)

	if err := Auth(clientTestExternalAuthKey); err != nil {
		t.Fatalf("Auth() error = %v, want nil", err)
	}
}

func TestExternalSetGetRoundTrip(t *testing.T) {
	resetHandlerStateForTest()

	mockClient := newExternalAuthedClientForTest(t)
	setSharedClient(mockClient)

	if err := Set("external-key", "external-value"); err != nil {
		t.Fatalf("Set() error = %v, want nil", err)
	}

	got, err := Get("external-key")
	if err != nil {
		t.Fatalf("Get() error = %v, want nil", err)
	}
	if got != "external-value" {
		t.Fatalf("Get() = %q, want %q", got, "external-value")
	}
}

func TestExternalProducePublishesMessageAndStopsOnCancel(t *testing.T) {
	resetHandlerStateForTest()

	client := newExternalAuthedClientForTest(t)

	ctxSub, cancelSub := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelSub()
	sub := client.Subscribe(ctxSub, "external-orders")
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Receive(ctxSub); err != nil {
		t.Fatalf("sub.Receive() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- produce(ctx, client)
	}()

	pubChan <- outgoingMessage{topic: "external-orders", payload: "hello"}

	msgCh := sub.Channel()
	select {
	case msg := <-msgCh:
		if msg == nil || msg.Payload != "hello" || msg.Channel != "external-orders" {
			t.Fatalf("received message = %#v, want topic=external-orders payload=hello", msg)
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
