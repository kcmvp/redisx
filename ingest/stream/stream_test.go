package stream

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type testDocument string

type recordedInstruction struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     int64    `json:"id"`
}

func (testDocument) Namespace() string  { return "test" }
func (testDocument) Mem() bool          { return false }
func (testDocument) KeyAttrs() []string { return []string{"id"} }
func (d testDocument) RawJSON() string  { return string(d) }
func (testDocument) TTL() time.Duration { return time.Minute }

func TestStartWriteAndClose(t *testing.T) {
	var upgrader websocket.Upgrader
	received := make(chan string, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		if writeErr := conn.WriteMessage(websocket.TextMessage, []byte(`{"data":"hello"}`)); writeErr != nil {
			t.Errorf("write welcome message failed: %v", writeErr)
			return
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("read client message failed: %v", err)
			return
		}
		received <- string(message)
	}))
	defer server.Close()

	s := Start[testDocument](toWebsocketURL(server.URL))
	defer s.Close()

	select {
	case message := <-s.C():
		if string(message) != `"hello"` {
			t.Fatalf("unexpected stream message: %s", message)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for stream message")
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := s.Write([]byte(`ping`))
		if err == nil {
			break
		}
		if !errors.Is(err, ErrDisconnected) {
			t.Fatalf("write failed: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for writable stream: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case message := <-received:
		if message != "ping" {
			t.Fatalf("unexpected written message: %s", message)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server write")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := s.Write([]byte(`ping`)); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestStreamSubscribeAndUnsubscribe(t *testing.T) {
	var upgrader websocket.Upgrader
	connected := make(chan struct{}, 1)
	instructions := make(chan recordedInstruction, 2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		select {
		case connected <- struct{}{}:
		default:
		}

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var instruction recordedInstruction
			if err := json.Unmarshal(message, &instruction); err != nil {
				t.Errorf("unmarshal instruction failed: %v", err)
				return
			}
			instructions <- instruction
		}
	}))
	defer server.Close()

	var nextID atomic.Int64
	subscribe := subscriptionMessage("SUBSCRIBE", &nextID)
	unsubscribe := subscriptionMessage("UNSUBSCRIBE", &nextID)

	s := StartSubscribable[testDocument](toWebsocketURL(server.URL), subscribe, unsubscribe)
	defer s.Close()

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for websocket connection")
	}

	if err := s.Subscribe("ethusdt@trade", "btcusdt@trade", "ethusdt@trade"); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	first := waitInstruction(t, instructions)
	if first.Method != "SUBSCRIBE" {
		t.Fatalf("unexpected subscribe method: %s", first.Method)
	}
	if !sameStrings(first.Params, []string{"ethusdt@trade", "btcusdt@trade"}) {
		t.Fatalf("unexpected subscribe params: %#v", first.Params)
	}

	if err := s.Unsubscribe("ethusdt@trade"); err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}

	second := waitInstruction(t, instructions)
	if second.Method != "UNSUBSCRIBE" {
		t.Fatalf("unexpected unsubscribe method: %s", second.Method)
	}
	if !slices.Equal(second.Params, []string{"ethusdt@trade"}) {
		t.Fatalf("unexpected unsubscribe params: %#v", second.Params)
	}

	if got := s.List(); !slices.Equal(got, []string{"btcusdt@trade"}) {
		t.Fatalf("unexpected subscription list: %#v", got)
	}
}

func TestStreamRestoresSubscriptionsAfterReconnect(t *testing.T) {
	var upgrader websocket.Upgrader
	var connects atomic.Int32
	instructions := make(chan recordedInstruction, 4)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		connects.Add(1)

		_, message, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var instruction recordedInstruction
		if err := json.Unmarshal(message, &instruction); err != nil {
			t.Errorf("unmarshal instruction failed: %v", err)
			return
		}
		instructions <- instruction

		_ = conn.Close()
	}))
	defer server.Close()

	var nextID atomic.Int64
	subscribe := subscriptionMessage("SUBSCRIBE", &nextID)
	unsubscribe := subscriptionMessage("UNSUBSCRIBE", &nextID)

	s := StartSubscribable[testDocument](toWebsocketURL(server.URL), subscribe, unsubscribe)
	defer s.Close()

	if err := s.Subscribe("ethusdt@trade", "btcusdt@trade"); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	first := waitInstruction(t, instructions)
	second := waitInstruction(t, instructions)

	if first.Method != "SUBSCRIBE" || second.Method != "SUBSCRIBE" {
		t.Fatalf("unexpected restore methods: %#v %#v", first, second)
	}
	want := []string{"btcusdt@trade", "ethusdt@trade"}
	if !slices.Equal(first.Params, want) {
		t.Fatalf("unexpected first restore params: %#v", first.Params)
	}
	if !slices.Equal(second.Params, want) {
		t.Fatalf("unexpected second restore params: %#v", second.Params)
	}
	if second.ID <= first.ID {
		t.Fatalf("expected reconnect instruction id to increase: first=%d second=%d", first.ID, second.ID)
	}
	if connects.Load() < 2 {
		t.Fatalf("expected reconnect, got %d connections", connects.Load())
	}
}

func TestPlainStreamRejectsSubscribeAndUnsubscribe(t *testing.T) {
	s := newStream("wss://example/ws", DefaultStreamHandler[testDocument], 0)

	assertPanic(t, func() {
		_ = s.Subscribe("btcusdt@trade")
	})
	assertPanic(t, func() {
		_ = s.Unsubscribe("btcusdt@trade")
	})
}

func TestSubscribeRollsBackStateOnCloseError(t *testing.T) {
	var s *Stream[testDocument]
	subscribe := func(params ...string) string {
		_ = s.Close()
		return `{"method":"SUBSCRIBE"}`
	}
	s = newStream("wss://example/ws", DefaultStreamHandler[testDocument], 0)
	s.subscribe = subscribe
	s.unsubscribe = func(params ...string) string { return `{"method":"UNSUBSCRIBE"}` }

	err := s.Subscribe("btcusdt@trade")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if got := s.List(); len(got) != 0 {
		t.Fatalf("expected rollback after close, got %#v", got)
	}
}

func TestUnsubscribeRollsBackStateOnCloseError(t *testing.T) {
	var s *Stream[testDocument]
	unsubscribe := func(params ...string) string {
		_ = s.Close()
		return `{"method":"UNSUBSCRIBE"}`
	}

	s = newStream("wss://example/ws", DefaultStreamHandler[testDocument], 0)
	s.subscribe = func(params ...string) string { return `{"method":"SUBSCRIBE"}` }
	s.unsubscribe = unsubscribe
	s.subs["btcusdt@trade"] = struct{}{}

	err := s.Unsubscribe("btcusdt@trade")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if got := s.List(); !slices.Equal(got, []string{"btcusdt@trade"}) {
		t.Fatalf("expected rollback after close, got %#v", got)
	}
}

func TestDetectConnTreatsInboundMessagesAsAlive(t *testing.T) {
	var upgrader websocket.Upgrader
	serverDone := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for i := 0; i < 8; i++ {
			<-ticker.C
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"data":"alive"}`)); err != nil {
				return
			}
		}

		<-serverDone
	}))
	defer server.Close()
	defer close(serverDone)

	ws, _, err := websocket.DefaultDialer.Dial(toWebsocketURL(server.URL), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer ws.Close()

	done := make(chan struct{})
	touch := detectConn(ws, 30*time.Millisecond, 0, done)
	defer close(done)

	for i := 0; i < 3; i++ {
		if _, _, err := ws.ReadMessage(); err != nil {
			t.Fatalf("unexpected read failure while stream is active: %v", err)
		}
		touch()
	}

	time.Sleep(20 * time.Millisecond)
	if err := ws.WriteMessage(websocket.TextMessage, []byte(`ping`)); err != nil {
		t.Fatalf("connection was closed despite inbound traffic: %v", err)
	}
}

func TestStartActivePing(t *testing.T) {
	var upgrader websocket.Upgrader
	pinged := make(chan struct{}, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		conn.SetPingHandler(func(appData string) error {
			select {
			case pinged <- struct{}{}:
			default:
			}
			return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(100*time.Millisecond))
		})

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	s := Start[testDocument](toWebsocketURL(server.URL), 20*time.Millisecond)
	defer s.Close()

	select {
	case <-pinged:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for active ping")
	}
}

func waitInstruction(t *testing.T, instructions <-chan recordedInstruction) recordedInstruction {
	t.Helper()

	select {
	case instruction := <-instructions:
		return instruction
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subscription instruction")
		return recordedInstruction{}
	}
}

func toWebsocketURL(url string) string {
	return "ws" + strings.TrimPrefix(url, "http")
}

func subscriptionMessage(method string, nextID *atomic.Int64) Subscription {
	return func(params ...string) string {
		payload, err := json.Marshal(recordedInstruction{
			Method: method,
			Params: params,
			ID:     nextID.Add(1),
		})
		if err != nil {
			panic(err)
		}
		return string(payload)
	}
}

func assertPanic(t *testing.T, fn func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	fn()
}

func sameStrings(got []string, want []string) bool {
	got = slices.Clone(got)
	want = slices.Clone(want)
	slices.Sort(got)
	slices.Sort(want)
	return slices.Equal(got, want)
}
