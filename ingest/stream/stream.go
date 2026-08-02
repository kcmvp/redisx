// Package stream provides a reconnecting websocket ingestion stream with
// optional subscription management.
package stream

import (
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kcmvp/redisx/x"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
)

const outputBufferSize = 512

const (
	websocketPingTimeout = 10 * time.Second
	websocketPongTimeout = 10 * time.Second
	websocketTimeout     = 600 * time.Second
)

var (
	// ErrClosed reports that the stream has already been closed.
	ErrClosed = errors.New("stream: closed")
	// ErrDisconnected reports that the stream is reconnecting and has no live
	// connection to write to.
	ErrDisconnected = errors.New("stream: disconnected")
)

// StreamHandler builds a websocket message handler that forwards decoded
// documents into out.
type StreamHandler[D x.Document] func(out chan<- D) func([]byte)

// Subscription formats protocol-level subscribe or unsubscribe messages for a
// set of subscription params.
type Subscription func(params ...string) string

// Stream keeps one reconnecting websocket connection, optional in-memory
// subscriptions, and exposes a bidirectional stream interface.
type Stream[D x.Document] struct {
	endpoint string
	out      chan D
	handler  StreamHandler[D]
	doneC    chan struct{}

	pingInterval time.Duration

	closeOnce sync.Once

	mu     sync.RWMutex
	conn   *conn
	closed bool

	subsMu      sync.RWMutex
	subscribe   Subscription
	unsubscribe Subscription
	subs        map[string]struct{}
}

// Start opens a fixed stream whose subscriptions are already encoded in the
// endpoint URL. Passing one ping interval enables active websocket ping.
func Start[D x.Document](endpoint string, pingInterval ...time.Duration) *Stream[D] {
	return StartWithHandler(endpoint, DefaultStreamHandler[D], pingInterval...)
}

// StartWithHandler is Start with a custom message-to-document handler.
func StartWithHandler[D x.Document](endpoint string, handler StreamHandler[D], pingInterval ...time.Duration) *Stream[D] {
	return start(endpoint, handler, nil, nil, normalizePingInterval(pingInterval...))
}

// StartSubscribable opens a stream with bound subscribe and unsubscribe
// instructions. Passing one ping interval enables active websocket ping.
func StartSubscribable[D x.Document](endpoint string, subscribe Subscription, unsubscribe Subscription, pingInterval ...time.Duration) *Stream[D] {
	return StartSubscribableWithHandler(endpoint, DefaultStreamHandler[D], subscribe, unsubscribe, pingInterval...)
}

// StartSubscribableWithHandler is StartSubscribable with a custom
// message-to-document handler.
func StartSubscribableWithHandler[D x.Document](endpoint string, handler StreamHandler[D], subscribe Subscription, unsubscribe Subscription, pingInterval ...time.Duration) *Stream[D] {
	lo.Assert(subscribe != nil, "stream: nil subscribe")
	lo.Assert(unsubscribe != nil, "stream: nil unsubscribe")
	return start(endpoint, handler, subscribe, unsubscribe, normalizePingInterval(pingInterval...))
}

// C returns the output channel.
func (s *Stream[D]) C() <-chan D {
	return s.out
}

// Write sends a raw protocol message through the current connection.
//
// It returns ErrDisconnected when the stream is currently reconnecting.
func (s *Stream[D]) Write(message []byte) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrClosed
	}
	c := s.conn
	s.mu.RUnlock()

	if c == nil {
		return ErrDisconnected
	}

	return c.write(message)
}

// Subscribe appends subscriptions to the in-memory subscription set and, when
// the connection is live, sends a subscribe instruction on the current
// connection.
func (s *Stream[D]) Subscribe(params ...string) error {
	lo.Assert(s.subscribe != nil, "stream: subscribe is unavailable")
	params = lo.Uniq(lo.Compact(params))
	if len(params) == 0 {
		return nil
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}

	s.subsMu.Lock()
	changed := lo.Filter(params, func(param string, _ int) bool {
		if lo.HasKey(s.subs, param) {
			return false
		}
		s.subs[param] = struct{}{}
		return true
	})
	s.subsMu.Unlock()

	if len(changed) == 0 {
		return nil
	}

	err := s.Write([]byte(s.subscribe(changed...)))
	if errors.Is(err, ErrDisconnected) {
		return nil
	}
	if errors.Is(err, ErrClosed) {
		s.subsMu.Lock()
		for _, param := range changed {
			delete(s.subs, param)
		}
		s.subsMu.Unlock()
	}
	return err
}

// Unsubscribe removes subscriptions from the in-memory subscription set and,
// when the connection is live, sends an unsubscribe instruction on the current
// connection.
func (s *Stream[D]) Unsubscribe(params ...string) error {
	lo.Assert(s.unsubscribe != nil, "stream: unsubscribe is unavailable")
	params = lo.Uniq(lo.Compact(params))
	if len(params) == 0 {
		return nil
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}

	s.subsMu.Lock()
	changed := lo.Filter(params, func(param string, _ int) bool {
		if !lo.HasKey(s.subs, param) {
			return false
		}
		delete(s.subs, param)
		return true
	})
	s.subsMu.Unlock()

	if len(changed) == 0 {
		return nil
	}

	err := s.Write([]byte(s.unsubscribe(changed...)))
	if errors.Is(err, ErrDisconnected) {
		return nil
	}
	if errors.Is(err, ErrClosed) {
		s.subsMu.Lock()
		for _, param := range changed {
			s.subs[param] = struct{}{}
		}
		s.subsMu.Unlock()
	}
	return err
}

// List returns the current in-memory subscription set.
func (s *Stream[D]) List() []string {
	s.subsMu.RLock()
	defer s.subsMu.RUnlock()

	streams := make([]string, 0, len(s.subs))
	for stream := range s.subs {
		streams = append(streams, stream)
	}
	sort.Strings(streams)

	return streams
}

// Close stops reconnecting and closes the current connection.
func (s *Stream[D]) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		c := s.conn
		s.conn = nil
		s.mu.Unlock()

		close(s.doneC)
		if c != nil {
			c.close()
		}
	})

	return nil
}

// DefaultStreamHandler forwards raw payloads and unwraps combined payloads by
// extracting the top-level data field when present.
func DefaultStreamHandler[D x.Document](out chan<- D) func([]byte) {
	return func(message []byte) {
		raw := gjson.GetBytes(message, "data").Raw
		if raw == "" {
			raw = string(message)
		}
		out <- D(raw)
	}
}

// private

type conn struct {
	doneC chan struct{}
	stopC chan struct{}

	writeC    chan writeReq
	closeOnce sync.Once
}

type writeReq struct {
	message []byte
	errC    chan error
}

// shared methods

func newStream[D x.Document](endpoint string, handler StreamHandler[D], pingInterval time.Duration) *Stream[D] {
	return &Stream[D]{
		endpoint:     endpoint,
		out:          make(chan D, outputBufferSize),
		handler:      handler,
		doneC:        make(chan struct{}),
		pingInterval: pingInterval,
		subs:         make(map[string]struct{}),
	}
}

func start[D x.Document](endpoint string, handler StreamHandler[D], subscribe Subscription, unsubscribe Subscription, pingInterval time.Duration) *Stream[D] {
	lo.Assert(handler != nil, "stream: nil handler")
	endpoint = strings.TrimSpace(endpoint)
	lo.Assert(endpoint != "", "stream: empty endpoint")

	stream := newStream(endpoint, handler, pingInterval)
	stream.subscribe = subscribe
	stream.unsubscribe = unsubscribe
	go stream.run(stream.restoreSubscriptions)
	return stream
}

func (s *Stream[D]) run(afterConnect func(*conn) error) {
	retryInterval := time.Second
	const maxRetryInterval = 60 * time.Second

	messageHandler := s.handler(s.out)

	for {
		select {
		case <-s.doneC:
			close(s.out)
			return
		default:
		}

		slog.Info("starting stream", "url", s.endpoint)

		c, err := feed(s.endpoint, s.pingInterval, messageHandler, func(err error) {
			slog.Error("stream error", "url", s.endpoint, "error", err)
		})
		if err != nil {
			slog.Error("dial stream failed", "url", s.endpoint, "error", err)
		} else {
			if afterConnect != nil {
				if err := afterConnect(c); err != nil {
					slog.Error("connect hook failed", "url", s.endpoint, "error", err)
					c.close()
					<-c.doneC
					goto retry
				}
			}

			s.setConn(c)
			retryInterval = time.Second

			select {
			case <-s.doneC:
				c.close()
				<-c.doneC
				s.clearConn(c)
				close(s.out)
				return
			case <-c.doneC:
				s.clearConn(c)
				slog.Info("stream disconnected", "url", s.endpoint)
			}
		}

	retry:
		if !sleepRetry(retryInterval, s.doneC) {
			close(s.out)
			return
		}

		retryInterval *= 2
		if retryInterval > maxRetryInterval {
			retryInterval = maxRetryInterval
		}
	}
}

func (s *Stream[D]) setConn(c *conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		c.close()
		return
	}

	s.conn = c
}

func (s *Stream[D]) clearConn(c *conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == c {
		s.conn = nil
	}
}

func (s *Stream[D]) restoreSubscriptions(c *conn) error {
	s.subsMu.RLock()
	subscription := s.subscribe
	s.subsMu.RUnlock()
	if subscription == nil {
		return nil
	}

	params := s.List()
	if len(params) == 0 {
		return nil
	}

	return c.write([]byte(subscription(params...)))
}

func (s *Stream[D]) ensureOpen() error {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	return nil
}

func (c *conn) write(message []byte) error {
	req := writeReq{
		message: append([]byte(nil), message...),
		errC:    make(chan error, 1),
	}

	select {
	case <-c.stopC:
		return ErrClosed
	case <-c.doneC:
		return ErrDisconnected
	case c.writeC <- req:
	}

	select {
	case err := <-req.errC:
		return err
	case <-c.stopC:
		return ErrClosed
	case <-c.doneC:
		return ErrDisconnected
	}
}

func (c *conn) close() {
	c.closeOnce.Do(func() {
		close(c.stopC)
	})
}

func detectConn(c *websocket.Conn, timeout time.Duration, pingInterval time.Duration, done <-chan struct{}) func() {
	ticker := time.NewTicker(timeout)
	var pingTicker *time.Ticker
	if pingInterval > 0 {
		pingTicker = time.NewTicker(pingInterval)
	}
	var pingC <-chan time.Time
	if pingTicker != nil {
		pingC = pingTicker.C
	}

	var lastResponse atomic.Int64
	touch := func() {
		lastResponse.Store(time.Now().UnixNano())
	}
	touch()

	c.SetPingHandler(func(pingData string) error {
		err := c.WriteControl(
			websocket.PongMessage,
			[]byte(pingData),
			time.Now().Add(websocketPongTimeout),
		)
		if err != nil {
			return err
		}
		touch()
		return nil
	})

	c.SetPongHandler(func(string) error {
		touch()
		return nil
	})

	go func() {
		defer ticker.Stop()
		if pingTicker != nil {
			defer pingTicker.Stop()
		}
		for {
			select {
			case <-done:
				return
			case <-pingC:
				if err := c.WriteControl(websocket.PingMessage, nil, time.Now().Add(websocketPingTimeout)); err != nil {
					_ = c.Close()
					return
				}
			case <-ticker.C:
			}
			last := time.Unix(0, lastResponse.Load())
			if time.Since(last) > timeout {
				_ = c.Close()
				return
			}
		}
	}()

	return touch
}

func feed(endpoint string, pingInterval time.Duration, handler func([]byte), errHandler func(error)) (*conn, error) {
	dialer := websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  45 * time.Second,
		EnableCompression: true,
	}

	ws, _, err := dialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(655350)

	c := &conn{
		doneC:  make(chan struct{}),
		stopC:  make(chan struct{}),
		writeC: make(chan writeReq),
	}

	go func() {
		for {
			select {
			case <-c.doneC:
				return
			case req := <-c.writeC:
				err := ws.WriteMessage(websocket.TextMessage, req.message)
				if err != nil {
					req.errC <- err
					_ = ws.Close()
					return
				}
				req.errC <- nil
			}
		}
	}()

	go func() {
		defer close(c.doneC)

		touch := detectConn(ws, websocketTimeout, pingInterval, c.doneC)

		var silent atomic.Bool
		go func() {
			select {
			case <-c.stopC:
				silent.Store(true)
			case <-c.doneC:
			}
			_ = ws.Close()
		}()

		for {
			_, message, readErr := ws.ReadMessage()
			if readErr != nil {
				if !silent.Load() {
					errHandler(readErr)
				}
				return
			}
			touch()
			handler(message)
		}
	}()

	return c, nil
}

func sleepRetry(duration time.Duration, done <-chan struct{}) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-done:
		return false
	case <-timer.C:
		return true
	}
}

func normalizePingInterval(intervals ...time.Duration) time.Duration {
	if len(intervals) == 0 {
		return 0
	}

	lo.Assert(len(intervals) == 1, "stream: too many ping intervals")
	lo.Assert(intervals[0] > 0, "stream: non-positive ping interval")
	return intervals[0]
}
