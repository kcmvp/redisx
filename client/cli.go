package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kcmvp/redisx/internal"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/internal/xcmd"
	"github.com/kcmvp/redisx/x"
	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
	"github.com/samber/mo"
)

const (
	bufferSize     = 200
	dialTimeout    = 3 * time.Second
	reconnectEvery = 5 * time.Second
)

// ReceivedMessage is a transport-agnostic message exposed to callers.
type ReceivedMessage struct {
	Channel string
	Pattern string
	Payload string
}

type outgoingMessage struct {
	topic   string
	payload string
}

var (
	pubChan        = make(chan outgoingMessage, bufferSize)
	pipeDrops      uint64
	subscribeReqCh = make(chan string, bufferSize)

	handlersMu        sync.RWMutex
	deliveryChByTopic = make(map[string]chan *ReceivedMessage)

	kvClientMu sync.RWMutex
	kvClient   *redis.Client
	cliOnce    sync.Once

	signalNotifyFn = signal.Notify
	signalStopFn   = signal.Stop
)

// resetHandlers closes all registered subscription channels and clears the
// topic registry. This is reserved for explicit full shutdown paths such as
// Disconnect, not for transient consumer restarts during reconnect.
func resetHandlers() {
	handlersMu.Lock()
	for _, ch := range deliveryChByTopic {
		if ch != nil {
			close(ch)
		}
	}
	deliveryChByTopic = make(map[string]chan *ReceivedMessage)
	handlersMu.Unlock()
}

func setSharedClient(client *redis.Client) *redis.Client {
	kvClientMu.Lock()
	prev := kvClient
	kvClient = client
	kvClientMu.Unlock()
	return prev
}

func getSharedClient() *redis.Client {
	kvClientMu.RLock()
	client := kvClient
	kvClientMu.RUnlock()
	return client
}

func clearSharedClientIf(target *redis.Client) {
	kvClientMu.Lock()
	if kvClient == target {
		kvClient = nil
	}
	kvClientMu.Unlock()
}

// Subscribe registers one local delivery channel for a topic and returns that
// read-only channel to the caller.
//
// Internally it does two things:
//   - stores the topic -> channel mapping in deliveryChByTopic so incoming
//     messages can be dispatched locally
//   - enqueues a request on subscribeReqCh so the background consumer can
//     sync the remote SUBSCRIBE state for newly added topics
//
// Callers consume messages from the returned channel. Duplicate topic
// registration is rejected.
func Subscribe(topic string) mo.Result[<-chan *ReceivedMessage] {
	if topic == "" {
		return mo.Err[<-chan *ReceivedMessage](errors.New("topic is empty"))
	}

	handlersMu.Lock()
	if _, exists := deliveryChByTopic[topic]; exists {
		handlersMu.Unlock()
		return mo.Err[<-chan *ReceivedMessage](errors.New("duplicated handler for topic"))
	}
	ch := make(chan *ReceivedMessage, bufferSize)
	deliveryChByTopic[topic] = ch
	handlersMu.Unlock()

	select {
	case subscribeReqCh <- topic:
	default:
		slog.Warn(
			"subscribe request dropped before remote subscribe",
			"topic", topic,
			"queue_len", len(subscribeReqCh),
			"queue_cap", cap(subscribeReqCh),
		)
	}

	// Potential issue: if the subscribe request is dropped above, the local
	// topic -> channel mapping still exists, but the remote SUBSCRIBE is not
	// issued for the current consumer lifecycle.
	return mo.Ok[<-chan *ReceivedMessage](ch)
}

var (
	lifecycleCtx atomic.Value
)

func setLifecycleCtx(ctx context.Context, cancel context.CancelFunc) {
	lifecycleCtx.Store(lo.T2(ctx, cancel))
}

func getLifecycleCtx() (context.Context, context.CancelFunc) {
	val := lifecycleCtx.Load()
	if val == nil {
		return nil, nil
	}
	t := val.(lo.Tuple2[context.Context, context.CancelFunc])
	return t.A, t.B
}

// Connect starts the bridge lifecycle.
func Connect(respAddr, authKey string) error {
	if authKey == "" {
		return errors.New("auth key is empty")
	}

	cliOnce.Do(func() {
		ctx, cancelRoot := context.WithCancel(context.Background())
		sigCh := make(chan os.Signal, 1)
		signalNotifyFn(sigCh, os.Interrupt, syscall.SIGTERM)
		cancel := func() {
			signalStopFn(sigCh)
			cancelRoot()
		}
		setLifecycleCtx(ctx, cancel)

		go func() {
			select {
			case sig := <-sigCh:
				if sig != nil {
					slog.Info("redisx client caught shutdown signal", "signal", sig.String())
				} else {
					slog.Info("redisx client caught shutdown signal")
				}
				cancel()
			case <-ctx.Done():
				signalStopFn(sigCh)
			}
		}()

		go func() {
			for {
				var err error
				var client *redis.Client
				_, _, _ = lo.AttemptWhileWithDelay(-1, reconnectEvery, func(i int, duration time.Duration) (error, bool) {
					runCtx, _ := getLifecycleCtx()
					if runCtx == nil {
						return nil, false
					}

					select {
					case <-runCtx.Done():
						return nil, false
					default:
					}
					if client, err = connect(respAddr, authKey); err != nil {
						slog.Warn("redisx connect failed", "error", err)
						return err, true
					}
					return nil, false
				})

				runCtx, _ := getLifecycleCtx()
				if runCtx == nil || runCtx.Err() != nil {
					return
				}

				if prev := setSharedClient(client); prev != nil && prev != client {
					if err := prev.Close(); err != nil {
						slog.Warn("redisx close previous client failed", "error", err)
					}
				}

				slog.Info("redisx connected")

				rootCtx, _ := getLifecycleCtx()
				workerCtx, cancelRun := context.WithCancel(rootCtx)

				firstExit := make(chan string, 1)
				var wg sync.WaitGroup
				notifyExit := func(worker string) {
					select {
					case firstExit <- worker:
					default:
					}
				}

				wg.Add(2)
				go func() {
					defer wg.Done()
					if err := produce(workerCtx, client); err != nil && err != context.Canceled {
						slog.Warn("redisx producer exited", "error", err)
					}
					notifyExit("producer")
				}()
				go func() {
					defer wg.Done()
					if err := consume(workerCtx, client); err != nil && err != context.Canceled {
						slog.Warn("redisx consumer exited", "error", err)
					}
					notifyExit("consumer")
				}()

				healthTicker := time.NewTicker(5 * time.Second)
				lost := false

				for !lost {
					select {
					case <-rootCtx.Done():
						lost = true
					case worker := <-firstExit:
						slog.Warn("redisx connection lost, restarting lifecycle", "worker", worker)
						lost = true
					case <-healthTicker.C:
						if err := healthCheck(context.Background(), client); err != nil {
							slog.Warn("redisx connection lost, restarting lifecycle", "error", err)
							lost = true
						}
					}
				}

				healthTicker.Stop()
				cancelRun()

				// Remove this client from shared state first, then close it to unblock workers.
				clearSharedClientIf(client)
				if err := client.Close(); err != nil {
					slog.Warn("redisx close client failed", "error", err)
				}

				waitDone := make(chan struct{})
				go func() {
					wg.Wait()
					close(waitDone)
				}()

				waitTicker := time.NewTicker(1 * time.Second)
				for {
					select {
					case <-waitDone:
						waitTicker.Stop()
						goto workersStopped
					case <-waitTicker.C:
						slog.Warn("redisx waiting for workers to stop")
					}
				}
			workersStopped:
				stopCtx, _ := getLifecycleCtx()
				if stopCtx == nil || stopCtx.Err() != nil {
					slog.Info("redisx client stopped")
					return
				}
			}
		}()
	})

	return nil
}

// ConnectEmbed starts the bridge lifecycle with the shared in-process auth key.
func ConnectEmbed(respAddr string) error {
	return Connect(respAddr, internal.AuthKey())
}

// disconnect stops the current bridge lifecycle and closes the shared client.
//
// Note: this is currently treated as a best-effort shutdown helper for tests
// and examples. It does not reset cliOnce, so calling Connect again after
// disconnect does not start a brand new lifecycle.
//
// If production usage later requires a full stop-then-restart flow, revisit
// the lifecycle state model here instead of relying on the current behavior.
func disconnect() {
	_, cancel := getLifecycleCtx()

	if cancel != nil {
		cancel()
	}
	client := getSharedClient()
	if client != nil {
		_ = client.Close()
		clearSharedClientIf(client)
	}
	resetHandlers()
}

// Get retrieves the value for the given key using the shared resp client.
func Get(key string) (string, error) {
	if key == "" {
		return "", nil
	}
	client := getSharedClient()
	if client == nil {
		return "", errors.New("resp client is not connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	val, err := client.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return val, nil
}

// Set stores a value for the given key using the shared resp client.
func SetWithTTL(key, value string, ttl time.Duration) error {
	if key == "" {
		return nil
	}

	client := getSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	return client.Set(ctx, key, value, ttl).Err()
}

// Set stores a value for the given key using the shared resp client.
func Set(key, value string) error {
	if key == "" {
		return nil
	}

	client := getSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	return client.Set(ctx, key, value, 0).Err()
}

// SetNX stores a value for the given key only if the key does not exist.
func SetNX(key, value string) (bool, error) {
	if key == "" {
		return false, nil
	}

	client := getSharedClient()
	if client == nil {
		return false, errors.New("resp client is not connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	res, err := client.Do(ctx, "SETNX", key, value).Int()
	return res == 1, err
}

// SetNXWithTTL stores a value for the given key only if the key does not
// exist, and applies the provided TTL when it is positive.
func SetNXWithTTL(key, value string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return SetNX(key, value)
	}

	if key == "" {
		return false, nil
	}

	client := getSharedClient()
	if client == nil {
		return false, errors.New("resp client is not connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	return client.SetNX(ctx, key, value, ttl).Result()
}

// Delete removes the specified key using the shared resp client.
func Delete(key string) (bool, error) {
	if key == "" {
		return false, nil
	}

	client := getSharedClient()
	if client == nil {
		return false, errors.New("resp client is not connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	res, err := client.Del(ctx, key).Result()
	return res > 0, err
}

// Keys returns all keys matching the given key pattern.
func Keys(keyPattern string) mo.Result[[]string] {
	if keyPattern == "" {
		return mo.Ok([]string{})
	}

	client := getSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	return mo.TupleToResult(client.Keys(ctx, keyPattern).Result())
}

// SearchIndex executes the RESP X command below on the shared connection:
//
//	SEARCHINDEX <index_name> <key_pattern> <json_filter> [ASC|DESC]
//
// The key pattern follows BuntDB glob matching, and the filter is serialized
// from the provided x.Filter into the MongoDB-style JSON syntax understood by
// the server.
//
// Example:
//
//	res := client.SearchIndex(
//		"idx_age",
//		"user:*",
//		x.And(
//			x.Gte("age", 18),
//			x.Eq("status", "active"),
//		),
//		false,
//	)
//	users := res.MustGet()
func SearchIndex(indexName string, keyPattern string, filter x.Filter, desc bool) mo.Result[[]string] {
	if indexName == "" {
		return mo.Err[[]string](errors.New("index name is required"))
	}
	if keyPattern == "" {
		return mo.Err[[]string](errors.New("key pattern is required"))
	}

	client := getSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	var filterJSON string
	if filter != nil {
		b, err := filter.MarshalJSON()
		if err != nil {
			return mo.Err[[]string](fmt.Errorf("failed to serialize filter: %w", err))
		}
		filterJSON = string(b)
	} else {
		filterJSON = "{}" // empty filter
	}

	order := "ASC"
	if desc {
		order = "DESC"
	}

	cmd := client.Do(ctx, proto.CmdSearchIndex, indexName, keyPattern, filterJSON, order)
	res, err := cmd.StringSlice()
	if err != nil {
		return mo.Err[[]string](err)
	}

	return mo.Ok(res)
}

// SearchKey executes the RESP X command below on the shared connection:
//
//	SEARCHKEY <key_pattern> <json_filter> [ASC|DESC]
//
// The key pattern follows BuntDB glob matching, and the filter is serialized from
// x.Filter into the server's MongoDB-style JSON query format.
//
// Example:
//
//	res := client.SearchKey(
//		"user:*",
//		x.Eq("region", "us"),
//		true,
//	)
//	users := res.MustGet()
func SearchKey(keyPattern string, filter x.Filter, desc bool) mo.Result[[]string] {
	if keyPattern == "" {
		return mo.Err[[]string](errors.New("key pattern is required"))
	}

	client := getSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	var filterJSON string
	if filter != nil {
		b, err := filter.MarshalJSON()
		if err != nil {
			return mo.Err[[]string](fmt.Errorf("failed to serialize filter: %w", err))
		}
		filterJSON = string(b)
	} else {
		filterJSON = "{}" // empty filter
	}

	order := "ASC"
	if desc {
		order = "DESC"
	}

	cmd := client.Do(ctx, proto.CmdSearchKey, keyPattern, filterJSON, order)
	res, err := cmd.StringSlice()
	if err != nil {
		return mo.Err[[]string](err)
	}

	return mo.Ok(res)
}

// Update executes the RESP X command below on the shared connection:
//
//	UPDATE <key_pattern> <json_filter> <update_json>
//
// The filter is serialized from x.Filter and the update payload is built from
// x.Mutation values, so callers do not need to handwrite update JSON.
//
// Example:
//
//	res := client.Update(
//		"user:*",
//		x.Eq("status", "pending"),
//		x.Set("status", "active"),
//		x.Set("verified", true),
//	)
//	updatedKeys := res.MustGet()
func Update(keyPattern string, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if keyPattern == "" {
		return mo.Err[[]string](errors.New("key pattern is required"))
	}
	if len(values) == 0 {
		return mo.Err[[]string](errors.New("no update values provided"))
	}

	client := getSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	filterJSON := "{}"
	if filter != nil {
		b, err := filter.MarshalJSON()
		if err != nil {
			return mo.Err[[]string](fmt.Errorf("failed to serialize filter: %w", err))
		}
		filterJSON = string(b)
	}

	updateJSON, err := xcmd.MarshalUpdate(values...)
	if err != nil {
		return mo.Err[[]string](fmt.Errorf("failed to serialize updates: %w", err))
	}

	cmd := client.Do(ctx, proto.CmdUpdate, keyPattern, filterJSON, string(updateJSON))
	res, err := cmd.StringSlice()
	if err != nil {
		return mo.Err[[]string](err)
	}

	return mo.Ok(res)
}

// Auth authenticates the shared resp client connection with the given key.
func Auth(authKey string) error {
	if authKey == "" {
		return errors.New("auth key is empty")
	}

	client := getSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	return client.Do(ctx, "AUTH", authKey).Err()
}

// Publish enqueues a message for publishing to a specific topic.
func Publish(topic, msg string) bool {
	if topic == "" {
		return false
	}

	if getSharedClient() == nil {
		slog.Warn("redisx send skipped, resp client is not connected")
		return false
	}

	select {
	case pubChan <- outgoingMessage{topic: topic, payload: msg}:
		return true
	default:
		cnt := atomic.AddUint64(&pipeDrops, 1)
		if cnt%100 == 0 {
			slog.Warn("pubChan is full, message dropped", "total_drops", cnt)
		}
		return false
	}
}

// healthCheck checks if a redis client is alive by sending a Ping with timeout.
// Returns error if the ping fails or times out.
func healthCheck(ctx context.Context, client *redis.Client) error {
	ctxPing, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	return client.Ping(ctxPing).Err()
}

// connect attempts to create and verify a single Resp client connection.
// Returns the connected client on success, or nil + error on failure.
// Caller must handle cleanup (Close) if an error is returned.
func connect(respAddr, authKey string) (*redis.Client, error) {
	if authKey == "" {
		return nil, errors.New("auth key is empty")
	}

	options := &redis.Options{
		Addr:        respAddr,
		DialTimeout: dialTimeout,
		ReadTimeout: 0,
		Protocol:    2,
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			return cn.Do(ctx, "AUTH", authKey).Err()
		},
	}

	client := redis.NewClient(options)
	if err := healthCheck(context.Background(), client); err != nil {
		if closeErr := client.Close(); closeErr != nil {
			slog.Warn("redisx close client after failed health check failed", "error", closeErr)
		}
		return nil, err
	}
	return client, nil
}

// produce publishes messages from pubChan using the provided client until ctx is done.
func produce(ctx context.Context, client *redis.Client) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-pubChan:
			if msg.topic == "" {
				continue
			}
			if err := client.Publish(ctx, msg.topic, msg.payload).Err(); err != nil {
				slog.Warn("redisx publish failed", "topic", msg.topic, "error", err)
				return err
			}
		}
	}
}

// consume rebuilds remote subscriptions from deliveryChByTopic, applies
// incremental subscription requests from subscribeReqCh, and dispatches
// incoming messages to the registered local handlers.
func consume(ctx context.Context, client *redis.Client) error {
	consumerTopics := make(map[string]struct{})

	handlersMu.RLock()
	for topic := range deliveryChByTopic {
		if topic != "" {
			consumerTopics[topic] = struct{}{}
		}
	}
	handlersMu.RUnlock()

	var consumer *redis.PubSub
	var remoteMsgCh <-chan *redis.Message
	subscribed := make(map[string]struct{}, len(consumerTopics))

	remoteSubscription := func(topic string) error {
		if topic == "" {
			return nil
		}
		if _, exists := subscribed[topic]; exists {
			return nil
		}

		if consumer == nil {
			consumer = client.Subscribe(ctx, topic)
			if _, err := consumer.Receive(ctx); err != nil {
				slog.Warn("redisx subscribe receive error", "error", err)
				return err
			}
			remoteMsgCh = consumer.Channel()
			subscribed[topic] = struct{}{}
			return nil
		}

		if err := consumer.Subscribe(ctx, topic); err != nil {
			return err
		}
		subscribed[topic] = struct{}{}
		return nil
	}

	defer func() {
		if consumer != nil {
			if err := consumer.Close(); err != nil {
				slog.Warn("redisx close consumer failed", "error", err)
			}
		}
	}()

	// Rebuild all already-registered subscriptions on each fresh consumer
	// lifecycle, including reconnects.
	for topic := range consumerTopics {
		if err := remoteSubscription(topic); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		// Handle newly registered subscriptions after this consumer started.
		case topic := <-subscribeReqCh:
			if err := remoteSubscription(topic); err != nil {
				return err
			}
		case msg, ok := <-remoteMsgCh:
			if !ok {
				return fmt.Errorf("subscribe channel closed")
			}
			handlersMu.RLock()
			h := deliveryChByTopic[msg.Channel]
			handlersMu.RUnlock()
			if h == nil {
				continue
			}
			select {
			case h <- &ReceivedMessage{Channel: msg.Channel, Pattern: msg.Pattern, Payload: msg.Payload}:
			default:
				slog.Warn("message handler channel full, message dropped", "topic", msg.Channel)
			}
		}
	}
}
