package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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
	pubChan   = make(chan outgoingMessage, bufferSize)
	pipeDrops uint64
	subReqCh  = make(chan string, bufferSize)

	handlersMu      sync.RWMutex
	handlersByTopic = make(map[string]chan *ReceivedMessage)

	kvClientMu sync.RWMutex
	kvClient   *redis.Client
	cliOnce    sync.Once
)

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

// Subscribe registers a message handler for a topic and returns a read-only channel.
func Subscribe(topic string) mo.Result[<-chan *ReceivedMessage] {
	if topic == "" {
		return mo.Err[<-chan *ReceivedMessage](errors.New("topic is empty"))
	}

	handlersMu.Lock()
	if _, exists := handlersByTopic[topic]; exists {
		handlersMu.Unlock()
		return mo.Err[<-chan *ReceivedMessage](errors.New("duplicated handler for topic"))
	}
	ch := make(chan *ReceivedMessage, bufferSize)
	handlersByTopic[topic] = ch
	handlersMu.Unlock()

	select {
	case subReqCh <- topic:
	default:
	}

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
		ctx, cancel := context.WithCancel(context.Background())
		setLifecycleCtx(ctx, cancel)

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
						slog.Warn("mresp connect failed", "error", err)
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
						slog.Warn("mresp close previous client failed", "error", err)
					}
				}

				slog.Info("mresp connected")

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
						slog.Warn("mresp producer exited", "error", err)
					}
					notifyExit("producer")
				}()
				go func() {
					defer wg.Done()
					if err := consume(workerCtx, client); err != nil && err != context.Canceled {
						slog.Warn("mresp consumer exited", "error", err)
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
						slog.Warn("mresp connection lost, restarting lifecycle", "worker", worker)
						lost = true
					case <-healthTicker.C:
						if err := healthCheck(context.Background(), client); err != nil {
							slog.Warn("mresp connection lost, restarting lifecycle", "error", err)
							lost = true
						}
					}
				}

				healthTicker.Stop()
				cancelRun()

				// Remove this client from shared state first, then close it to unblock workers.
				clearSharedClientIf(client)
				if err := client.Close(); err != nil {
					slog.Warn("mresp close client failed", "error", err)
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
						slog.Warn("mresp waiting for workers to stop")
					}
				}
			workersStopped:
				stopCtx, _ := getLifecycleCtx()
				if stopCtx == nil || stopCtx.Err() != nil {
					return
				}
			}
		}()
	})

	return nil
}

// Disconnect stops the bridge lifecycle and closes the shared client.
func Disconnect() {
	_, cancel := getLifecycleCtx()

	if cancel != nil {
		cancel()
	}
	client := getSharedClient()
	if client != nil {
		_ = client.Close()
		clearSharedClientIf(client)
	}
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

// Keys returns all keys matching the given pattern.
func Keys(pattern string) mo.Result[[]string] {
	if pattern == "" {
		return mo.Ok([]string{})
	}

	client := getSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	return mo.TupleToResult(client.Keys(ctx, pattern).Result())
}

// SearchIndex executes a query on a specific JSON attribute with the provided filter.
func SearchIndex(indexAttr string, filter x.Filter, desc bool) mo.Result[[]string] {
	if indexAttr == "" {
		return mo.Err[[]string](errors.New("index attribute is required"))
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

	cmd := client.Do(ctx, "SEARCHINDEX", indexAttr, filterJSON, order)
	res, err := cmd.StringSlice()
	if err != nil {
		return mo.Err[[]string](err)
	}

	return mo.Ok(res)
}

// SearchKey executes a query on matching keys with the provided filter.
func SearchKey(pattern string, filter x.Filter, desc bool) mo.Result[[]string] {
	if pattern == "" {
		return mo.Err[[]string](errors.New("pattern is required"))
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

	cmd := client.Do(ctx, "SEARCHKEY", pattern, filterJSON, order)
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
		slog.Warn("mresp send skipped, resp client is not connected")
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
			slog.Warn("mresp close client after failed health check failed", "error", closeErr)
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
				slog.Warn("mresp publish failed", "topic", msg.topic, "error", err)
				return err
			}
		}
	}
}

// consume subscribes by registered handler topics and dispatches messages.
func consume(ctx context.Context, client *redis.Client) error {
	consumerTopics := make(map[string]struct{})

	handlersMu.RLock()
	for topic := range handlersByTopic {
		if topic != "" {
			consumerTopics[topic] = struct{}{}
		}
	}
	handlersMu.RUnlock()

	var consumer *redis.PubSub
	var ch <-chan *redis.Message
	subscribed := make(map[string]struct{}, len(consumerTopics))

	subscribeTopic := func(topic string) error {
		if topic == "" {
			return nil
		}
		if _, exists := subscribed[topic]; exists {
			return nil
		}

		if consumer == nil {
			consumer = client.Subscribe(ctx, topic)
			if _, err := consumer.Receive(ctx); err != nil {
				slog.Warn("mresp subscribe receive error", "error", err)
				return err
			}
			ch = consumer.Channel()
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
				slog.Warn("mresp close consumer failed", "error", err)
			}
		}

		// Close all registered handler channels when the consumer exits
		handlersMu.Lock()
		for _, ch := range handlersByTopic {
			if ch != nil {
				close(ch)
			}
		}
		// Clear the map so any new Connect/lifecycle start starts fresh
		handlersByTopic = make(map[string]chan *ReceivedMessage)
		handlersMu.Unlock()
	}()

	for topic := range consumerTopics {
		if err := subscribeTopic(topic); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case topic := <-subReqCh:
			if err := subscribeTopic(topic); err != nil {
				return err
			}
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("subscribe channel closed")
			}
			handlersMu.RLock()
			h := handlersByTopic[msg.Channel]
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
