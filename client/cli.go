package client

// Write Hook Subsystem — safety-net composition policy:
//
//   - Double recover: stack-safe, no side effects (innermost-first).
//   - Double timeout:
//     User timeout < framework timeout → no duplicate log.
//     User timeout > framework timeout → duplicate slog. Fix: use per-hook timeouts + globally SetHookTimeout(0).
//   - AbortHook timeout/panic = fail-closed (ABORT). Non-negotiable security default.
//   - Observer*Hook timeout/panic = fail-open (LOG ONLY). Never impact write.
//
// Execution order (synchronous, all Before hooks lifecycle complete before Set returns):
//  1. AbortHook (fail-closed) → abort on any error/panic/timeout
//  2. TransformHook (fail-closed) → abort on any error/panic/timeout; chains in registration order
//  3. ObserverHook (fail-open Before) → sees post-Transform value; only log on panic/timeout
//  4. Actual Redis SET / SETNX / ...
//  5. ObserverAfterHook (fail-open After) → receives final value + writeErr; only log on panic/timeout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kcmvp/redisx/internal"
	"github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/internal/respconn"
	"github.com/kcmvp/redisx/x"
	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
	"github.com/samber/mo"
)

const (
	bufferSize     = 200
	dialTimeout    = 3 * time.Second
	reconnectEvery = 5 * time.Second

	defaultHookTimeout = 100 * time.Millisecond
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

type HookID uint64

type AbortHook func(key string, value []byte) error
type TransformHook func(key string, value []byte) (newValue []byte, err error)
type ObserverHook func(key string, value []byte)
type ObserverAfterHook func(key string, value []byte, committed bool, writeErr error)

type registeredAbortHook struct {
	id HookID
	h  AbortHook
}
type registeredTransformHook struct {
	id HookID
	h  TransformHook
}
type registeredObserverHook struct {
	id HookID
	h  ObserverHook
}
type registeredObserverAfterHook struct {
	id HookID
	h  ObserverAfterHook
}

type hooksRegistry struct {
	aborts     []registeredAbortHook
	transforms []registeredTransformHook
	observers  []registeredObserverHook
	afters     []registeredObserverAfterHook
}

var (
	hooksMu      sync.RWMutex
	hooksStore   atomic.Pointer[hooksRegistry]
	hooksNextID  HookID
	hooksTimeout atomic.Int64
)

func init() {
	hooksStore.Store(nil)
	hooksTimeout.Store(int64(defaultHookTimeout))
}

func SetHookTimeout(d time.Duration) {
	hooksTimeout.Store(int64(d))
}

func getHookTimeout() time.Duration {
	return time.Duration(hooksTimeout.Load())
}

func snapshotHooks() *hooksRegistry {
	return hooksStore.Load()
}

func cloneRegistry(reg *hooksRegistry) *hooksRegistry {
	n := &hooksRegistry{}
	if reg != nil {
		n.aborts = append([]registeredAbortHook(nil), reg.aborts...)
		n.transforms = append([]registeredTransformHook(nil), reg.transforms...)
		n.observers = append([]registeredObserverHook(nil), reg.observers...)
		n.afters = append([]registeredObserverAfterHook(nil), reg.afters...)
	}
	return n
}

func nextHookID() HookID {
	for {
		cur := atomic.LoadUint64((*uint64)(&hooksNextID))
		nxt := cur + 1
		if nxt == 0 {
			nxt = 1
		}
		if atomic.CompareAndSwapUint64((*uint64)(&hooksNextID), cur, nxt) {
			return HookID(nxt)
		}
	}
}

func AddAbortHook(h AbortHook) HookID {
	hooksMu.Lock()
	defer hooksMu.Unlock()
	id := nextHookID()
	reg := cloneRegistry(hooksStore.Load())
	reg.aborts = append(reg.aborts, registeredAbortHook{id: id, h: h})
	hooksStore.Store(reg)
	return id
}

func AddTransformHook(h TransformHook) HookID {
	hooksMu.Lock()
	defer hooksMu.Unlock()
	id := nextHookID()
	reg := cloneRegistry(hooksStore.Load())
	reg.transforms = append(reg.transforms, registeredTransformHook{id: id, h: h})
	hooksStore.Store(reg)
	return id
}

func AddObserverHook(h ObserverHook) HookID {
	hooksMu.Lock()
	defer hooksMu.Unlock()
	id := nextHookID()
	reg := cloneRegistry(hooksStore.Load())
	reg.observers = append(reg.observers, registeredObserverHook{id: id, h: h})
	hooksStore.Store(reg)
	return id
}

func AddObserverAfterHook(h ObserverAfterHook) HookID {
	hooksMu.Lock()
	defer hooksMu.Unlock()
	id := nextHookID()
	reg := cloneRegistry(hooksStore.Load())
	reg.afters = append(reg.afters, registeredObserverAfterHook{id: id, h: h})
	hooksStore.Store(reg)
	return id
}

func RemoveHook(id HookID) {
	if id == 0 {
		return
	}
	hooksMu.Lock()
	defer hooksMu.Unlock()
	reg := hooksStore.Load()
	if reg == nil {
		return
	}
	n := &hooksRegistry{}
	for _, h := range reg.aborts {
		if h.id != id {
			n.aborts = append(n.aborts, h)
		}
	}
	for _, h := range reg.transforms {
		if h.id != id {
			n.transforms = append(n.transforms, h)
		}
	}
	for _, h := range reg.observers {
		if h.id != id {
			n.observers = append(n.observers, h)
		}
	}
	for _, h := range reg.afters {
		if h.id != id {
			n.afters = append(n.afters, h)
		}
	}
	if len(n.aborts) == 0 && len(n.transforms) == 0 && len(n.observers) == 0 && len(n.afters) == 0 {
		hooksStore.Store(nil)
		return
	}
	hooksStore.Store(n)
}

func resetHooks() {
	hooksMu.Lock()
	defer hooksMu.Unlock()
	hooksStore.Store(nil)
	atomic.StoreUint64((*uint64)(&hooksNextID), 0)
	hooksTimeout.Store(int64(defaultHookTimeout))
}

func runHookWithSafety(label string, fn func() error) error {
	d := getHookTimeout()
	if d <= 0 {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Warn("write hook panic", "hook", label, "panic", r, "stack", stackStr())
					err = fmt.Errorf("hook %s panic: %v", label, r)
				}
			}()
			err = fn()
		}()
		return err
	}
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Warn("write hook panic", "hook", label, "panic", r, "stack", stackStr())
				done <- fmt.Errorf("hook %s panic: %v", label, r)
			}
		}()
		done <- fn()
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		slog.Warn("write hook timeout", "hook", label, "timeout", d)
		return fmt.Errorf("hook %s timeout after %s", label, d)
	case err := <-done:
		return err
	}
}

func stackStr() string {
	buf := make([]byte, 4096)
	n := runtimeStack(buf, false)
	return string(buf[:n])
}

var runtimeStack = runtime.Stack

func runBeforeHooks(reg *hooksRegistry, key string, value []byte) ([]byte, error) {
	for i, rh := range reg.aborts {
		h := rh.h
		if err := runHookWithSafety(fmt.Sprintf("Abort#%d", i), func() error {
			return h(key, value)
		}); err != nil {
			return nil, err
		}
	}
	cur := value
	for i, rh := range reg.transforms {
		h := rh.h
		var nv []byte
		herr := runHookWithSafety(fmt.Sprintf("Transform#%d", i), func() error {
			var innerErr error
			nv, innerErr = h(key, cur)
			return innerErr
		})
		if herr != nil {
			return nil, herr
		}
		if nv == nil {
			return nil, fmt.Errorf("hook Transform#%d returned nil bytes without error", i)
		}
		cur = nv
	}
	for i, rh := range reg.observers {
		h := rh.h
		_ = runHookWithSafety(fmt.Sprintf("Observer#%d", i), func() error {
			h(key, cur)
			return nil
		})
	}
	return cur, nil
}

func runAfterHooks(reg *hooksRegistry, key string, value []byte, committed bool, writeErr error) {
	for i, rh := range reg.afters {
		h := rh.h
		_ = runHookWithSafety(fmt.Sprintf("ObserverAfter#%d", i), func() error {
			h(key, value, committed, writeErr)
			return nil
		})
	}
}

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
		notifyFn := signalNotifyFn
		stopFn := signalStopFn
		notifyFn(sigCh, os.Interrupt, syscall.SIGTERM)
		cancel := func() {
			stopFn(sigCh)
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
				stopFn(sigCh)
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

// ———  Typesafe API (top-most as user requested)  ———

func Get[D x.Document](key string) (D, error) {
	var zero D
	raw, err := GetRaw(x.StorageKeyValue[D](key))
	if err != nil {
		return zero, err
	}
	return D(raw), nil
}

func Set[D x.Document](d D) error {
	key, err := x.StorageKey(d)
	if err != nil {
		return err
	}
	return SetWithTTLRaw(key, d.RawJSON(), d.TTL())
}

func SetNX[D x.Document](d D) (bool, error) {
	key, err := x.StorageKey(d)
	if err != nil {
		return false, err
	}
	return SetNXWithTTLRaw(key, d.RawJSON(), d.TTL())
}

func Delete[D x.Document](d D) (bool, error) {
	key, err := x.StorageKey(d)
	if err != nil {
		return false, err
	}
	return DeleteRaw(key)
}

func Keys[D x.Document](keyPattern string) mo.Result[[]string] {
	fullKeyPattern, err := x.ValidateKeyPattern[D](keyPattern)
	if err != nil {
		return mo.Err[[]string](err)
	}
	return KeysRaw(fullKeyPattern)
}

func SearchIndex[D x.Document](idxName string, scopedKR x.KeyRange, filter x.Filter, desc bool) mo.Result[[]D] {
	if scopedKR == nil {
		return mo.Err[[]D](errors.New("key range is required"))
	}
	fullIdxName, err := x.ValidateIdxName[D](idxName)
	if err != nil {
		return mo.Err[[]D](err)
	}

	fullKR, err := x.ScopeKeyRange[D](scopedKR)
	if err != nil {
		return mo.Err[[]D](err)
	}

	res := SearchIndexRaw(fullIdxName, fullKR, filter, desc)
	if res.IsError() {
		return mo.Err[[]D](res.Error())
	}

	raws := res.MustGet()
	out := make([]D, 0, len(raws))
	for _, raw := range raws {
		out = append(out, D(raw))
	}
	return mo.Ok(out)
}

func SearchKey[D x.Document](scopedKR x.KeyRange, filter x.Filter, desc bool) mo.Result[[]D] {
	if scopedKR == nil {
		return mo.Err[[]D](errors.New("key range is required"))
	}
	fullKR, err := x.ScopeKeyRange[D](scopedKR)
	if err != nil {
		return mo.Err[[]D](err)
	}

	res := SearchKeyRaw(fullKR, filter, desc)
	if res.IsError() {
		return mo.Err[[]D](res.Error())
	}

	raws := res.MustGet()
	out := make([]D, 0, len(raws))
	for _, raw := range raws {
		out = append(out, D(raw))
	}
	return mo.Ok(out)
}

func Update[D x.Document](scopedKR x.KeyRange, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if scopedKR == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	fullKR, err := x.ScopeKeyRange[D](scopedKR)
	if err != nil {
		return mo.Err[[]string](err)
	}
	return UpdateRaw(fullKR, filter, values...)
}

func RegisterSchema[T x.Schema]() error {
	var zero T
	type raw struct {
		Namespace string        `json:"namespace"`
		Mem       bool          `json:"mem"`
		KeyAttrs  []string      `json:"key_attrs"`
		TTL       time.Duration `json:"ttl_ns"`
	}
	r := raw{
		Namespace: zero.Namespace(),
		Mem:       zero.Mem(),
		KeyAttrs:  append([]string(nil), zero.KeyAttrs()...),
		TTL:       zero.TTL(),
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return RegisterSchemaFromJSON(string(b))
}

func RegisterIndex[D x.Document](logical string, keyPattern string, jsonPaths ...string) error {
	if logical == "" {
		return errors.New("logical index name is empty")
	}
	if keyPattern == "" {
		return errors.New("key pattern is empty")
	}
	if len(jsonPaths) == 0 {
		return errors.New("at least one jsonPath is required")
	}
	var zero D
	ns := zero.Namespace()
	mem := zero.Mem()
	if err := naming.ValidateDocLogicalNamespace(ns); err != nil {
		return err
	}
	storageNs := naming.BuildStorageNs(ns, mem)
	paths := make([]string, 0, len(jsonPaths))
	for _, p := range jsonPaths {
		if p == "" {
			return errors.New("jsonPaths contains empty path")
		}
		paths = append(paths, strings.ReplaceAll(p, ".", "_"))
	}
	type raw struct {
		OwnerNs    string   `json:"owner_ns"`
		OwnerMem   bool     `json:"owner_mem,omitempty"`
		Logical    string   `json:"logical"`
		Paths      []string `json:"paths"`
		KeyPattern string   `json:"key_pattern"`
	}
	spec := raw{
		OwnerNs:    storageNs,
		OwnerMem:   mem,
		Logical:    strings.ToLower(logical),
		Paths:      paths,
		KeyPattern: keyPattern,
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("RegisterIndex build JSON: %w", err)
	}
	return RegisterIndexFromJSON(string(b))
}

func DropSchema[T x.Schema]() error {
	var zero T
	ns := zero.Namespace()
	if ns == "" {
		return errors.New("schema returned empty namespace")
	}
	return DropSchemaRaw(ns)
}

func DropIndex[D x.Document](logical string) error {
	if logical == "" {
		return errors.New("logical index name is empty")
	}
	var zero D
	ns := zero.Namespace()
	mem := zero.Mem()
	if err := naming.ValidateDocLogicalNamespace(ns); err != nil {
		return err
	}
	storageNs := naming.BuildStorageNs(ns, mem)
	return DropIndexRaw(storageNs, strings.ToLower(logical))
}

// ———  Raw API  ———

// Get retrieves the value for the given key using the shared resp client.
func GetRaw(key string) (string, error) {
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

// SetWithTTLRaw stores a value for the given key using the shared resp client, applying a positive TTL.
func SetWithTTLRaw(key, value string, ttl time.Duration) error {
	if key == "" {
		return nil
	}

	client := getSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}

	reg := hooksStore.Load()
	finalVal := value
	var valBytes []byte
	if reg != nil {
		valBytes = []byte(value)
		transformed, herr := runBeforeHooks(reg, key, valBytes)
		if herr != nil {
			return herr
		}
		finalVal = string(transformed)
		valBytes = transformed
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	werr := client.Set(ctx, key, finalVal, ttl).Err()
	committed := werr == nil

	if reg != nil {
		runAfterHooks(reg, key, valBytes, committed, werr)
	}
	return werr
}

// SetRaw stores a value for the given key using the shared resp client with no TTL.
func SetRaw(key, value string) error {
	return SetWithTTLRaw(key, value, 0)
}

// SetNXRaw stores a value for the given key only if the key does not exist.
func SetNXRaw(key, value string) (bool, error) {
	if key == "" {
		return false, nil
	}

	client := getSharedClient()
	if client == nil {
		return false, errors.New("resp client is not connected")
	}

	reg := hooksStore.Load()
	finalVal := value
	var valBytes []byte
	if reg != nil {
		valBytes = []byte(value)
		transformed, herr := runBeforeHooks(reg, key, valBytes)
		if herr != nil {
			return false, herr
		}
		finalVal = string(transformed)
		valBytes = transformed
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	res, werr := client.Do(ctx, "SETNX", key, finalVal).Int()
	ok := res == 1
	committed := ok && werr == nil

	if reg != nil {
		runAfterHooks(reg, key, valBytes, committed, werr)
	}
	return ok, werr
}

// SetNXWithTTLRaw stores a value for the given key only if the key does not
// exist, and applies the provided TTL when it is positive.
func SetNXWithTTLRaw(key, value string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return SetNXRaw(key, value)
	}

	if key == "" {
		return false, nil
	}

	client := getSharedClient()
	if client == nil {
		return false, errors.New("resp client is not connected")
	}

	reg := hooksStore.Load()
	finalVal := value
	var valBytes []byte
	if reg != nil {
		valBytes = []byte(value)
		transformed, herr := runBeforeHooks(reg, key, valBytes)
		if herr != nil {
			return false, herr
		}
		finalVal = string(transformed)
		valBytes = transformed
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	ok, werr := client.SetNX(ctx, key, finalVal, ttl).Result()
	committed := ok && werr == nil

	if reg != nil {
		runAfterHooks(reg, key, valBytes, committed, werr)
	}
	return ok, werr
}

// DeleteRaw removes the specified key using the shared resp client.
func DeleteRaw(key string) (bool, error) {
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

// KeysRaw returns all keys matching the given key pattern.
func KeysRaw(keyPattern string) mo.Result[[]string] {
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
//	SEARCHINDEX <index_name> <keyrange_json> <json_filter> [ASC|DESC] [LIMIT count]
//
// The key range is a sealed x.KeyRange value (6 ctors: KeysBt / KeysGt /
// KeysGte / KeysLt / KeysLte / KeysPattern) serialized via MarshalJSON.
// The optional truncation limit is CARRIED ON the KeyRange object itself via
//
//	kr.Limit(count)
//
// (the function signature intentionally does NOT expose a separate limit
// parameter, matching SearchKey shape).
//
// Example:
//
//	res := client.SearchIndexRaw(
//		"idx_age",
//		x.KeysBt("user:engineering:0100", "user:engineering:0200").Limit(50),
//		x.And(
//			x.Gte("age", 18),
//			x.Eq("status", "active"),
//		),
//		false,
//	)
//	users := res.MustGet()
func SearchIndexRaw(indexName string, kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if indexName == "" {
		return mo.Err[[]string](errors.New("index name is required"))
	}
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}

	client := getSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	krBytes, krErr := kr.MarshalJSON()
	if krErr != nil {
		return mo.Err[[]string](fmt.Errorf("failed to serialize key range: %w", krErr))
	}

	var filterJSON string
	if filter != nil {
		b, err := filter.MarshalJSON()
		if err != nil {
			return mo.Err[[]string](fmt.Errorf("failed to serialize filter: %w", err))
		}
		filterJSON = string(b)
	} else {
		filterJSON = "{}"
	}

	args := make([]interface{}, 0, 7)
	args = append(args, proto.CmdSearchIndex, indexName, string(krBytes), filterJSON)

	if desc {
		args = append(args, "DESC")
	} else {
		args = append(args, "ASC")
	}
	if lim := kr.GetLimit(); lim != -1 {
		args = append(args, "LIMIT", strconv.Itoa(lim))
	}

	cmd := client.Do(ctx, args...)
	res, err := cmd.StringSlice()
	if err != nil {
		return mo.Err[[]string](err)
	}

	return mo.Ok(res)
}

// SearchKey executes the RESP X command below on the shared connection:
//
//	SEARCHKEY <keyrange_json> <json_filter> [ASC|DESC] [LIMIT count]
//
// The key range is a sealed x.KeyRange value (6 ctors: KeysBt / KeysGt /
// KeysGte / KeysLt / KeysLte / KeysPattern) serialized via MarshalJSON.
// The optional truncation limit is CARRIED ON the KeyRange object itself via
//
//	kr.Limit(count)
//
// (the function signature intentionally does NOT expose a separate limit
// parameter, so the function shape remains 3-arg, identical to the Update
// sibling which intentionally stays as the legacy string pattern arg on its
// first param — zero cross-feature scope creep).
//
// Example:
//
//	res := client.SearchKeyRaw(
//		x.KeysGte("user:engineering:0100").Limit(50),
//		x.Eq("region", "us"),
//		true,
//	)
//	users := res.MustGet()
func SearchKeyRaw(kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}

	client := getSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	krBytes, krErr := kr.MarshalJSON()
	if krErr != nil {
		return mo.Err[[]string](fmt.Errorf("failed to serialize key range: %w", krErr))
	}

	var filterJSON string
	if filter != nil {
		b, err := filter.MarshalJSON()
		if err != nil {
			return mo.Err[[]string](fmt.Errorf("failed to serialize filter: %w", err))
		}
		filterJSON = string(b)
	} else {
		filterJSON = "{}"
	}

	args := make([]interface{}, 0, 6)
	args = append(args, proto.CmdSearchKey, string(krBytes), filterJSON)

	if desc {
		args = append(args, "DESC")
	} else {
		args = append(args, "ASC")
	}
	if lim := kr.GetLimit(); lim != -1 {
		args = append(args, "LIMIT", strconv.Itoa(lim))
	}

	cmd := client.Do(ctx, args...)
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
//	res := client.UpdateRaw(
//		x.KeysPattern("user:*"),
//		x.Eq("status", "pending"),
//		x.Set("status", "active"),
//		x.Set("verified", true),
//	)
//	updatedKeys := res.MustGet()
func UpdateRaw(kr x.KeyRange, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
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

	krBytes, krErr := kr.MarshalJSON()
	if krErr != nil {
		return mo.Err[[]string](fmt.Errorf("failed to serialize key range: %w", krErr))
	}

	filterJSON := "{}"
	if filter != nil {
		b, err := filter.MarshalJSON()
		if err != nil {
			return mo.Err[[]string](fmt.Errorf("failed to serialize filter: %w", err))
		}
		filterJSON = string(b)
	}

	updateJSON, err := x.MarshalUpdate(values...)
	if err != nil {
		return mo.Err[[]string](fmt.Errorf("failed to serialize updates: %w", err))
	}

	args := make([]interface{}, 0, 6)
	args = append(args, proto.CmdUpdate, string(krBytes), filterJSON, string(updateJSON))

	if lim := kr.GetLimit(); lim != -1 {
		args = append(args, "LIMIT", strconv.Itoa(lim))
	}

	cmd := client.Do(ctx, args...)
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

func hostPortFromAddr(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid resp address %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid resp port %q: %w", portStr, err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, port, nil
}

// connect attempts to create and verify a single Resp client connection.
// Returns the connected client on success, or nil + error on failure.
// Caller must handle cleanup (Close) if an error is returned.
func connect(respAddr, authKey string) (*redis.Client, error) {
	if authKey == "" {
		return nil, errors.New("auth key is empty")
	}
	host, port, err := hostPortFromAddr(respAddr)
	if err != nil {
		return nil, err
	}
	res, err := internalDialForRespconnInternal(respconn.Options{
		Host:         host,
		Port:         port,
		Auth:         authKey,
		TimeoutMs:    int(dialTimeout.Milliseconds()),
		AuthOptional: false,
		ReadTimeout:  0,
	})
	if err != nil {
		if res != nil && res.Client != nil {
			if closeErr := res.Client.Close(); closeErr != nil {
				slog.Warn("redisx close client after failed dial handshake failed", "error", closeErr)
			}
		}
		return nil, err
	}
	return res.Client, nil
}

var internalDialForRespconnInternal = respconn.DialAndHandshake

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

func RegisterSchemaFromJSON(specJSON string) error {
	if specJSON == "" {
		return errors.New("schema spec json is empty")
	}
	client := getSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	return client.Do(ctx, proto.CmdRegisterSchema, specJSON).Err()
}

func RegisterIndexShort(ownerNs, logical string, paths ...string) error {
	if ownerNs == "" {
		return errors.New("owner_ns is empty")
	}
	if logical == "" {
		return errors.New("logical is empty")
	}
	if len(paths) == 0 {
		return errors.New("at least one path is required")
	}
	clean := make([]string, 0, len(paths))
	for _, p := range paths {
		if p == "" {
			return errors.New("paths contains an empty entry")
		}
		clean = append(clean, strings.ReplaceAll(p, ".", "_"))
	}
	type raw struct {
		OwnerNs    string   `json:"owner_ns"`
		Logical    string   `json:"logical"`
		Paths      []string `json:"paths"`
		KeyPattern string   `json:"key_pattern,omitempty"`
	}
	ownerMem := strings.HasPrefix(ownerNs, "_m_")
	nsScope := ownerNs
	if !ownerMem {
		nsScope = "_d_" + ownerNs
	}
	spec := raw{
		OwnerNs:    ownerNs,
		Logical:    strings.ToLower(logical),
		Paths:      clean,
		KeyPattern: nsScope + ":*",
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("RegisterIndexShort build JSON: %w", err)
	}
	client := getSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	return client.Do(ctx, proto.CmdRegisterIndex, string(b)).Err()
}

func RegisterIndexFromJSON(specJSON string) error {
	if specJSON == "" {
		return errors.New("index spec json is empty")
	}
	client := getSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	return client.Do(ctx, proto.CmdRegisterIndex, specJSON).Err()
}

func DropSchemaRaw(logicalNs string) error {
	if logicalNs == "" {
		return errors.New("logical ns is empty")
	}
	client := getSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	return client.Do(ctx, proto.CmdDropSchema, logicalNs).Err()
}

func DropIndexRaw(ownerNsOrFull string, logical ...string) error {
	if ownerNsOrFull == "" {
		return errors.New("owner ns or full name is required")
	}
	client := getSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	switch len(logical) {
	case 0:
		return client.Do(ctx, proto.CmdDropIndex, ownerNsOrFull).Err()
	case 1:
		return client.Do(ctx, proto.CmdDropIndex, ownerNsOrFull, logical[0]).Err()
	default:
		return errors.New("DropIndexRaw takes at most 2 args: (fullName)  or  (ownerNs, logical)")
	}
}
