package hook

// Write Hook Subsystem — safety-net composition policy:
//
//   - Double recover: stack-safe, no side effects (innermost-first).
//   - Double timeout:
//     User timeout < framework timeout → no duplicate log.
//     User timeout > framework timeout → duplicate slog. Fix: use per-hook timeouts + globally SetTimeout(0).
//   - Abort timeout/panic = fail-closed (ABORT). Non-negotiable security default.
//   - Observer* timeout/panic = fail-open (LOG ONLY). Never impact write.
//
// Execution order (synchronous, all Before hooks lifecycle complete before Set returns):
//  1. Abort (fail-closed) → abort on any error/panic/timeout
//  2. Transform (fail-closed) → abort on any error/panic/timeout; chains in registration order
//  3. Observer (fail-open Before) → sees post-Transform value; only log on panic/timeout
//  4. Actual Redis SET / SETNX / ...
//  5. ObserverAfter (fail-open After) → receives final value + writeErr; only log on panic/timeout

import (
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const defaultTimeout = 100 * time.Millisecond

// ID uniquely identifies a registered hook. The zero value is never assigned.
type ID uint64

// Abort vetoes a write when it returns an error (fail-closed).
type Abort func(key string, value []byte) error

// Transform rewrites the value before the write (fail-closed; chained in registration order).
type Transform func(key string, value []byte) (newValue []byte, err error)

// Observer inspects the post-Transform value before the write (fail-open).
type Observer func(key string, value []byte)

// ObserverAfter inspects the write outcome after commit (fail-open).
type ObserverAfter func(key string, value []byte, committed bool, writeErr error)

type registeredAbort struct {
	id ID
	h  Abort
}

type registeredTransform struct {
	id ID
	h  Transform
}

type registeredObserver struct {
	id ID
	h  Observer
}

type registeredObserverAfter struct {
	id ID
	h  ObserverAfter
}

// Registry is an immutable snapshot of all registered hooks.
type Registry struct {
	aborts     []registeredAbort
	transforms []registeredTransform
	observers  []registeredObserver
	afters     []registeredObserverAfter
}

var (
	mu      sync.RWMutex
	store   atomic.Pointer[Registry]
	nextID  ID
	timeout atomic.Int64
)

func init() {
	store.Store(nil)
	timeout.Store(int64(defaultTimeout))
}

// SetTimeout sets the per-hook execution timeout. Values <= 0 disable
// timeouts (hooks then run with panic recovery only).
func SetTimeout(d time.Duration) {
	timeout.Store(int64(d))
}

// GetTimeout returns the current per-hook execution timeout.
func GetTimeout() time.Duration {
	return time.Duration(timeout.Load())
}

// Snapshot returns the current hook registry, or nil when no hooks are
// registered, keeping the no-hook path zero-cost.
func Snapshot() *Registry {
	return store.Load()
}

func (r *Registry) LenAborts() int {
	if r == nil {
		return 0
	}
	return len(r.aborts)
}

func (r *Registry) LenTransforms() int {
	if r == nil {
		return 0
	}
	return len(r.transforms)
}

func (r *Registry) LenObservers() int {
	if r == nil {
		return 0
	}
	return len(r.observers)
}

func (r *Registry) LenAfters() int {
	if r == nil {
		return 0
	}
	return len(r.afters)
}

func cloneRegistry(reg *Registry) *Registry {
	n := &Registry{}
	if reg != nil {
		n.aborts = append([]registeredAbort(nil), reg.aborts...)
		n.transforms = append([]registeredTransform(nil), reg.transforms...)
		n.observers = append([]registeredObserver(nil), reg.observers...)
		n.afters = append([]registeredObserverAfter(nil), reg.afters...)
	}
	return n
}

func nextHookID() ID {
	for {
		cur := atomic.LoadUint64((*uint64)(&nextID))
		nxt := cur + 1
		if nxt == 0 {
			nxt = 1
		}
		if atomic.CompareAndSwapUint64((*uint64)(&nextID), cur, nxt) {
			return ID(nxt)
		}
	}
}

func AddAbort(h Abort) ID {
	mu.Lock()
	defer mu.Unlock()
	id := nextHookID()
	reg := cloneRegistry(store.Load())
	reg.aborts = append(reg.aborts, registeredAbort{id: id, h: h})
	store.Store(reg)
	return id
}

func AddTransform(h Transform) ID {
	mu.Lock()
	defer mu.Unlock()
	id := nextHookID()
	reg := cloneRegistry(store.Load())
	reg.transforms = append(reg.transforms, registeredTransform{id: id, h: h})
	store.Store(reg)
	return id
}

func AddObserver(h Observer) ID {
	mu.Lock()
	defer mu.Unlock()
	id := nextHookID()
	reg := cloneRegistry(store.Load())
	reg.observers = append(reg.observers, registeredObserver{id: id, h: h})
	store.Store(reg)
	return id
}

func AddObserverAfter(h ObserverAfter) ID {
	mu.Lock()
	defer mu.Unlock()
	id := nextHookID()
	reg := cloneRegistry(store.Load())
	reg.afters = append(reg.afters, registeredObserverAfter{id: id, h: h})
	store.Store(reg)
	return id
}

// Remove unregisters the hook with the given id. Unknown or zero ids are
// ignored. When the last hook is removed the registry snapshot becomes nil
// again so the no-hook path stays zero-cost.
func Remove(id ID) {
	if id == 0 {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	reg := store.Load()
	if reg == nil {
		return
	}
	n := &Registry{}
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
		store.Store(nil)
		return
	}
	store.Store(n)
}

// Reset clears all registered hooks and restores the default hook timeout.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	store.Store(nil)
	atomic.StoreUint64((*uint64)(&nextID), 0)
	timeout.Store(int64(defaultTimeout))
}

func runWithSafety(label string, fn func() error) error {
	d := GetTimeout()
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
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

// RunBefore runs Abort, Transform, and Observer hooks (in that order) against
// the incoming value and returns the final transformed value.
func RunBefore(reg *Registry, key string, value []byte) ([]byte, error) {
	for i, rh := range reg.aborts {
		h := rh.h
		if err := runWithSafety(fmt.Sprintf("Abort#%d", i), func() error {
			return h(key, value)
		}); err != nil {
			return nil, err
		}
	}
	cur := value
	for i, rh := range reg.transforms {
		h := rh.h
		var nv []byte
		herr := runWithSafety(fmt.Sprintf("Transform#%d", i), func() error {
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
		_ = runWithSafety(fmt.Sprintf("Observer#%d", i), func() error {
			h(key, cur)
			return nil
		})
	}
	return cur, nil
}

// RunAfter runs ObserverAfter hooks with the write outcome.
func RunAfter(reg *Registry, key string, value []byte, committed bool, writeErr error) {
	for i, rh := range reg.afters {
		h := rh.h
		_ = runWithSafety(fmt.Sprintf("ObserverAfter#%d", i), func() error {
			h(key, value, committed, writeErr)
			return nil
		})
	}
}
