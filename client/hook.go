package client

import (
	"time"

	"github.com/kcmvp/redisx/client/internal/hook"
)

// HookID uniquely identifies a registered write hook. The zero value means
// "invalid" and is a no-op for RemoveHook.
type HookID = hook.ID

// AddAbortHook registers a fail-closed hook that vetoes the write when it
// returns an error. Applies to every subsequent write through this client.
func AddAbortHook(h func(key string, value []byte) error) HookID {
	return hook.AddAbort(h)
}

// AddTransformHook registers a fail-closed hook that rewrites the value
// before the write; transforms chain in registration order.
func AddTransformHook(h func(key string, value []byte) ([]byte, error)) HookID {
	return hook.AddTransform(h)
}

// AddObserverHook registers a fail-open hook that inspects the post-Transform
// value before the write; panics and timeouts are logged, never fail the write.
func AddObserverHook(h func(key string, value []byte)) HookID {
	return hook.AddObserver(h)
}

// AddObserverAfterHook registers a fail-open hook that receives the write
// outcome (final value, committed flag, write error) after commit.
func AddObserverAfterHook(h func(key string, value []byte, committed bool, writeErr error)) HookID {
	return hook.AddObserverAfter(h)
}

// RemoveHook unregisters the hook with the given id; unknown ids are ignored.
func RemoveHook(id HookID) {
	hook.Remove(id)
}

// SetHookTimeout sets the per-hook wall-clock timeout. d <= 0 disables
// timeouts (panic isolation remains on). Default is 100ms.
func SetHookTimeout(d time.Duration) {
	hook.SetTimeout(d)
}
