package conn

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// DialTimeout bounds dial, handshake, and single round-trip operations.
const DialTimeout = 3 * time.Second

var (
	clientMu sync.RWMutex
	client   *redis.Client
)

var lifecycleCtx atomic.Value

// SetSharedClient installs the shared client and returns the previous one.
func SetSharedClient(c *redis.Client) *redis.Client {
	clientMu.Lock()
	prev := client
	client = c
	clientMu.Unlock()
	return prev
}

// GetSharedClient returns the current shared client, or nil when not connected.
func GetSharedClient() *redis.Client {
	clientMu.RLock()
	c := client
	clientMu.RUnlock()
	return c
}

// ClearSharedClientIf clears the shared client only if it still points at target.
func ClearSharedClientIf(target *redis.Client) {
	clientMu.Lock()
	if client == target {
		client = nil
	}
	clientMu.Unlock()
}

// SetLifecycleCtx stores the bridge lifecycle context and its cancel func.
func SetLifecycleCtx(ctx context.Context, cancel context.CancelFunc) {
	lifecycleCtx.Store(struct {
		ctx    context.Context
		cancel context.CancelFunc
	}{ctx: ctx, cancel: cancel})
}

// GetLifecycleCtx returns the lifecycle context and cancel func, or (nil, nil)
// when no lifecycle has been installed yet.
func GetLifecycleCtx() (context.Context, context.CancelFunc) {
	val := lifecycleCtx.Load()
	if val == nil {
		return nil, nil
	}
	t := val.(struct {
		ctx    context.Context
		cancel context.CancelFunc
	})
	return t.ctx, t.cancel
}
