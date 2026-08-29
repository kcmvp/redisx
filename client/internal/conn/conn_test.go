package conn

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ─── SetSharedClient / GetSharedClient ───

func TestSetGetSharedClient(t *testing.T) {
	// Clean slate
	clientMu.Lock()
	client = nil
	clientMu.Unlock()

	require.Nil(t, GetSharedClient())

	c1 := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	prev := SetSharedClient(c1)
	require.Nil(t, prev, "first set should return nil previous")
	require.Equal(t, c1, GetSharedClient())

	c2 := redis.NewClient(&redis.Options{Addr: "localhost:6380"})
	prev = SetSharedClient(c2)
	require.Equal(t, c1, prev, "second set should return first client")
	require.Equal(t, c2, GetSharedClient())

	// Cleanup
	clientMu.Lock()
	client = nil
	clientMu.Unlock()
}

// ─── ClearSharedClientIf ───

func TestClearSharedClientIf(t *testing.T) {
	tests := []struct {
		name      string
		setup     func() *redis.Client
		target    func(current *redis.Client) *redis.Client
		wantNil   bool
		wantSame  bool
	}{
		{
			name: "matching target clears",
			setup: func() *redis.Client {
				c := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
				SetSharedClient(c)
				return c
			},
			target:  func(current *redis.Client) *redis.Client { return current },
			wantNil: true,
		},
		{
			name: "non-matching target preserved",
			setup: func() *redis.Client {
				c := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
				SetSharedClient(c)
				return c
			},
			target: func(current *redis.Client) *redis.Client {
				return redis.NewClient(&redis.Options{Addr: "localhost:9999"})
			},
			wantSame: true,
		},
		{
			name: "nil client no-op",
			setup: func() *redis.Client {
				clientMu.Lock()
				client = nil
				clientMu.Unlock()
				return nil
			},
			target: func(current *redis.Client) *redis.Client { return nil },
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := tt.setup()
			target := tt.target(current)
			ClearSharedClientIf(target)

			got := GetSharedClient()
			if tt.wantNil {
				require.Nil(t, got)
			}
			if tt.wantSame {
				require.Equal(t, current, got)
			}

			// Cleanup
			clientMu.Lock()
			client = nil
			clientMu.Unlock()
		})
	}
}

// ─── SetLifecycleCtx / GetLifecycleCtx ───

func TestLifecycleCtx(t *testing.T) {
	// Reset
	lifecycleCtx.Store(struct {
		ctx    context.Context
		cancel context.CancelFunc
	}{})

	tests := []struct {
		name     string
		setup    func()
		wantNil  bool
		wantCancel bool
	}{
		{
			name: "unset returns nil",
			setup: func() {
				lifecycleCtx.Store(struct {
					ctx    context.Context
					cancel context.CancelFunc
				}{})
			},
			wantNil: true,
		},
		{
			name: "set returns ctx and cancel",
			setup: func() {
				ctx, cancel := context.WithCancel(context.Background())
				SetLifecycleCtx(ctx, cancel)
			},
			wantNil:    false,
			wantCancel: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			ctx, cancel := GetLifecycleCtx()
			if tt.wantNil {
				require.Nil(t, ctx)
				require.Nil(t, cancel)
			} else {
				require.NotNil(t, ctx)
				if tt.wantCancel {
					require.NotNil(t, cancel)
					cancel() // cleanup
				}
			}
		})
	}
}
