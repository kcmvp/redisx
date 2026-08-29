package client

import (
	"testing"
	"time"

	"github.com/kcmvp/redisx/client/internal/hook"
	"github.com/stretchr/testify/require"
)

func TestPublicHookAPI(t *testing.T) {
	defer hook.Reset()

	tests := []struct {
		name    string
		add     func() HookID
		wantLen func(*hook.Registry) int
	}{
		{
			name:    "AddAbortHook",
			add:     func() HookID { return AddAbortHook(func(string, []byte) error { return nil }) },
			wantLen: func(r *hook.Registry) int { return r.LenAborts() },
		},
		{
			name:    "AddTransformHook",
			add:     func() HookID { return AddTransformHook(func(_ string, v []byte) ([]byte, error) { return v, nil }) },
			wantLen: func(r *hook.Registry) int { return r.LenTransforms() },
		},
		{
			name:    "AddObserverHook",
			add:     func() HookID { return AddObserverHook(func(string, []byte) {}) },
			wantLen: func(r *hook.Registry) int { return r.LenObservers() },
		},
		{
			name:    "AddObserverAfterHook",
			add:     func() HookID { return AddObserverAfterHook(func(string, []byte, bool, error) {}) },
			wantLen: func(r *hook.Registry) int { return r.LenAfters() },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hook.Reset()
			id := tt.add()
			require.NotZero(t, id)
			reg := hook.Snapshot()
			require.NotNil(t, reg)
			require.Equal(t, 1, tt.wantLen(reg))
		})
	}
}

func TestRemoveHook_PublicAPI(t *testing.T) {
	defer hook.Reset()
	id := AddAbortHook(func(string, []byte) error { return nil })
	require.Equal(t, 1, hook.Snapshot().LenAborts())

	RemoveHook(id)
	require.Nil(t, hook.Snapshot())
}

func TestRemoveHook_ZeroID_Noop(t *testing.T) {
	defer hook.Reset()
	AddAbortHook(func(string, []byte) error { return nil })
	RemoveHook(HookID(0))
	require.Equal(t, 1, hook.Snapshot().LenAborts())
}

func TestSetHookTimeout_PublicAPI(t *testing.T) {
	defer hook.Reset()
	SetHookTimeout(500 * time.Millisecond)
	require.Equal(t, 500*time.Millisecond, hook.GetTimeout())
}
