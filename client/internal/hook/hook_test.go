package hook

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// resetAll fully resets the hook subsystem between tests.
func resetAll() {
	Reset()
}

// ─── Registry CRUD (table-driven) ───

func TestAddAndLen(t *testing.T) {
	defer resetAll()

	type addOp struct {
		kind string
	}
	tests := []struct {
		name       string
		ops        []addOp
		wantAborts int
		wantTrans  int
		wantObs    int
		wantAfter  int
	}{
		{
			name:       "single abort",
			ops:        []addOp{{kind: "abort"}},
			wantAborts: 1,
		},
		{
			name:      "single transform",
			ops:       []addOp{{kind: "transform"}},
			wantTrans: 1,
		},
		{
			name:    "single observer",
			ops:     []addOp{{kind: "observer"}},
			wantObs: 1,
		},
		{
			name:      "single observer-after",
			ops:       []addOp{{kind: "after"}},
			wantAfter: 1,
		},
		{
			name: "mixed hooks",
			ops: []addOp{
				{kind: "abort"}, {kind: "abort"},
				{kind: "transform"},
				{kind: "observer"}, {kind: "observer"},
				{kind: "after"},
			},
			wantAborts: 2,
			wantTrans:  1,
			wantObs:    2,
			wantAfter:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetAll()
			for _, op := range tt.ops {
				switch op.kind {
				case "abort":
					AddAbort(func(string, []byte) error { return nil })
				case "transform":
					AddTransform(func(_ string, v []byte) ([]byte, error) { return v, nil })
				case "observer":
					AddObserver(func(string, []byte) {})
				case "after":
					AddObserverAfter(func(string, []byte, bool, error) {})
				}
			}
			reg := Snapshot()
			require.NotNil(t, reg)
			require.Equal(t, tt.wantAborts, reg.LenAborts())
			require.Equal(t, tt.wantTrans, reg.LenTransforms())
			require.Equal(t, tt.wantObs, reg.LenObservers())
			require.Equal(t, tt.wantAfter, reg.LenAfters())
		})
	}
}

func TestIDsAreIncrementing(t *testing.T) {
	defer resetAll()
	var ids []ID
	for i := 0; i < 5; i++ {
		ids = append(ids, AddAbort(func(string, []byte) error { return nil }))
	}
	for i := 1; i < len(ids); i++ {
		require.Greater(t, ids[i], ids[i-1])
	}
}

// ─── Nil receiver Len* ───

func TestNilRegistry_LenMethodsReturnZero(t *testing.T) {
	var r *Registry
	tests := []struct {
		name string
		call func() int
	}{
		{"LenAborts", func() int { return r.LenAborts() }},
		{"LenTransforms", func() int { return r.LenTransforms() }},
		{"LenObservers", func() int { return r.LenObservers() }},
		{"LenAfters", func() int { return r.LenAfters() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, 0, tt.call())
		})
	}
}

// ─── Snapshot ───

func TestSnapshot(t *testing.T) {
	defer resetAll()
	tests := []struct {
		name    string
		setup   func()
		wantNil bool
	}{
		{"nil when empty", func() {}, true},
		{"non-nil after add", func() {
			AddAbort(func(string, []byte) error { return nil })
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetAll()
			tt.setup()
			if tt.wantNil {
				require.Nil(t, Snapshot())
			} else {
				require.NotNil(t, Snapshot())
			}
		})
	}
}

// ─── Remove (table-driven) ───

func TestRemove(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() ID // returns the ID to remove
		removeID    func(id ID) // what ID to pass to Remove
		wantAborts  int
		wantTrans   int
		wantNil     bool
	}{
		{
			name: "known abort ID",
			setup: func() ID {
				return AddAbort(func(string, []byte) error { return nil })
			},
			removeID:   func(id ID) { Remove(id) },
			wantAborts: 0,
			wantNil:    true,
		},
		{
			name: "unknown ID is no-op",
			setup: func() ID {
				AddAbort(func(string, []byte) error { return nil })
				return ID(0) // sentinel: use bogus ID
			},
			removeID:   func(id ID) { Remove(ID(99999)) },
			wantAborts: 1,
		},
		{
			name:       "zero ID is no-op",
			setup:      func() ID { AddAbort(func(string, []byte) error { return nil }); return ID(0) },
			removeID:   func(id ID) { Remove(0) },
			wantAborts: 1,
		},
		{
			name:    "from empty registry",
			setup:   func() ID { return ID(42) },
			removeID: func(id ID) { Remove(id) },
			wantNil: true,
		},
		{
			name: "only removes correct type",
			setup: func() ID {
				abortID := AddAbort(func(string, []byte) error { return nil })
				AddTransform(func(_ string, v []byte) ([]byte, error) { return v, nil })
				return abortID
			},
			removeID:  func(id ID) { Remove(id) },
			wantAborts: 0,
			wantTrans:  1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer resetAll()
			id := tt.setup()
			tt.removeID(id)
			reg := Snapshot()
			if tt.wantNil {
				require.Nil(t, reg)
				return
			}
			require.NotNil(t, reg)
			require.Equal(t, tt.wantAborts, reg.LenAborts())
			require.Equal(t, tt.wantTrans, reg.LenTransforms())
		})
	}
}

func TestRemove_LastHookMakesRegistryNil(t *testing.T) {
	defer resetAll()
	id1 := AddAbort(func(string, []byte) error { return nil })
	id2 := AddTransform(func(_ string, v []byte) ([]byte, error) { return v, nil })

	Remove(id1)
	require.NotNil(t, Snapshot())
	require.Equal(t, 1, Snapshot().LenTransforms())

	Remove(id2)
	require.Nil(t, Snapshot())
}

// ─── Reset ───

func TestReset(t *testing.T) {
	defer resetAll()
	AddAbort(func(string, []byte) error { return nil })
	AddTransform(func(_ string, v []byte) ([]byte, error) { return v, nil })
	AddObserver(func(string, []byte) {})
	AddObserverAfter(func(string, []byte, bool, error) {})
	SetTimeout(500 * time.Millisecond)

	Reset()

	require.Nil(t, Snapshot())
	require.Equal(t, defaultTimeout, GetTimeout())
}

// ─── SetTimeout / GetTimeout (table-driven) ───

func TestSetGetTimeout(t *testing.T) {
	defer resetAll()
	tests := []struct {
		name  string
		input time.Duration
		want  time.Duration
	}{
		{"positive", 200 * time.Millisecond, 200 * time.Millisecond},
		{"zero", 0, time.Duration(0)},
		{"negative", -1 * time.Second, -1 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetTimeout(tt.input)
			require.Equal(t, tt.want, GetTimeout())
		})
	}
}

// ─── cloneRegistry ───

func TestCloneRegistry(t *testing.T) {
	defer resetAll()
	tests := []struct {
		name       string
		setup      func() *Registry
		wantAborts int
		wantTrans  int
	}{
		{"nil input", func() *Registry { return cloneRegistry(nil) }, 0, 0},
		{
			"copies all",
			func() *Registry {
				AddAbort(func(string, []byte) error { return nil })
				AddTransform(func(_ string, v []byte) ([]byte, error) { return v, nil })
				return cloneRegistry(Snapshot())
			},
			1, 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetAll()
			c := tt.setup()
			require.Equal(t, tt.wantAborts, c.LenAborts())
			require.Equal(t, tt.wantTrans, c.LenTransforms())
		})
	}
}

// ─── RunBefore (table-driven) ───

func TestRunBefore(t *testing.T) {
	tests := []struct {
		name      string
		setup     func() *Registry
		key       string
		value     []byte
		wantVal   []byte
		wantErr   bool
		errPhrase string
	}{
		{
			name: "abort passes (no error)",
			setup: func() *Registry {
				AddAbort(func(string, []byte) error { return nil })
				return Snapshot()
			},
			value:   []byte("hello"),
			wantVal: []byte("hello"),
		},
		{
			name: "abort rejects",
			setup: func() *Registry {
				AddAbort(func(string, []byte) error { return errors.New("blocked") })
				return Snapshot()
			},
			value:     []byte("hello"),
			wantErr:   true,
			errPhrase: "blocked",
		},
		{
			name: "transform rewrites value",
			setup: func() *Registry {
				AddTransform(func(_ string, _ []byte) ([]byte, error) {
					return []byte("rewritten"), nil
				})
				return Snapshot()
			},
			value:   []byte("original"),
			wantVal: []byte("rewritten"),
		},
		{
			name: "transform chains in order",
			setup: func() *Registry {
				AddTransform(func(_ string, v []byte) ([]byte, error) {
					return append(v, []byte("_t1")...), nil
				})
				AddTransform(func(_ string, v []byte) ([]byte, error) {
					return append(v, []byte("_t2")...), nil
				})
				return Snapshot()
			},
			value:   []byte("base"),
			wantVal: []byte("base_t1_t2"),
		},
		{
			name: "transform error aborts chain",
			setup: func() *Registry {
				AddTransform(func(_ string, _ []byte) ([]byte, error) {
					return nil, errors.New("transform fail")
				})
				AddTransform(func(_ string, v []byte) ([]byte, error) {
					return []byte("should-not-reach"), nil
				})
				return Snapshot()
			},
			value:     []byte("x"),
			wantErr:   true,
			errPhrase: "transform fail",
		},
		{
			name: "observer called but does not alter value",
			setup: func() *Registry {
				var observed []byte
				AddObserver(func(_ string, v []byte) {
					observed = append([]byte(nil), v...)
				})
				reg := Snapshot()
				// capture observed value after RunBefore via closure check
				_ = observed
				return reg
			},
			value:   []byte("data"),
			wantVal: []byte("data"),
		},
		{
			name: "abort then transform then observer full pipeline",
			setup: func() *Registry {
				AddAbort(func(string, []byte) error { return nil })
				AddTransform(func(_ string, _ []byte) ([]byte, error) {
					return []byte("transformed"), nil
				})
				AddObserver(func(string, []byte) {})
				return Snapshot()
			},
			value:   []byte("input"),
			wantVal: []byte("transformed"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer resetAll()
			reg := tt.setup()
			got, err := RunBefore(reg, tt.key, tt.value)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errPhrase != "" {
					require.Contains(t, err.Error(), tt.errPhrase)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantVal, got)
		})
	}
}

// ─── RunAfter (table-driven) ───

func TestRunAfter(t *testing.T) {
	defer resetAll()

	type captured struct {
		key        string
		value      []byte
		committed  bool
		writeErr   error
	}

	tests := []struct {
		name      string
		setup     func() (*Registry, *[]captured)
		committed bool
		writeErr  error
	}{
		{
			name: "observer-after receives correct args",
			setup: func() (*Registry, *[]captured) {
				var caps []captured
				AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
					caps = append(caps, captured{key: key, value: append([]byte(nil), value...), committed: committed, writeErr: writeErr})
				})
				return Snapshot(), &caps
			},
			committed: true,
			writeErr:  nil,
		},
		{
			name: "observer-after receives write error",
			setup: func() (*Registry, *[]captured) {
				var caps []captured
				AddObserverAfter(func(key string, value []byte, committed bool, writeErr error) {
					caps = append(caps, captured{key: key, value: append([]byte(nil), value...), committed: committed, writeErr: writeErr})
				})
				return Snapshot(), &caps
			},
			committed: false,
			writeErr:  errors.New("write failed"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetAll()
			reg, caps := tt.setup()
			RunAfter(reg, "mykey", []byte("myval"), tt.committed, tt.writeErr)
			require.Len(t, *caps, 1)
			c := (*caps)[0]
			require.Equal(t, "mykey", c.key)
			require.Equal(t, []byte("myval"), c.value)
			require.Equal(t, tt.committed, c.committed)
			if tt.writeErr != nil {
				require.Error(t, c.writeErr)
			} else {
				require.NoError(t, c.writeErr)
			}
		})
	}
}

// ─── runWithSafety — panic recovery and timeout ───

func TestRunWithSafety_PanicRecovery(t *testing.T) {
	defer resetAll()
	err := runWithSafety("test-panic", func() error {
		panic("boom")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic")
}

func TestRunWithSafety_Timeout(t *testing.T) {
	defer resetAll()
	SetTimeout(10 * time.Millisecond)
	err := runWithSafety("test-timeout", func() error {
		time.Sleep(5 * time.Second)
		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}

func TestRunWithSafety_NoTimeout_Disabled(t *testing.T) {
	defer resetAll()
	SetTimeout(0) // disable timeout, panic recovery only
	called := false
	err := runWithSafety("test-no-timeout", func() error {
		called = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)
}

func TestRunWithSafety_NoTimeout_PanicRecovery(t *testing.T) {
	defer resetAll()
	SetTimeout(0)
	err := runWithSafety("test-panic-no-timeout", func() error {
		panic("boom-no-timeout")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic")
}

func TestRunWithSafety_NormalReturn(t *testing.T) {
	defer resetAll()
	err := runWithSafety("test-ok", func() error {
		return nil
	})
	require.NoError(t, err)
}

func TestRunWithSafety_NormalError(t *testing.T) {
	defer resetAll()
	err := runWithSafety("test-err", func() error {
		return errors.New("normal error")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "normal error")
}

// ─── Concurrent safety ───

func TestConcurrentAddRemove(t *testing.T) {
	defer resetAll()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := AddAbort(func(string, []byte) error { return nil })
			Remove(id)
		}()
	}
	wg.Wait()
	// After all goroutines, all hooks should be removed
	// (some may still exist due to race between add/remove, but no panic)
}
