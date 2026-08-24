package testutil

import (
	"encoding/json"
	"strings"
	"testing"

	naming "github.com/kcmvp/redisx/internal/naming"
	"github.com/stretchr/testify/require"
)

func TestCountXIs100(t *testing.T) {
	require.Equal(t, 100, CountX())
}

func TestXKeyPrefix(t *testing.T) {
	t.Run("mem layer adds _m_ prefix", func(t *testing.T) {
		require.Equal(t, "_m_:ns:", XKeyPrefix("ns", true))
	})
	t.Run("disk layer omits _m_ prefix", func(t *testing.T) {
		require.Equal(t, "ns:", XKeyPrefix("ns", false))
	})
}

func TestXIDKey(t *testing.T) {
	require.Equal(t, "_m_:probe:p050", XIDKey("probe", true, "p050"))
	require.Equal(t, "disk:p100", XIDKey("disk", false, "p100"))
}

func TestXIDsFromValues(t *testing.T) {
	values := []string{
		`{"id":"a","age":10}`,
		`{"id":"b","age":11}`,
		`{}`,
	}
	require.Equal(t, []string{"a", "b", ""}, XIDsFromValues(values))
}

func TestXRangeIDs(t *testing.T) {
	t.Run("inclusive 50..59 is 10 items", func(t *testing.T) {
		got := XRangeIDs(50, 59)
		require.Len(t, got, 10)
		require.Equal(t, "p050", got[0])
		require.Equal(t, "p059", got[9])
	})
	t.Run("singleton lo==hi", func(t *testing.T) {
		require.Equal(t, []string{"p007"}, XRangeIDs(7, 7))
	})
	t.Run("empty when hi<lo returns zero", func(t *testing.T) {
		got := XRangeIDs(10, 5)
		require.Empty(t, got)
	})
}

func TestXFirstN(t *testing.T) {
	ids := []string{"p0", "p1", "p2", "p3"}
	t.Run("n smaller cap returns first n", func(t *testing.T) {
		require.Equal(t, []string{"p0", "p1"}, XFirstN(ids, 2))
	})
	t.Run("n larger than len returns all", func(t *testing.T) {
		require.Equal(t, ids, XFirstN(ids, 10))
	})
	t.Run("n equals len returns all", func(t *testing.T) {
		require.Equal(t, ids, XFirstN(ids, 4))
	})
	t.Run("empty ids returns empty", func(t *testing.T) {
		require.Empty(t, XFirstN(nil, 3))
	})
}

func TestXLastN(t *testing.T) {
	ids := []string{"p0", "p1", "p2", "p3"}
	t.Run("n smaller returns tail", func(t *testing.T) {
		require.Equal(t, []string{"p2", "p3"}, XLastN(ids, 2))
	})
	t.Run("n larger returns all", func(t *testing.T) {
		require.Equal(t, ids, XLastN(ids, 20))
	})
	t.Run("empty ids returns empty", func(t *testing.T) {
		require.Empty(t, XLastN(nil, 3))
	})
}

func TestXReverseIDs(t *testing.T) {
	t.Run("even length", func(t *testing.T) {
		require.Equal(t, []string{"p3", "p2", "p1", "p0"},
			XReverseIDs([]string{"p0", "p1", "p2", "p3"}))
	})
	t.Run("odd length", func(t *testing.T) {
		require.Equal(t, []string{"c", "b", "a"},
			XReverseIDs([]string{"a", "b", "c"}))
	})
	t.Run("empty returns empty", func(t *testing.T) {
		require.Empty(t, XReverseIDs(nil))
	})
	t.Run("singleton returns same", func(t *testing.T) {
		require.Equal(t, []string{"only"}, XReverseIDs([]string{"only"}))
	})
}

func TestXStrictMonotonic(t *testing.T) {
	asc := []string{"p010", "p020", "p030", "p040"}
	desc := []string{"p040", "p030", "p020", "p010"}
	t.Run("asc strict increasing true", func(t *testing.T) {
		require.True(t, XStrictMonotonic(asc, false))
	})
	t.Run("desc strict decreasing true", func(t *testing.T) {
		require.True(t, XStrictMonotonic(desc, true))
	})
	t.Run("asc with equal fails", func(t *testing.T) {
		require.False(t, XStrictMonotonic([]string{"a", "b", "b", "c"}, false))
	})
	t.Run("asc with revert fails", func(t *testing.T) {
		require.False(t, XStrictMonotonic([]string{"a", "c", "b"}, false))
	})
	t.Run("desc with equal fails", func(t *testing.T) {
		require.False(t, XStrictMonotonic([]string{"c", "b", "b", "a"}, true))
	})
	t.Run("desc with revert fails", func(t *testing.T) {
		require.False(t, XStrictMonotonic([]string{"c", "a", "b"}, true))
	})
	t.Run("empty and singleton always true", func(t *testing.T) {
		require.True(t, XStrictMonotonic(nil, false))
		require.True(t, XStrictMonotonic(nil, true))
		require.True(t, XStrictMonotonic([]string{"x"}, false))
	})
}

func TestLoadX(t *testing.T) {
	kvs := LoadX(t)
	require.Len(t, kvs, CountX())
	t.Run("all keys start with _m_:probe: prefix", func(t *testing.T) {
		for _, kv := range kvs {
			require.True(t, strings.HasPrefix(kv.K, "_m_:probe:"),
				"unexpected key prefix: %s", kv.K)
		}
	})
	t.Run("all values are valid JSON with an id field equal to key suffix", func(t *testing.T) {
		seen := make(map[string]bool, len(kvs))
		for _, kv := range kvs {
			var rec map[string]any
			require.NoError(t, json.Unmarshal([]byte(kv.V), &rec), "bad JSON at key %s", kv.K)
			id, ok := rec["id"].(string)
			require.True(t, ok, "id missing or not string at key %s: %v", kv.K, rec)
			require.Equal(t, kv.K, naming.BuildStorageKey(naming.BuildStorageNs("probe", true), id))
			require.False(t, seen[id], "duplicate id: %s", id)
			seen[id] = true
		}
	})
}

func TestLoadXFor_DifferentNamespace(t *testing.T) {
	a := LoadXFor(t, "alpha", true)
	b := LoadXFor(t, "beta", false)
	require.Len(t, a, CountX())
	require.Len(t, b, CountX())
	aKeys := make(map[string]bool, len(a))
	for _, kv := range a {
		aKeys[kv.K] = true
		require.True(t, strings.HasPrefix(kv.K, "_m_:alpha:"))
	}
	for _, kv := range b {
		require.True(t, strings.HasPrefix(kv.K, "beta:"))
		require.False(t, aKeys[kv.K], "unexpected overlap: %s", kv.K)
	}
}

func TestLoadXRaw(t *testing.T) {
	docs := LoadXRaw(t)
	require.Len(t, docs, CountX())
	ids := make(map[string]bool, len(docs))
	for i, d := range docs {
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(d.RawJSON()), &rec))
		id, ok := rec["id"].(string)
		require.True(t, ok, "doc %d missing id", i)
		ids[id] = true
	}
	require.Len(t, ids, CountX())
}
