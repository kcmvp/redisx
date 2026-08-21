package testutil

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tailscale/hujson"
	"github.com/tidwall/gjson"
)

type KV struct {
	K string
	V string
}

type TestDoc string

func (TestDoc) Namespace() string  { return "probe" }
func (TestDoc) Mem() bool          { return true }
func (TestDoc) KeyAttrs() []string { return []string{"id"} }
func (d TestDoc) RawJSON() string  { return string(d) }
func (TestDoc) TTL() time.Duration { return 0 }

func XKeyPrefix(namespace string, mem bool) string {
	if mem {
		return "_m_" + namespace + ":"
	}
	return namespace + ":"
}

func XIDKey(namespace string, mem bool, id string) string {
	return XKeyPrefix(namespace, mem) + id
}

func XIDsFromValues(values []string) []string {
	ids := make([]string, len(values))
	for i, v := range values {
		ids[i] = gjson.Get(v, "id").String()
	}
	return ids
}

func XRangeIDs(lo, hi int) []string {
	out := make([]string, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = append(out, fmt.Sprintf("p%03d", i))
	}
	return out
}

func XFirstN(ids []string, n int) []string {
	if len(ids) < n {
		return ids
	}
	return ids[:n]
}

func XLastN(ids []string, n int) []string {
	if len(ids) < n {
		return ids
	}
	return ids[len(ids)-n:]
}

func XReverseIDs(asc []string) []string {
	out := make([]string, len(asc))
	for i, v := range asc {
		out[len(asc)-1-i] = v
	}
	return out
}

func XStrictMonotonic(ids []string, desc bool) bool {
	for i := 1; i < len(ids); i++ {
		if desc {
			if ids[i-1] <= ids[i] {
				return false
			}
		} else {
			if ids[i-1] >= ids[i] {
				return false
			}
		}
	}
	return true
}

func LoadX(tb testing.TB) []KV {
	tb.Helper()
	return LoadXFor(tb, "probe", true)
}

func LoadXFor(tb testing.TB, namespace string, mem bool) []KV {
	tb.Helper()

	_, self, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("LoadXFor: could not locate fixture file")
	}

	path := filepath.Join(filepath.Dir(self), "testdata", "x.jsonc")
	raw, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("LoadXFor: read %s: %v", path, err)
	}

	standard, err := hujson.Standardize(raw)
	if err != nil {
		tb.Fatalf("LoadXFor: hujson standardize: %v", err)
	}

	var records = make([]map[string]any, 0)
	if err := json.Unmarshal(standard, &records); err != nil {
		tb.Fatalf("LoadXFor: unmarshal records: %v", err)
	}

	out := make([]KV, 0, len(records))
	prefix := XKeyPrefix(namespace, mem)
	for i, rec := range records {
		compact, err := json.Marshal(rec)
		if err != nil {
			tb.Fatalf("LoadXFor: marshal record %d: %v", i, err)
		}

		idRaw, ok := rec["id"]
		if !ok {
			tb.Fatalf("LoadXFor: record %d missing 'id' field", i)
		}
		idStr, ok := idRaw.(string)
		if !ok {
			tb.Fatalf("LoadXFor: record %d 'id' field is not string (got %T)", i, idRaw)
		}
		key := prefix + idStr

		out = append(out, KV{K: key, V: string(compact)})
	}

	return out
}

func LoadXRaw(tb testing.TB) []TestDoc {
	tb.Helper()
	kvs := LoadX(tb)
	docs := make([]TestDoc, len(kvs))
	for i, kv := range kvs {
		docs[i] = TestDoc(kv.V)
	}
	return docs
}

func CountX() int { return 100 }
