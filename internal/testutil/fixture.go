package testutil

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/require"
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

func KeyRangeDocKeyAttrs() []string { return []string{"id"} }
func KeyRangeDocTTL() time.Duration { return 0 }
func KeyRangeFixtureMem() bool      { return true }

func XKeyPrefix(namespace string, mem bool) string {
	if mem {
		return "_m_" + namespace + ":"
	}
	return namespace + ":"
}

func XIDKey(namespace string, mem bool, id string) string {
	return XKeyPrefix(namespace, mem) + id
}

func KeyRangeIndexName(namespace string, mem bool, name string) string {
	prefix := XKeyPrefix(namespace, mem)
	if len(prefix) > 0 && prefix[len(prefix)-1] == ':' {
		return prefix[:len(prefix)-1] + "_" + name
	}
	return prefix + "_" + name
}

func KeyRangeKeyPattern(namespace string, mem bool) string {
	return XKeyPrefix(namespace, mem) + "*"
}

func KeyRangeRawIndexes(namespace string, mem bool) []x.Index {
	kp := KeyRangeKeyPattern(namespace, mem)
	return []x.Index{
		x.RawIndex(KeyRangeIndexName(namespace, mem, "score"), kp, "score"),
		x.RawIndex(KeyRangeIndexName(namespace, mem, "bucket"), kp, "bucket"),
		x.RawIndex(KeyRangeIndexName(namespace, mem, "sparse_amt"), kp, "sparse_amt"),
	}
}

func XIDsFromValues(values []string) []string {
	ids := make([]string, len(values))
	for i, v := range values {
		ids[i] = gjson.Get(v, "id").String()
	}
	return ids
}

func XRangeIDs(lo, hi int) []string {
	if hi < lo {
		return nil
	}
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

type KeyRangeCtorCase struct {
	Name    string
	Build   func(idFn func(string) string) x.KeyRange
	WantAsc []string
}

func KeyRangeCtorCases() []KeyRangeCtorCase {
	allAsc := XRangeIDs(0, 99)
	out := make([]KeyRangeCtorCase, 0, 15)

	out = append(out, KeyRangeCtorCase{
		Name:    "KeysPattern_star_all_100",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysPattern(idFn("*")) },
		WantAsc: allAsc,
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysPattern_leading_glob_p05_star_band05_only_10",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysPattern(idFn("p05*")) },
		WantAsc: XRangeIDs(50, 59),
	})
	out = append(out, KeyRangeCtorCase{
		Name:  "KeysPattern_single_char_question_p0_Q5_10_every_tenth",
		Build: func(idFn func(string) string) x.KeyRange { return x.KeysPattern(idFn("p0?5")) },
		WantAsc: []string{
			"p005", "p015", "p025", "p035", "p045",
			"p055", "p065", "p075", "p085", "p095",
		},
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysGte_literal_p050_INCLUSIVE_50",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysGte(idFn("p050")) },
		WantAsc: XRangeIDs(50, 99),
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysGte_pattern_p05_star_10_ONLY_match_true_kept",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysGte(idFn("p05*")) },
		WantAsc: XRangeIDs(50, 59),
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysGt_literal_p050_EXCLUSIVE_49",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysGt(idFn("p050")) },
		WantAsc: XRangeIDs(51, 99),
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysGt_pattern_p05_star_SAME_10_as_Gte_pattern_for_band05",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysGt(idFn("p05*")) },
		WantAsc: XRangeIDs(50, 59),
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysLte_literal_p049_INCLUSIVE_50",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysLte(idFn("p049")) },
		WantAsc: XRangeIDs(0, 49),
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysLte_pattern_p04_star_EMPTY_never_both_true",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysLte(idFn("p04*")) },
		WantAsc: []string{},
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysLt_literal_p050_EXCLUSIVE_50",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysLt(idFn("p050")) },
		WantAsc: XRangeIDs(0, 49),
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysLt_pattern_p05_star_EMPTY_never_both_true",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysLt(idFn("p05*")) },
		WantAsc: []string{},
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysBt_literal_literal_p020_p070_halfopen_50",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysBt(idFn("p020"), idFn("p070")) },
		WantAsc: XRangeIDs(20, 69),
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysBt_pattern_ge_p03_star_literal_lt_p070_40",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysBt(idFn("p03*"), idFn("p070")) },
		WantAsc: XRangeIDs(30, 69),
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysBt_literal_ge_p020_pattern_lt_p06_star_50",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysBt(idFn("p020"), idFn("p06*")) },
		WantAsc: XRangeIDs(20, 69),
	})
	out = append(out, KeyRangeCtorCase{
		Name:    "KeysBt_pattern_p03_star_pattern_p06_star_40",
		Build:   func(idFn func(string) string) x.KeyRange { return x.KeysBt(idFn("p03*"), idFn("p06*")) },
		WantAsc: XRangeIDs(30, 69),
	})

	return out
}

type AssertableT interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

func assertTrue(t AssertableT, value bool, format string, args ...any) {
	t.Helper()
	if !value {
		t.Errorf("assert failed: "+format, args...)
	}
}

func assertEqual(t AssertableT, expected, actual any, format string, args ...any) {
	t.Helper()
	expJ, err1 := json.Marshal(expected)
	actJ, err2 := json.Marshal(actual)
	if err1 != nil || err2 != nil {
		t.Errorf("assert equal marshal fail: %v %v (expected=%v actual=%v)", err1, err2, expected, actual)
		return
	}
	if string(expJ) != string(actJ) {
		t.Errorf(format+": expected=%s actual=%s", append(args, string(expJ), string(actJ))...)
	}
}

func assertLen(t AssertableT, object any, length int, format string, args ...any) {
	t.Helper()
	l, ok := lenAny(object)
	if !ok || l != length {
		arg := append([]any{length}, args...)
		t.Errorf("len mismatch expected=%d: "+format, arg...)
	}
}

func lenAny(v any) (int, bool) {
	switch s := v.(type) {
	case []string:
		return len(s), true
	case []int:
		return len(s), true
	case []any:
		return len(s), true
	case string:
		return len(s), true
	default:
		return -1, false
	}
}

type SearchKeyRunFn = func(kr x.KeyRange, desc bool) (ids []string, ok bool, errMsg string)
type SearchIndexRunFn = func(idxName string, kr x.KeyRange, desc bool) (ids []string, ok bool, errMsg string)

func AssertKeyRangeRunResult(t AssertableT, caseName, label string, wantAsc []string, ids []string, ok bool, errMsg string, desc bool) {
	t.Helper()
	if !ok {
		t.Errorf("%s/%s: expected Ok, got Error: %s", caseName, label, errMsg)
		return
	}
	if len(wantAsc) != len(ids) {
		t.Errorf("%s/%s: length mismatch want=%d got=%d ids=%v", caseName, label, len(wantAsc), len(ids), ids)
		return
	}
	if len(ids) > 0 {
		if !XStrictMonotonic(ids, desc) {
			t.Errorf("%s/%s: monotonic check failed (desc=%v) ids=%v", caseName, label, desc, ids)
			return
		}
	}
	var want []string
	if desc {
		want = make([]string, len(wantAsc))
		copy(want, wantAsc)
		for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
			want[i], want[j] = want[j], want[i]
		}
	} else {
		want = wantAsc
	}
	if len(want) == 0 && len(ids) == 0 {
		return
	}
	assertEqual(t, want, ids, "%s/%s content mismatch (desc=%v)", caseName, label, desc)
}

func AssertSearchKeyMatrix(t AssertableT, run SearchKeyRunFn, cases []KeyRangeCtorCase, idFn func(string) string, caseNamePrefix string) {
	t.Helper()
	for _, tc := range cases {
		tc := tc
		kr := tc.Build(idFn)
		fullCase := caseNamePrefix + tc.Name

		ids, ok, errMsg := run(kr, false)
		AssertKeyRangeRunResult(t, fullCase, "ASC_no_limit", tc.WantAsc, ids, ok, errMsg, false)
		ids, ok, errMsg = run(kr, true)
		AssertKeyRangeRunResult(t, fullCase, "DESC_no_limit", tc.WantAsc, ids, ok, errMsg, true)

		if len(tc.WantAsc) >= 5 {
			limit5 := tc.WantAsc[:5]
			ids, ok, errMsg = run(kr.Limit(5), false)
			AssertKeyRangeRunResult(t, fullCase, "ASC_Limit_5_is_first_5", limit5, ids, ok, errMsg, false)
			wantDesc5 := make([]string, 5)
			copy(wantDesc5, tc.WantAsc[len(tc.WantAsc)-5:])
			ids, ok, errMsg = run(kr.Limit(5), true)
			AssertKeyRangeRunResult(t, fullCase, "DESC_Limit_5_is_last_5_rev", wantDesc5, ids, ok, errMsg, true)
		}
		if len(tc.WantAsc) >= 3 {
			ids, ok, errMsg = run(kr.Limit(len(tc.WantAsc)), false)
			AssertKeyRangeRunResult(t, fullCase, "ASC_Limit_EQ_count_returns_all", tc.WantAsc, ids, ok, errMsg, false)
			ids, ok, errMsg = run(kr.Limit(len(tc.WantAsc)+500), false)
			AssertKeyRangeRunResult(t, fullCase, "ASC_Limit_OVER_count_safe", tc.WantAsc, ids, ok, errMsg, false)
		}
	}
}

func AssertSearchIndexMatrix(t AssertableT, run SearchIndexRunFn, idxName string, cases []KeyRangeCtorCase, idFn func(string) string, caseNamePrefix string) {
	t.Helper()
	for _, tc := range cases {
		tc := tc
		kr := tc.Build(idFn)
		fullCase := caseNamePrefix + tc.Name

		ids, ok, errMsg := run(idxName, kr, false)
		AssertKeyRangeRunResult(t, fullCase, "ASC_no_limit", tc.WantAsc, ids, ok, errMsg, false)
		ids, ok, errMsg = run(idxName, kr, true)
		AssertKeyRangeRunResult(t, fullCase, "DESC_no_limit", tc.WantAsc, ids, ok, errMsg, true)

		if len(tc.WantAsc) >= 5 {
			limit5 := tc.WantAsc[:5]
			ids, ok, errMsg = run(idxName, kr.Limit(5), false)
			AssertKeyRangeRunResult(t, fullCase, "ASC_Limit_5_is_first_5", limit5, ids, ok, errMsg, false)
			wantDesc5 := make([]string, 5)
			copy(wantDesc5, tc.WantAsc[len(tc.WantAsc)-5:])
			ids, ok, errMsg = run(idxName, kr.Limit(5), true)
			AssertKeyRangeRunResult(t, fullCase, "DESC_Limit_5_is_last_5_rev", wantDesc5, ids, ok, errMsg, true)
		}
		if len(tc.WantAsc) >= 3 {
			ids, ok, errMsg = run(idxName, kr.Limit(len(tc.WantAsc)), false)
			AssertKeyRangeRunResult(t, fullCase, "ASC_Limit_EQ_count_returns_all", tc.WantAsc, ids, ok, errMsg, false)
			ids, ok, errMsg = run(idxName, kr.Limit(len(tc.WantAsc)+500), false)
			AssertKeyRangeRunResult(t, fullCase, "ASC_Limit_OVER_count_safe", tc.WantAsc, ids, ok, errMsg, false)
		}
	}
}

func AssertGtGteGap1(t AssertableT, gteIDs []string, gtIDs []string, boundary string) {
	t.Helper()
	assertTrue(t, len(gteIDs) >= 1, "gte result set must have >=1 items (boundary=%s)", boundary)
	assertEqual(t, len(gteIDs)-1, len(gtIDs), "Gte - Gt length diff must be exactly 1 (boundary %s)", boundary)
	assertEqual(t, boundary, gteIDs[0], "Gte first must equal boundary %s", boundary)
	next := fmt.Sprintf("p%03d", atoi(boundary[1:])+1)
	if len(gtIDs) > 0 {
		assertEqual(t, next, gtIDs[0], "Gt first must equal boundary+1 %s", next)
	}
	assertEqual(t, gteIDs[1:], gtIDs, "Gt should equal Gte[1:]")
}

func AssertLtLteGap1(t AssertableT, lteIDs []string, ltIDs []string, boundary string) {
	t.Helper()
	assertTrue(t, len(lteIDs) >= 1, "Lte result set must have >=1 items (boundary=%s)", boundary)
	assertEqual(t, len(lteIDs)-1, len(ltIDs), "Lte - Lt length diff must be exactly 1 (boundary %s)", boundary)
	assertEqual(t, boundary, lteIDs[len(lteIDs)-1], "Lte last must equal boundary %s", boundary)
	if len(ltIDs) > 0 {
		prev := fmt.Sprintf("p%03d", atoi(boundary[1:])-1)
		assertEqual(t, prev, ltIDs[len(ltIDs)-1], "Lt last must equal boundary-1 %s", prev)
	}
}

func AssertBucketDistribution(t AssertableT, idsA, idsC, allIDs []string) {
	t.Helper()
	want := map[string]int{"A": 34, "B": 33, "C": 33}
	got := map[string]int{
		"A": len(idsA),
		"B": len(allIDs) - len(idsA) - len(idsC),
		"C": len(idsC),
	}
	assertEqual(t, want, got, "bucket A/B/C distribution mismatch against probe fixture spec")
	assertLen(t, idsA, 34, "bucket A count should be 34")
	assertLen(t, idsC, 33, "bucket C count should be 33")
	assertLen(t, allIDs, CountX(), "bucket ASC total must = CountX() = %d", CountX())
	assertTrue(t, XStrictMonotonic(idsA, false), "bucket A ASC tiebreak should be id lex monotonic")
}

func AssertSparseLimit10(t AssertableT, siIDs []string) {
	t.Helper()
	assertLen(t, siIDs, 10, "SI sparse_amt ASC Limit(10) should return exactly 10 items")
	want := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		want = append(want, fmt.Sprintf("p%03d", (i*11)%100))
	}
	assertEqual(t, want, siIDs, "SI sparse_amt ASC first 10 must be p000 p011 p022 … p099 (cycle every 11)")
}

func AssertScoreEqSKId(t AssertableT, siAsc, skAsc, siDesc, skDesc []string) {
	t.Helper()
	assertEqual(t, skAsc, siAsc, "SI score ASC must strictly equal SK id ASC (score==id in probe dataset Δ1/Δ2)")
	assertEqual(t, skDesc, siDesc, "SI score DESC must strictly equal SK id DESC")
	assertTrue(t, XStrictMonotonic(siAsc, false), "SI score ASC must be lex monotonic")
}

func atoi(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			continue
		}
		n = n*10 + int(s[i]-'0')
	}
	return n
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

	path := filepath.Join(filepath.Dir(self), "testdata", "key_range.jsonc")
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

var _ require.TestingT = (testing.TB)(nil)
