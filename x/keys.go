package x

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tidwall/match"
)

// RangeDirection selects ascending or descending walk for storage layer
// iteration.
type RangeDirection uint8

const (
	RangeAsc RangeDirection = iota
	RangeDesc
)

type KeyRangeKind uint8

const (
	KeyRangeInvalid KeyRangeKind = iota
	KeyRangeBt
	KeyRangeGt
	KeyRangeGte
	KeyRangeLt
	KeyRangeLte
	KeyRangePattern
)

func (k KeyRangeKind) String() string {
	switch k {
	case KeyRangeBt:
		return "bt"
	case KeyRangeGt:
		return "gt"
	case KeyRangeGte:
		return "gte"
	case KeyRangeLt:
		return "lt"
	case KeyRangeLte:
		return "lte"
	case KeyRangePattern:
		return "pattern"
	}
	return "invalid"
}

// globChars lists characters recognised as glob metacharacters by the
// tidwall/match package. A string containing any of them is treated as a
// pattern; otherwise it is a literal storage key.
const globChars = "*?["

// IsLiteral reports whether s contains no glob metacharacters (i.e. it is a
// literal storage key rather than a glob pattern).
func IsLiteral(s string) bool { return !strings.ContainsAny(s, globChars) }

func isLiteral(s string) bool { return IsLiteral(s) }

// hasLeadingWildcard reports whether the first byte of the candidate
// "anchor" returned by Pattern() / Bounds() is a glob metacharacter. A
// leading wildcard means the candidate does NOT anchor the keyspace. This
// mirrors resolvePatternLayer semantics in server/db.go exactly — first
// byte check only, metachar set limited to '*' and '?' (matches legacy).
func hasLeadingWildcard(s string) bool {
	return s != "" && (s[0] == '*' || s[0] == '?')
}

// KeyRange is the sealed primary-storage key range contract shared by every
// BTree-scan X command. The interface is sealed: the only valid implementors
// are the six concrete structs inside this file, produced by the six package
// level ctors [KeysBt], [KeysGt], [KeysGte], [KeysLt], [KeysLte], [KeysPattern].
//
// LIMIT is carried on the sealed object itself: [KeyRange.GetLimit] reports
// the configured truncation (or -1 when unset), [KeyRange.Limit] produces a
// new sealed copy of the receiver with a positive truncation value.
type KeyRange interface {
	private_key_range_seal()

	// Bounds reports the widest lo/hi dictionary anchors for this range.
	// Pattern ctors return match.Allowable(p); single-boundary ctors pad the
	// unbounded side with "" (lo) or a high sentinel string that buntdb's
	// BTree treats as +inf. If neither side is anchored (pure KeysPattern
	// with a leading wildcard), Bounds returns "", "".
	Bounds() (lo, hi string)

	// Pattern reports the canonical glob predicate for this range and
	// whether this range is "glob shaped". KeysPattern always ("p", true);
	// range ctors that contain pattern params report a merged glob that
	// covers their semantics so layer routing can inspect glob-anchored
	// vs. non-anchored ranges. Literal-only ctors return "", false.
	Pattern() (glob string, ok bool)

	// MarshalJSON produces the RESP-wire JSON shape described in the FR
	// documents fr-searchkeyrange.md / fr-searchindex.md. One of the six
	// op tokens is written; no other ops exist (sealed set).
	MarshalJSON() ([]byte, error)

	// GetLimit returns the stored LIMIT value. -1 means the caller has not
	// invoked Limit(N) on this value (no truncation, iterate normally).
	// Positive values are exactly the N previously passed to Limit.
	GetLimit() int

	// Limit returns a NEW KeyRange of the same concrete type (immutable
	// copy) where the stored truncation is n. n MUST be strictly positive;
	// values <= 0 are programming errors and panic immediately.
	Limit(n int) KeyRange
}

// InspectKeyRange exposes the sealed concrete shape of kr to callers outside
// package x (in particular, the server package iteration implementation
// that lives in server/apply_keyrange.go). The returned kind identifies
// which of the six ctors produced kr; Bt and Pattern carry both ge/lt and
// p respectively; single-boundary kinds (Gt/Gte/Lt/Lte) return their pivot
// in A, B is "". Limit is exactly kr.GetLimit().
//
// Callers MUST NOT mutate the returned strings — they are shared with the
// sealed value.
func InspectKeyRange(kr KeyRange) (kind KeyRangeKind, pivotA string, pivotB string, limit int) {
	switch r := kr.(type) {
	case keyRangeBt:
		return KeyRangeBt, r.ge, r.lt, r.limit
	case keyRangeGt:
		return KeyRangeGt, r.pivot, "", r.limit
	case keyRangeGte:
		return KeyRangeGte, r.pivot, "", r.limit
	case keyRangeLt:
		return KeyRangeLt, r.pivot, "", r.limit
	case keyRangeLte:
		return KeyRangeLte, r.pivot, "", r.limit
	case keyRangePattern:
		return KeyRangePattern, r.p, "", r.limit
	}
	// defence-in-depth: external implementors cannot satisfy the sealed
	// interface, so a correct program never reaches here.
	panic(fmt.Sprintf("InspectKeyRange: unhandled sealed KeyRange concrete %T", kr))
}

// ——————————————————— 6 sealed concrete types ———————————————————

type keyRangeBt struct {
	ge, lt string
	limit  int
}

func (keyRangeBt) private_key_range_seal() {}

func (r keyRangeBt) Bounds() (lo, hi string) { return r.ge, r.lt }

func (r keyRangeBt) Pattern() (string, bool) {
	if isLiteral(r.ge) && isLiteral(r.lt) {
		return "", false
	}
	// Merge predicate used only by layer routing to detect leading-wildcard
	// cases. For storage-key scanning Apply does the real work.
	parts := make([]string, 0, 2)
	if !isLiteral(r.ge) {
		parts = append(parts, r.ge)
	}
	if !isLiteral(r.lt) {
		parts = append(parts, r.lt)
	}
	return strings.Join(parts, "|"), true
}

func (r keyRangeBt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Op string `json:"op"`
		Ge string `json:"ge"`
		Lt string `json:"lt"`
	}{"bt", r.ge, r.lt})
}

func (r keyRangeBt) GetLimit() int { return r.limit }

func (r keyRangeBt) Limit(n int) KeyRange {
	if n <= 0 {
		panic("KeyRange.Limit: n <= 0")
	}
	r.limit = n
	return r
}

// ————— single boundary —————

type keyRangeGt struct {
	pivot string
	limit int
}

func (keyRangeGt) private_key_range_seal() {}

func (r keyRangeGt) Bounds() (lo, hi string) { return NextLex(r.pivot), "" }

func (r keyRangeGt) Pattern() (string, bool) {
	if isLiteral(r.pivot) {
		return "", false
	}
	return r.pivot, true
}

func (r keyRangeGt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Op    string `json:"op"`
		Pivot string `json:"pivot"`
	}{"gt", r.pivot})
}

func (r keyRangeGt) GetLimit() int { return r.limit }

func (r keyRangeGt) Limit(n int) KeyRange {
	if n <= 0 {
		panic("KeyRange.Limit: n <= 0")
	}
	r.limit = n
	return r
}

type keyRangeGte struct {
	pivot string
	limit int
}

func (keyRangeGte) private_key_range_seal() {}

func (r keyRangeGte) Bounds() (lo, hi string) { return r.pivot, "" }

func (r keyRangeGte) Pattern() (string, bool) {
	if isLiteral(r.pivot) {
		return "", false
	}
	return r.pivot, true
}

func (r keyRangeGte) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Op    string `json:"op"`
		Pivot string `json:"pivot"`
	}{"gte", r.pivot})
}

func (r keyRangeGte) GetLimit() int { return r.limit }

func (r keyRangeGte) Limit(n int) KeyRange {
	if n <= 0 {
		panic("KeyRange.Limit: n <= 0")
	}
	r.limit = n
	return r
}

type keyRangeLt struct {
	pivot string
	limit int
}

func (keyRangeLt) private_key_range_seal() {}

func (r keyRangeLt) Bounds() (lo, hi string) { return "", r.pivot }

func (r keyRangeLt) Pattern() (string, bool) {
	if isLiteral(r.pivot) {
		return "", false
	}
	return r.pivot, true
}

func (r keyRangeLt) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Op    string `json:"op"`
		Pivot string `json:"pivot"`
	}{"lt", r.pivot})
}

func (r keyRangeLt) GetLimit() int { return r.limit }

func (r keyRangeLt) Limit(n int) KeyRange {
	if n <= 0 {
		panic("KeyRange.Limit: n <= 0")
	}
	r.limit = n
	return r
}

type keyRangeLte struct {
	pivot string
	limit int
}

func (keyRangeLte) private_key_range_seal() {}

func (r keyRangeLte) Bounds() (lo, hi string) { return "", NextLex(r.pivot) }

func (r keyRangeLte) Pattern() (string, bool) {
	if isLiteral(r.pivot) {
		return "", false
	}
	return r.pivot, true
}

func (r keyRangeLte) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Op    string `json:"op"`
		Pivot string `json:"pivot"`
	}{"lte", r.pivot})
}

func (r keyRangeLte) GetLimit() int { return r.limit }

func (r keyRangeLte) Limit(n int) KeyRange {
	if n <= 0 {
		panic("KeyRange.Limit: n <= 0")
	}
	r.limit = n
	return r
}

// ————— glob pattern catch-all —————

type keyRangePattern struct {
	p     string
	limit int
}

func (keyRangePattern) private_key_range_seal() {}

func (r keyRangePattern) Bounds() (lo, hi string) { return match.Allowable(r.p) }

func (r keyRangePattern) Pattern() (string, bool) { return r.p, true }

func (r keyRangePattern) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Op string `json:"op"`
		P  string `json:"p"`
	}{"pattern", r.p})
}

func (r keyRangePattern) GetLimit() int { return r.limit }

func (r keyRangePattern) Limit(n int) KeyRange {
	if n <= 0 {
		panic("KeyRange.Limit: n <= 0")
	}
	r.limit = n
	return r
}

// ——————————————————— 6 ctors ———————————————————

// KeysBt returns the half-open dictionary range [ge, lt). Either boundary
// may be a literal string or a glob pattern.
func KeysBt(ge, lt string) KeyRange { return keyRangeBt{ge: ge, lt: lt, limit: -1} }

// KeysGt returns the strict-greater-than range (pivot, +∞) by dictionary order.
// pivot accepts both literals and patterns.
func KeysGt(pivot string) KeyRange { return keyRangeGt{pivot: pivot, limit: -1} }

// KeysGte returns the greater-or-equal range [pivot, +∞).
func KeysGte(pivot string) KeyRange { return keyRangeGte{pivot: pivot, limit: -1} }

// KeysLt returns the strict-less-than range (-∞, pivot).
func KeysLt(pivot string) KeyRange { return keyRangeLt{pivot: pivot, limit: -1} }

// KeysLte returns the less-or-equal range (-∞, pivot].
func KeysLte(pivot string) KeyRange { return keyRangeLte{pivot: pivot, limit: -1} }

// KeysPattern returns the glob-pattern range p, matching exactly the keys
// that satisfy match.Match(key, p).
func KeysPattern(p string) KeyRange { return keyRangePattern{p: p, limit: -1} }

// ——————————————————— utilities ———————————————————

// NextLex appends a single NUL byte to s, producing the smallest string that
// is strictly lexicographically greater than s. This matches the BuntdB
// convention for representing half-open upper bounds as the lower-bound of
// the "first key after pivot".
func NextLex(s string) string { return s + "\x00" }

// EscapeGlob escapes the four glob metacharacters recognized by
// tidwall/match so a literal string can be used as a glob pattern.
func EscapeGlob(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '*', '?', '[', ']':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// ——————————————————— RESP wire deserializer ———————————————————

type wireKeyRange struct {
	Op    string `json:"op"`
	Ge    string `json:"ge,omitempty"`
	Lt    string `json:"lt,omitempty"`
	Pivot string `json:"pivot,omitempty"`
	P     string `json:"p,omitempty"`
}

// UnmarshalKeyRange parses the RESP-wire JSON shape described in the FR
// documents. The returned sealed KeyRange has GetLimit() == -1; any argc
// trailing LIMIT count argument must be applied by the caller via
//
//	kr = kr.Limit(count)
//
// after successful unmarshaling.
func UnmarshalKeyRange(data []byte) (KeyRange, error) {
	if len(data) == 0 || data[0] != '{' {
		return nil, errors.New("key range must be JSON object")
	}
	var w wireKeyRange
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("invalid key range json: %w", err)
	}
	switch strings.ToLower(w.Op) {
	case "bt":
		if w.Ge == "" && w.Lt == "" {
			return nil, errors.New("keys bt requires at least one of ge/lt")
		}
		return KeysBt(w.Ge, w.Lt), nil
	case "gt":
		if w.Pivot == "" {
			return nil, errors.New("keys gt requires pivot")
		}
		return KeysGt(w.Pivot), nil
	case "gte":
		if w.Pivot == "" {
			return nil, errors.New("keys gte requires pivot")
		}
		return KeysGte(w.Pivot), nil
	case "lt":
		if w.Pivot == "" {
			return nil, errors.New("keys lt requires pivot")
		}
		return KeysLt(w.Pivot), nil
	case "lte":
		if w.Pivot == "" {
			return nil, errors.New("keys lte requires pivot")
		}
		return KeysLte(w.Pivot), nil
	case "pattern":
		if w.P == "" {
			return nil, errors.New("keys pattern requires p")
		}
		return KeysPattern(w.P), nil
	}
	return nil, fmt.Errorf("unknown key range op: %s", w.Op)
}

// ——————————————————— shared: matches storage key (used by SEARCHINDEX) ———————————————————

// MatchesStorageKey returns true iff storageKey falls inside the receiver's
// logical key range. Bounds(), Pattern(), and the 6 ctor-specific predicates
// are unified here exactly as Apply performs them on the default storage
// index, but without touching any BTree iterator. SEARCHINDEX uses this
// helper in its secondary-index sweep callback: after extracting the real
// storage key from buntdb's composite indexKey =
// "<index_value_bytes>\x00<storage_key>", it invokes
// MatchesStorageKey(kr, storageKey) to perform the identical per-key subset
// selection SK's Apply would have produced on the default index.
//
// ORDERING: because this is pure subset selection (filter only, no sort),
// the sweep order of the parent caller (buntdb secondary-index BTree order)
// is preserved — exactly matching the same order-preservation property
// guaranteed by the per-callback matchFilter wrappers inside Apply.
func MatchesStorageKey(kr KeyRange, storageKey string) bool {
	switch r := kr.(type) {
	case keyRangeBt:
		geOK := storageKey >= r.ge || !isLiteral(r.ge) && match.Match(storageKey, r.ge)
		ltOK := storageKey < r.lt || !isLiteral(r.lt) && match.Match(storageKey, r.lt)
		return geOK && ltOK
	case keyRangeGt:
		if isLiteral(r.pivot) {
			return storageKey > r.pivot
		}
		return storageKey > r.pivot && match.Match(storageKey, r.pivot)
	case keyRangeGte:
		if isLiteral(r.pivot) {
			return storageKey >= r.pivot
		}
		return storageKey >= r.pivot && match.Match(storageKey, r.pivot)
	case keyRangeLt:
		if isLiteral(r.pivot) {
			return storageKey < r.pivot
		}
		return storageKey < r.pivot && match.Match(storageKey, r.pivot)
	case keyRangeLte:
		if isLiteral(r.pivot) {
			return storageKey <= r.pivot
		}
		return storageKey <= r.pivot && match.Match(storageKey, r.pivot)
	case keyRangePattern:
		return match.Match(storageKey, r.p)
	}
	// External implementor cannot exist (sealed via private_key_range_seal).
	// Panic as a defence-in-depth guarantee against silent regressions if
	// someone later adds a new ctor and forgets the switch case.
	panic(fmt.Sprintf("MatchesStorageKey: unhandled sealed KeyRange concrete %T", kr))
}

// LayerRoutingAnchor resolves the "primary anchor string" used by storage
// layer routing decisions. It combines Bounds() and Pattern() information
// to produce a single string whose leading-wildcard property answers the
// question "does this KeyRange, by itself, identify exactly one storage
// layer?". Callers should test hasLeadingWildcard on the returned string to
// recover SearchKey's old constrained == false behaviour.
//
// The precedence order for routing:
//
//  1. If Pattern() reports (glob, true) → use glob as the anchor (so
//     leading wildcards are preserved for routing exactly as
//     resolvePatternLayer(keyPattern) would have computed them in the
//     legacy string-only days).
//  2. Otherwise use the concatenated lo:hi Bounds string; a literal-only
//     range will have real non-wildcard bytes on both sides.
func LayerRoutingAnchor(kr KeyRange) string {
	if glob, ok := kr.Pattern(); ok {
		return glob
	}
	lo, hi := kr.Bounds()
	if lo != "" {
		return lo
	}
	return hi
}

// LayerRoutingConstrained reports whether a given KeyRange is sufficiently
// constrained to pin down a single storage layer without scanning both the
// disk and memory stores. It mirrors the behaviour of resolvePatternLayer:
// false means the caller should reject (SearchKey) or double-sweep
// (future cross-layer scans if ever desired). True, layer reports the
// pinned layer.
//
// caller is responsible for providing a layerForKey function that mirrors
// db.go's storageLayer routing (strings.HasPrefix(key, MemNsPrefix) vs.
// not). This avoids importing the server package from x.
func LayerRoutingConstrained(kr KeyRange, layerForKey func(string) any) (any /*storageLayer*/, bool) {
	anchor := LayerRoutingAnchor(kr)
	if anchor == "" || hasLeadingWildcard(anchor) {
		return nil, false
	}
	return layerForKey(anchor), true
}
