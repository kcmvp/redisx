package internal

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/kcmvp/redisx/x"
	"github.com/kcmvp/redisx/x/contract"
)

var (
	authKeyOnce sync.Once
	authKey     string
)

func AuthKey() string {
	authKeyOnce.Do(func() {
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		authKey = hex.EncodeToString(b)
	})

	return authKey
}

// ValidateKeyPattern resolves one document-scoped key pattern into its full
// storage pattern. It rejects already-prefixed storage patterns because the
// document type itself already determines the namespace prefix.
func ValidateKeyPattern[D x.Document](keyPattern string) (string, error) {
	fullNamespace := x.StorageKeyValue[D]("")
	fullPrefix := fullNamespace + contract.StorageKeySeparator
	if keyPattern == fullNamespace || strings.HasPrefix(keyPattern, fullPrefix) {
		return "", fmt.Errorf("key pattern must be document-scoped, got storage pattern: %s", keyPattern)
	}
	return x.StorageKeyValue[D](keyPattern), nil
}

// scopeWireKeyRangeShape is the RESP-wire JSON shape for a sealed x.KeyRange.
// Marshal → Unmarshal → re-prefix → rebuild via exported x ctors (avoids
// touching unexported private fields on the sealed concrete types in x/keys.go,
// which private_key_range_seal() prevents external packages from constructing).
type scopeWireKeyRangeShape struct {
	Op    string `json:"op"`
	Ge    string `json:"ge,omitempty"`
	Lt    string `json:"lt,omitempty"`
	Pivot string `json:"pivot,omitempty"`
	P     string `json:"p,omitempty"`
}

// ScopeKeyRange resolves one document-scoped x.KeyRange into its fully
// qualified storage key range. It rejects any field value that already starts
// with the document namespace prefix (mirroring ValidateKeyPattern on the
// legacy string path). Every string field in the 6 sealed ctors is prefixed
// with the D namespace + separator, and any LIMIT set on the input scopedKR
// is carried onto the returned sealed range via .Limit(old).
func ScopeKeyRange[D x.Document](scopedKR x.KeyRange) (x.KeyRange, error) {
	fullNamespace := x.StorageKeyValue[D]("")
	fullPrefix := fullNamespace + contract.StorageKeySeparator

	checkPrefixed := func(fieldName, v string) error {
		if v == fullNamespace || strings.HasPrefix(v, fullPrefix) {
			return fmt.Errorf("key range %s must be document-scoped, got prefixed storage value: %s", fieldName, v)
		}
		return nil
	}
	prefix := func(s string) string {
		if s == "" {
			return fullNamespace
		}
		return fullPrefix + s
	}

	wireBytes, err := scopedKR.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("scoped key range marshal: %w", err)
	}
	var wire scopeWireKeyRangeShape
	if err := json.Unmarshal(wireBytes, &wire); err != nil {
		return nil, fmt.Errorf("scoped key range unmarshal: %w", err)
	}

	var built x.KeyRange
	switch strings.ToLower(wire.Op) {
	case "bt":
		if wire.Ge != "" {
			if perr := checkPrefixed("ge", wire.Ge); perr != nil {
				return nil, perr
			}
		}
		if wire.Lt != "" {
			if perr := checkPrefixed("lt", wire.Lt); perr != nil {
				return nil, perr
			}
		}
		built = x.KeysBt(prefix(wire.Ge), prefix(wire.Lt))
	case "gt":
		if perr := checkPrefixed("pivot", wire.Pivot); perr != nil {
			return nil, perr
		}
		built = x.KeysGt(prefix(wire.Pivot))
	case "gte":
		if perr := checkPrefixed("pivot", wire.Pivot); perr != nil {
			return nil, perr
		}
		built = x.KeysGte(prefix(wire.Pivot))
	case "lt":
		if perr := checkPrefixed("pivot", wire.Pivot); perr != nil {
			return nil, perr
		}
		built = x.KeysLt(prefix(wire.Pivot))
	case "lte":
		if perr := checkPrefixed("pivot", wire.Pivot); perr != nil {
			return nil, perr
		}
		built = x.KeysLte(prefix(wire.Pivot))
	case "pattern":
		if perr := checkPrefixed("p", wire.P); perr != nil {
			return nil, perr
		}
		built = x.KeysPattern(prefix(wire.P))
	default:
		return nil, fmt.Errorf("unknown key range op: %s", wire.Op)
	}

	if carry := scopedKR.GetLimit(); carry != -1 {
		built = built.Limit(carry)
	}
	return built, nil
}

// ValidateIdxName resolves one logical document index name into its full
// runtime name. It rejects already-prefixed index names because the document
// type itself already determines the namespace prefix.
func ValidateIdxName[D x.Document](idxName string) (string, error) {
	if idxName == "" {
		return "", fmt.Errorf("index name is required")
	}

	fullNamespace := strings.ToLower(x.StorageKeyValue[D](""))
	lowerName := strings.ToLower(idxName)
	if strings.HasPrefix(lowerName, fullNamespace+"_") {
		return "", fmt.Errorf("idx name must be logical, got fully-qualified index name: %s", idxName)
	}
	return x.IdxFullName[D](idxName), nil
}

// ——————————————————— §11 SSoT Naming Helpers (Non-Generic, Server & Shell Shared) ———————————————————

// SplitStorageKey splits a fully-qualified storage key (storageNs:pkSuffix or
// just storageNs with no pk suffix) into its two canonical components.
//
// Input forms accepted:
//   - "user"                  → ("user", "", nil)      — namespace-only, no pk
//   - "user:acme_0100"        → ("user", "acme_0100", nil)  — namespace : pk-suffix
//   - "_m_user:tenant5_id99"  → ("_m_user", "tenant5_id99", nil)  — mem prefix included as part of storageNs
//   - "_doc_user"             → ("_doc_user", "", nil)        — internal meta key (no separator)
//
// It returns an error if the key is empty. It does NOT reject internal
// meta namespaces (callers use [IsInternalStorageNs] for that).
//
// NOTE: This is the ONE AND ONLY place a storage key is split on ":". No
// other file may ad-hoc strings.Split(k, ":"). This is the SSoT.
func SplitStorageKey(storageKey string) (storageNs string, pkSuffix string, err error) {
	if storageKey == "" {
		return "", "", fmt.Errorf("empty storage key")
	}
	sep := contract.StorageKeySeparator
	idx := strings.Index(storageKey, sep)
	if idx < 0 {
		return storageKey, "", nil
	}
	return storageKey[:idx], storageKey[idx+len(sep):], nil
}

// IsInternalStorageNs reports whether the given storage namespace (the first
// component returned by [SplitStorageKey]) refers to an internal meta
// keyspace reserved by redisx itself, i.e. one of:
//
//   - _doc_*   (Doc registry metadata)
//   - _idx_*   (Index registry metadata)
//   - _auth_*  (Auth key metadata)
//
// NOTE: The memory-layer prefix "_m_" is NOT treated as internal here —
// "_m_user" is a valid business document namespace routed to the mem layer,
// and Strict Gate still requires it to be registered. "_m_" is a layer
// marker, not an internal meta-keyspace in the same sense as _doc_/_idx_/_auth_.
//
// This is the SSoT for "internal ns vs business ns". Strict Gate's
// "No Table = No Op" rule should apply only when this returns false.
func IsInternalStorageNs(storageNs string) bool {
	if storageNs == "" {
		return false
	}
	return strings.HasPrefix(storageNs, contract.DocMetaNsPrefix) ||
		strings.HasPrefix(storageNs, contract.IdxMetaNsPrefix) ||
		strings.HasPrefix(storageNs, contract.AuthNsPrefix)
}

// StripMemPrefix strips the leading "_m_" layer marker from a storage
// namespace or storage key IF present, returning the logical business
// namespace without the mem-layer hint. If the prefix is absent, the input
// is returned unchanged.
//
// Examples:
//   - StripMemPrefix("_m_user")       → "user"
//   - StripMemPrefix("_m_user:abc")   → "user:abc"  (also works for full keys)
//   - StripMemPrefix("user")          → "user"
//   - StripMemPrefix("_doc_user")     → "_doc_user"  (_doc_ is NOT a mem prefix, preserved)
//
// This is the SSoT for "_m_ stripping". Strict Gate ns resolution should
// call this before looking up the DocRegistry, so both disk-layer "user" and
// mem-layer "_m_user" resolve to the same registered Doc.
func StripMemPrefix(s string) string {
	if strings.HasPrefix(s, contract.MemNsPrefix) {
		return s[len(contract.MemNsPrefix):]
	}
	return s
}

// ExtractPKSuffixes splits a composite PK suffix string (joined with
// contract.KeyAttrsJoin = "_") back into its individual attribute segments,
// preserving order. Returns a nil slice (not empty) for empty input so
// range-callers don't need to len-check additionally for nil vs empty.
//
// Examples:
//   - ExtractPKSuffixes("acme_0100")          → ["acme", "0100"]
//   - ExtractPKSuffixes("just_one")           → ["just_one"]
//   - ExtractPKSuffixes("tenant5_id99")       → ["tenant5", "id99"]
//   - ExtractPKSuffixes("")                   → nil, nil
//
// This is the SSoT for splitting pk-suffix components. No ad-hoc
// strings.Split(s, "_") for this job anywhere else.
//
// NOTE: We intentionally do NOT perform URL-decoding / unescaping here. If
// individual pk values are allowed to contain "_" literally (rare), the
// storage contract should escape them first at write time (future work);
// MVP SSoT guarantees only the split delimiter location, not value escaping.
func ExtractPKSuffixes(pkSuffix string) ([]string, error) {
	if pkSuffix == "" {
		return nil, nil
	}
	return strings.Split(pkSuffix, contract.KeyAttrsJoin), nil
}

// ParseIndexFullName parses a fully-qualified (lowercase) registered index
// name like "user_age" or "_m_cache_hitratio" back into its two logical
// components: owner storageNs (also lowercased, mem-prefix preserved) and
// the index's logical (short) name.
//
// Formally, for registered index name = JoinKeyAttrs(lower(storageNs), lower(logical)):
//
//	ParseIndexFullName(JoinKeyAttrs(ns, log)) == (ns, log, nil)
//
// Examples:
//   - ParseIndexFullName("user_age")           → ("user",       "age",       nil)
//   - ParseIndexFullName("_m_cache_hitratio")  → ("_m_cache",   "hitratio",  nil)
//   - ParseIndexFullName("a_b_c")              → ("a_b",        "c",         nil)   // last "_" is separator; multi-underscore owner ns handled correctly
//   - ParseIndexFullName("no_underscores")     → ("", "", error) // fails: no join char
//   - ParseIndexFullName("_just_prefix_")      → ("", "", error) // fails: empty suffix part
//
// This is the SSoT for index-name decomposition. !DROPINDEX UX flow and
// registry lookup must use this instead of hand-written SplitN(..., "_", 2).
func ParseIndexFullName(fullIndexName string) (ownerStorageNs string, logicalIdxName string, err error) {
	if fullIndexName == "" {
		return "", "", fmt.Errorf("empty index full name")
	}
	join := contract.KeyAttrsJoin
	idx := strings.LastIndex(fullIndexName, join)
	if idx < 0 {
		return "", "", fmt.Errorf("index full name %q has no join separator %q (expected shape <storageNs>%s<logical>)", fullIndexName, join, join)
	}
	ownerNs := fullIndexName[:idx]
	logical := fullIndexName[idx+len(join):]
	if ownerNs == "" {
		return "", "", fmt.Errorf("index full name %q has empty owner storage namespace", fullIndexName)
	}
	if logical == "" {
		return "", "", fmt.Errorf("index full name %q has empty logical suffix", fullIndexName)
	}
	return ownerNs, logical, nil
}
