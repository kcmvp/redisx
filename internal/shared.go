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
