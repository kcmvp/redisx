package naming

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	storageKeySeparator = ":"
	memNsPrefix         = "_m_"
	keyAttrsJoin        = "_"

	docMetaNsPrefix = "_doc_"
	idxMetaNsPrefix = "_idx_"
	authNsPrefix    = "_auth_"

	globSuffix = "*"
)

var (
	docLogicalNsRegex = regexp.MustCompile(`^[a-z][a-z0-9]{0,62}$`)
	logicalIdxRegex   = regexp.MustCompile(`^[a-z][a-z0-9]{0,62}$`)
)

// ============================================================
// Public Getters — the canonical separators/prefixes are stored as private
// constants above (the single source of truth). These exported getter
// functions are the ONLY supported way to read the naming contract values
// from outside this package. Public constant-based access (previously via
// x/constant.go exported constants) has been removed entirely to eliminate
// ad-hoc string concatenation that bypasses naming validators.
// ============================================================

func StorageKeySeparator() string { return storageKeySeparator }
func MemNsPrefix() string         { return memNsPrefix }
func KeyAttrsJoin() string        { return keyAttrsJoin }
func DocMetaNsPrefix() string     { return docMetaNsPrefix }
func IdxMetaNsPrefix() string     { return idxMetaNsPrefix }
func AuthNsPrefix() string        { return authNsPrefix }

func BuildStorageNs(logical string, mem bool) string {
	if err := ValidateDocLogicalNamespace(logical); err != nil {
		panic(fmt.Sprintf("naming.BuildStorageNs: %v", err))
	}
	if mem {
		return memNsPrefix + storageKeySeparator + logical
	}
	return logical
}

func BuildStorageKey(storageNs, pkSuffix string) string {
	if storageNs == "" {
		panic("naming.BuildStorageKey: storageNs is required")
	}
	if strings.ContainsRune(pkSuffix, ':') {
		panic(fmt.Sprintf("naming.BuildStorageKey: pkSuffix %q contains illegal ':' (storage-key separator); multi-segment pk must join with '%s'", pkSuffix, keyAttrsJoin))
	}
	if pkSuffix == "" {
		return storageNs
	}
	return storageNs + storageKeySeparator + pkSuffix
}

func JoinPKAttrValues(values []string) string {
	return strings.Join(values, keyAttrsJoin)
}

func BuildIdxFullName(storageNs, logical string) string {
	if storageNs == "" {
		panic("naming.BuildIdxFullName: storageNs is required")
	}
	if err := ValidateLogicalIndexName(logical); err != nil {
		panic(fmt.Sprintf("naming.BuildIdxFullName: %v", err))
	}
	return storageNs + storageKeySeparator + logical
}

func DocMetaKey(storageNs string) string {
	if storageNs == "" {
		panic("naming.DocMetaKey: storageNs is required")
	}
	return docMetaNsPrefix + storageKeySeparator + storageNs
}

func IdxMetaKey(idxFullName string) string {
	if idxFullName == "" {
		panic("naming.IdxMetaKey: idxFullName is required")
	}
	return idxMetaNsPrefix + storageKeySeparator + idxFullName
}

func AuthStorageKey(keyName string) string {
	if keyName == "" {
		panic("naming.AuthStorageKey: keyName is required")
	}
	return authNsPrefix + storageKeySeparator + keyName
}

func DocMetaGlob() string     { return docMetaNsPrefix + storageKeySeparator + globSuffix }
func IdxMetaGlob() string     { return idxMetaNsPrefix + storageKeySeparator + globSuffix }
func AuthStorageGlob() string { return authNsPrefix + storageKeySeparator + globSuffix }

func StorageNsKeyPattern(storageNs string) string {
	if storageNs == "" {
		panic("naming.StorageNsKeyPattern: storageNs is required")
	}
	return storageNs + storageKeySeparator + globSuffix
}

func StorageNsScope(storageNs string) string {
	if storageNs == "" {
		panic("naming.StorageNsScope: storageNs is required")
	}
	return storageNs + storageKeySeparator
}

func SplitStorageKey(storageKey string) (storageNs, pkSuffix string, err error) {
	if storageKey == "" {
		return "", "", fmt.Errorf("empty storage key")
	}
	i := strings.LastIndex(storageKey, storageKeySeparator)
	if i < 0 {
		return storageKey, "", nil
	}
	return storageKey[:i], storageKey[i+len(storageKeySeparator):], nil
}

func StripMemPrefixIfMem(s string) (underlying string, isMem bool) {
	memFullPrefix := memNsPrefix + storageKeySeparator
	if strings.HasPrefix(s, memFullPrefix) {
		return strings.TrimPrefix(s, memFullPrefix), true
	}
	return s, false
}

// StripAuthPrefixIfAuth strips the canonical "_auth_:" prefix from a storage
// key and returns the bare auth key id plus an isAuth=true flag. If the
// storage key does not start with the auth prefix both values are returned
// unchanged and isAuth will be false.
//
// This companion helper mirrors StripMemPrefixIfMem and provides the single
// supported way to extract a raw auth key id from an auth storage key.
func StripAuthPrefixIfAuth(s string) (keyID string, isAuth bool) {
	authFullPrefix := authNsPrefix + storageKeySeparator
	if strings.HasPrefix(s, authFullPrefix) {
		return strings.TrimPrefix(s, authFullPrefix), true
	}
	return s, false
}

func ExtractPKSuffixes(pkSuffix string) ([]string, error) {
	if pkSuffix == "" {
		return nil, nil
	}
	return strings.Split(pkSuffix, keyAttrsJoin), nil
}

func ParseIdxFullName(full string) (ownerStorageNs, logical string, err error) {
	ns, log, splitErr := SplitStorageKey(full)
	if splitErr != nil {
		return "", "", fmt.Errorf("index full name %q has no join separator %q (expected shape <storageNs>%s<logical>): %w", full, storageKeySeparator, storageKeySeparator, splitErr)
	}
	if log == "" {
		return "", "", fmt.Errorf("index full name %q has no join separator %q (expected shape <storageNs>%s<logical>)", full, storageKeySeparator, storageKeySeparator)
	}
	if ns == "" {
		return "", "", fmt.Errorf("index full name %q has empty owner storage namespace", full)
	}
	return ns, log, nil
}

func IsInternalStorageNs(storageNs string) bool {
	if storageNs == "" {
		return false
	}
	return storageNs == docMetaNsPrefix ||
		storageNs == idxMetaNsPrefix ||
		storageNs == authNsPrefix
}

func HasMemPrefix(s string) bool {
	return strings.HasPrefix(s, memNsPrefix+storageKeySeparator)
}

// HasUnderscorePrefix returns true when s begins with the reserved "_" byte
// AND is not one of the recognised canonical underscore-prefixed forms
// (mem-layer storage keys starting with "_m_:" or the three bare internal
// storage namespace constants "_doc_", "_idx_", "_auth_").
//
// Use this as a strict-gate predicate for user-visible storage keys: anything
// that returns true is a candidate for a future naming expansion and must be
// blocked on user-supplied inputs.
func HasUnderscorePrefix(s string) bool {
	if s == "" {
		return false
	}
	if s[0] != '_' {
		return false
	}
	if HasMemPrefix(s) {
		return false
	}
	if IsInternalStorageNs(s) {
		return false
	}
	return true
}

func ValidateDocLogicalNamespace(ns string) error {
	if ns == "" {
		return fmt.Errorf("doc logical namespace is required")
	}
	if strings.ContainsAny(ns, storageKeySeparator+"*?-") {
		return fmt.Errorf("doc logical namespace %q contains one of the reserved characters ': * ? -' which are not allowed; canonical doc ns must match ^[a-z][a-z0-9]{0,62}$", ns)
	}
	if strings.ContainsRune(ns, '_') {
		return fmt.Errorf("doc logical namespace %q contains '_' which is reserved as the index ownership boundary separator (IdxFullName = <storageNs>_<logical>); to keep index owner resolution unambiguous, canonical doc ns must match ^[a-z][a-z0-9]{0,62}$ (underscores forbidden)", ns)
	}
	if !docLogicalNsRegex.MatchString(ns) {
		return fmt.Errorf("doc logical namespace %q is invalid; canonical doc ns must match ^[a-z][a-z0-9]{0,62}$ (starts with lowercase letter, 1-63 lowercase-letter-or-digit chars, no separators)", ns)
	}
	return nil
}
func ValidateLogicalIndexName(logical string) error {
	if logical == "" {
		return fmt.Errorf("logical index name is required")
	}
	if strings.ContainsAny(logical, storageKeySeparator+"*?-") {
		return fmt.Errorf("logical index name %q contains one of the reserved characters ': * ? -'; canonical logical index must match ^[a-z][a-z0-9]{0,62}$", logical)
	}
	if strings.ContainsRune(logical, '_') {
		return fmt.Errorf("logical index name %q contains '_' which is reserved as the IdxFullName join character (IdxFullName = <storageNs>_<logical>); canonical logical index must match ^[a-z][a-z0-9]{0,62}$", logical)
	}
	if !logicalIdxRegex.MatchString(logical) {
		return fmt.Errorf("logical index name %q is invalid; canonical logical index must match ^[a-z][a-z0-9]{0,62}$", logical)
	}
	return nil
}

func ValidateStorageNs(storageNs string) error {
	if storageNs == "" {
		return fmt.Errorf("storage namespace is required")
	}
	if IsInternalStorageNs(storageNs) {
		return nil
	}
	underlying, isMem := StripMemPrefixIfMem(storageNs)
	if isMem {
		if strings.Contains(underlying, storageKeySeparator) {
			memFullPrefix := memNsPrefix + storageKeySeparator
			return fmt.Errorf("mem storageNs %q after stripping %q yields %q which still contains ':'; only a single mem-layer prefix '%s' is allowed", storageNs, memFullPrefix, underlying, memFullPrefix)
		}
		return ValidateDocLogicalNamespace(underlying)
	}
	if strings.HasPrefix(storageNs, "_") {
		return fmt.Errorf("storageNs %q starts with reserved leading underscore but is not a recognized internal namespace (_doc_ / _idx_ / _auth_); custom storageNs with leading underscore are forbidden to avoid future internal-namespace collisions", storageNs)
	}
	return ValidateDocLogicalNamespace(storageNs)
}
