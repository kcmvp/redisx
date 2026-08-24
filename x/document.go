package x

import (
	"errors"
	"fmt"
	"strings"
	"time"

	naming "github.com/kcmvp/redisx/internal/naming"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Package x provides the building-block types for registering typed Redisx
// document types and building query / index / update primitives.
//
// The primary entry points are:
//
//   - Define a [Schema] / [Document] (string-alias types with zero-value methods)
//   - Use [Key] or [StorageKey] to resolve storage keys from a raw JSON payload
//   - Use [Idx] to declare a registered JSON index for search flows
//   - Use the filter combinators ([Eq], [Gt], [And], [In], ...) and [Set]
//     mutations to compose read and write operations.
//
// All naming decisions (storage namespace shape, ns:pk separator, multi-segment
// pk join character, internal meta-key prefixes, and the validators used by
// Strict Gate) live inside the package-private internal/naming subpackage and
// are NEVER exported to consumers of package x directly. Consumers build keys
// through the typed helpers declared in this package only, which keeps the
// naming contract free-form from accidental ad-hoc string concatenation.

// Schema declares the runtime-passable metadata contract for one JSON document
// type. A zero-value of your string-alias type (e.g. UserDoc("")) fully
// implements Schema — the receiver is never read, only used for interface
// dispatch.
//
// Schema is intentionally free of type elements (no ~string) so it can be used
// as a value type: variadic params, slices, fields all work.
//
// Example:
//
//	server.Start("redisx.yaml",
//	    UserDoc(""),
//	    OrderDoc(""),
//	)
type Schema interface {
	// Namespace returns the key prefix of this document type, such as "user".
	//
	// Final keys are stored as:
	//
	//	namespace:key_suffix
	//
	// Keep it short, because it is repeated in every stored key.
	Namespace() string
	// Mem reports whether this document type lives in the memory-only layer.
	//
	// When true, the final key is automatically prefixed with "_m_:" before
	// [Schema.Namespace] (using the canonical mem-layer marker defined by the
	// naming single-source-of-truth), resulting in:
	//
	//	user:1   -> _m_:user:1
	Mem() bool
	// KeyAttrs returns the ordered JSON paths used to derive the key suffix,
	// such as []string{"tenant", "id"}.
	//
	// Their resolved values are joined with the canonical pk-join character
	// defined by the naming single-source-of-truth in order.
	KeyAttrs() []string
	// TTL returns the default TTL used by typed write helpers.
	//
	// It does not participate in key derivation.
	TTL() time.Duration
}

// Document declares the full generic-constraint contract for one JSON document
// type. It extends Schema with ~string (so typed helpers can cast loaded raw
// JSON payloads back to the concrete alias type) and RawJSON (needed to build
// storage keys from a populated payload instance).
//
// A string-alias type that implements Document automatically satisfies Schema
// as well — there is zero extra code to write when defining a new document.
//
// Example:
//
//	type UserDoc string
//
//	func (UserDoc) Namespace() string  { return "user" }
//	func (UserDoc) Mem() bool          { return false }
//	func (UserDoc) KeyAttrs() []string { return []string{"id"} }
//	func (u UserDoc) RawJSON() string  { return string(u) }
//	func (UserDoc) TTL() time.Duration { return time.Hour }
type Document interface {
	~string
	Schema
	// RawJSON returns the raw JSON payload of this document value.
	//
	// [StorageKey] reads it together with [Schema.KeyAttrs] to build one full
	// storage key for the current document instance.
	RawJSON() string
}

// Index describes one registered JSON index definition.
//
// It contains the fully-qualified runtime index name, the full storage-key
// pattern it applies to, and the normalized JSON path used by the backend
// index.
type Index struct {
	name       string
	keyPattern string
	path       string
}

// Name returns the fully-qualified runtime index name.
func (d Index) Name() string { return d.name }

// KeyPattern returns the full storage-key pattern bound to this index.
func (d Index) KeyPattern() string { return d.keyPattern }

// Path returns the normalized JSON path used by the backend index.
func (d Index) Path() string { return d.path }

// StorageKey resolves one full storage key directly from the document.
//
// Each path declared by [Document.KeyAttrs] must exist in [Document.RawJSON].
// When multiple attributes are declared, their resolved values are joined with
// the canonical pk-join character declared by [JoinPKAttrValues] to form the
// key value part. Boolean key attrs are normalized to "1" and "0" so generated
// keys stay compact and stable.
//
// This function delegates all shape decisions (storageNs prefix, ns:pk
// separator, multi-segment pk join character) to the naming SSoT.
func StorageKey[D Document](d D) (string, error) {
	return Key[D](d.RawJSON())
}

// Key resolves one full storage key for document type D using the given raw
// JSON payload string.
//
// This is the primary typed-helper entry point for callers that do not have a
// concrete document alias instance yet (e.g. a raw JSON string received from
// RESP / CLI / HTTP). All shape decisions are delegated to the naming SSoT;
// the x package never writes any separator literal directly.
//
// Returns an error if D declares an invalid namespace (per
// [ValidateDocLogicalNamespace]), a KeyAttrs path is empty, or any declared
// KeyAttr is missing from the provided raw JSON payload.
func Key[D Schema](rawJSON string) (string, error) {
	var d D
	ns := d.Namespace()
	mem := d.Mem()

	if err := naming.ValidateDocLogicalNamespace(ns); err != nil {
		return "", fmt.Errorf("document namespace invalid: %w", err)
	}
	paths := d.KeyAttrs()
	for _, p := range paths {
		if p == "" {
			return "", fmt.Errorf("key attr path is empty")
		}
	}

	storageNs := naming.BuildStorageNs(ns, mem)

	if len(paths) == 0 {
		return naming.BuildStorageKey(storageNs, ""), nil
	}

	parts := make([]string, 0, len(paths))
	for _, path := range paths {
		result := gjson.Get(rawJSON, path)
		if !result.Exists() {
			return "", fmt.Errorf("missing key attr: %s", path)
		}
		parts = append(parts, keyAttrValue(result))
	}

	return naming.BuildStorageKey(storageNs, naming.JoinPKAttrValues(parts)), nil
}

func keyAttrValue(result gjson.Result) string {
	if result.Type == gjson.True {
		return "1"
	}
	if result.Type == gjson.False {
		return "0"
	}
	return result.String()
}

func validateStorageNs[D Document]() string {
	var d D
	ns := d.Namespace()
	mem := d.Mem()
	if err := naming.ValidateDocLogicalNamespace(ns); err != nil {
		panic(fmt.Sprintf("x.validateStorageNs: %v", err))
	}
	for _, p := range d.KeyAttrs() {
		if p == "" {
			panic("x.validateStorageNs: document key attr path is empty")
		}
	}
	return naming.BuildStorageNs(ns, mem)
}

// StorageKeyValue joins a resolved key value with the document namespace
// derived from the document metadata type.
//
// This function is intended for concrete pkSuffix values such as an already
// joined pk string. For glob patterns and index key-patterns (which may
// legitimately contain reserved characters such as '*' or literal ':' in the
// pattern payload) use [StorageNsKeyPattern] or [RawIndex] respectively.
func StorageKeyValue[D Document](v string) string {
	storageNs := validateStorageNs[D]()
	return naming.BuildStorageKey(storageNs, v)
}

// StorageNsKeyPattern returns a canonical storage-key glob pattern that matches
// every key belonging to document type D, appending the caller-provided
// pattern suffix (for example "*", "tenant_*") to the storage namespace using
// the canonical ns:pk separator.
//
// Unlike [StorageKeyValue] this function does NOT validate the suffix because
// patterns intentionally contain wildcards and literal ':' characters that
// would be invalid in a concrete pk value.
//
// This is the Schema-generic version. For a raw non-generic wrapper that
// accepts an already-built storageNs string and always appends a "*" glob
// suffix, see the identically-named helper inside the package-private
// x/internal/naming.go SSoT.
func StorageNsKeyPattern[D Schema](patternSuffix string) string {
	var d D
	ns := d.Namespace()
	mem := d.Mem()
	if err := naming.ValidateDocLogicalNamespace(ns); err != nil {
		panic(fmt.Sprintf("x.StorageNsKeyPattern: %v", err))
	}
	storageNs := naming.BuildStorageNs(ns, mem)
	base := naming.StorageNsScope(storageNs)
	return base + patternSuffix
}

// MemKey returns a key routed to the memory-only storage layer.
//
// The returned key uses the reserved mem-layer marker declared by the naming
// SSoT. If key already uses that marker, it is returned unchanged.
func MemKey(key string) string {
	if naming.HasMemPrefix(key) {
		return key
	}
	ns, pk, err := naming.SplitStorageKey(key)
	if err != nil {
		return key
	}
	if naming.IsInternalStorageNs(ns) {
		return key
	}
	underlying, isMem := naming.StripMemPrefixIfMem(ns)
	if !isMem {
		if underlying == "" {
			return key
		}
		if err := naming.ValidateDocLogicalNamespace(underlying); err != nil {
			return key
		}
		memNs := naming.BuildStorageNs(underlying, true)
		return naming.BuildStorageKey(memNs, pk)
	}
	return key
}

// ============================================================
// Index section: moved here from x.go to keep document helpers together
// with document-only index-scope helpers.
// ============================================================

// RawIndex constructs one Index directly without binding to a document type.
//
// Callers are responsible for passing a fully-qualified index name (same shape
// as IdxFullName would produce) and a full storage-key pattern (same shape as
// StorageKeyValue would produce).
//
// The jsonPath argument accepts dot-separated JSON paths (same style as [Idx]):
// dots are normalized automatically by replacing '.' with '_' so callers do NOT
// need to pre-replace them. Pre-replacing is harmless but unnecessary.
//
// If any argument is empty RawIndex panics.
//
// RawIndex is intended for pure-data fixtures that register indexes over a
// manually-keyed KV space without needing a concrete [Document] type (e.g.
// shared SEARCHKEY / SEARCHINDEX parity suites).
func RawIndex(name string, keyPattern string, jsonPath string) Index {
	if name == "" {
		panic("RawIndex: name is required")
	}
	if keyPattern == "" {
		panic("RawIndex: keyPattern is required")
	}
	if jsonPath == "" {
		panic("RawIndex: jsonPath is required")
	}
	return Index{
		name:       strings.ToLower(name),
		keyPattern: keyPattern,
		path:       strings.ReplaceAll(jsonPath, ".", "_"),
	}
}

// IdxFullName returns the fully-qualified index name for one document type and
// logical index name.
//
// All character-set and separator decisions are delegated to the naming SSoT.
func IdxFullName[D Document](idxName string) string {
	if idxName == "" {
		panic("index name is required")
	}
	storageNs := validateStorageNs[D]()
	full := naming.BuildIdxFullName(storageNs, strings.ToLower(idxName))
	return strings.ToLower(full)
}

// Idx declares one document-scoped JSON index.
//
// The runtime index name stored inside the system is always normalized as:
//
//	namespace_idxname
//
// where namespace comes from D, idxName is lowercased, and the final result is
// entirely lowercase.
func Idx[D Document](idxName string, keyPattern string, jsonPath string) Index {
	if keyPattern == "" {
		panic("index key pattern is required")
	}
	if strings.HasPrefix(keyPattern, ":") {
		panic("index key pattern must not start with separator")
	}
	if jsonPath == "" {
		panic("index json path is required")
	}
	jsonPath = strings.ReplaceAll(jsonPath, ".", "_")

	return Index{
		name:       IdxFullName[D](idxName),
		keyPattern: StorageNsKeyPattern[D](keyPattern),
		path:       jsonPath,
	}
}

// ValidateKeyPattern resolves one document-scoped key pattern into its full
// storage pattern. It rejects already-prefixed storage patterns because the
// document type itself already determines the namespace prefix.
func ValidateKeyPattern[D Document](keyPattern string) (string, error) {
	fullNamespace := StorageKeyValue[D]("")
	fullPrefix := naming.StorageNsScope(fullNamespace)
	if keyPattern == fullNamespace || strings.HasPrefix(keyPattern, fullPrefix) {
		return "", fmt.Errorf("key pattern must be document-scoped, got storage pattern: %s", keyPattern)
	}
	return StorageKeyValue[D](keyPattern), nil
}

// ValidateIdxName resolves one logical document index name into its full
// runtime name. It rejects already-prefixed index names because the document
// type itself already determines the namespace prefix.
func ValidateIdxName[D Document](idxName string) (string, error) {
	if idxName == "" {
		return "", fmt.Errorf("index name is required")
	}

	fullNamespace := strings.ToLower(StorageKeyValue[D](""))
	lowerName := strings.ToLower(idxName)
	if strings.HasPrefix(lowerName, fullNamespace+naming.KeyAttrsJoin()) {
		return "", fmt.Errorf("idx name must be logical, got fully-qualified index name: %s", idxName)
	}
	return IdxFullName[D](idxName), nil
}

// ============================================================
// Mutation codec section: wire serialization for x.Mutation.
//
// Client calls MarshalUpdate to convert []Mutation → wire JSON bytes
// before sending UPDATE. Server calls ParseUpdate on the receiving end
// to convert wire JSON → []Mutation → apply to the matched docs.
// These helpers live alongside Mutation/Set (defined in filter.go) so
// the "type definition + wire codec" for UPDATE stays in one package.
// ============================================================

// MarshalUpdate converts mutations into the update JSON accepted by UPDATE.
func MarshalUpdate(values ...Mutation) ([]byte, error) {
	if len(values) == 0 {
		return nil, errors.New("no update values provided")
	}

	doc := "{}"
	var err error
	for _, value := range values {
		doc, err = sjson.Set(doc, value.Path(), value.Value())
		if err != nil {
			return nil, err
		}
	}
	return []byte(doc), nil
}

// ParseUpdate converts an update JSON object into mutations.
func ParseUpdate(jsonStr string) ([]Mutation, error) {
	if !gjson.Valid(jsonStr) {
		return nil, errors.New("invalid update json format")
	}

	root := gjson.Parse(jsonStr)
	if root.Type != gjson.JSON {
		return nil, errors.New("update must be a json object")
	}

	var pairs []Mutation
	var walk func(prefix string, node gjson.Result)
	walk = func(prefix string, node gjson.Result) {
		switch node.Type {
		case gjson.String:
			pairs = append(pairs, Set(prefix, node.String()))
		case gjson.Number:
			pairs = append(pairs, Set(prefix, node.Float()))
		case gjson.True:
			pairs = append(pairs, Set(prefix, true))
		case gjson.False:
			pairs = append(pairs, Set(prefix, false))
		case gjson.JSON:
			if node.IsObject() {
				node.ForEach(func(key, value gjson.Result) bool {
					next := key.String()
					if prefix != "" {
						next = prefix + "." + next
					}
					walk(next, value)
					return true
				})
			}
		}
	}

	root.ForEach(func(key, value gjson.Result) bool {
		walk(key.String(), value)
		return true
	})

	if len(pairs) == 0 {
		return nil, errors.New("no valid updates provided")
	}
	return pairs, nil
}
