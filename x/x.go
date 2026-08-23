package x

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"
)

// SECTION: Filter

// Expression represents a MongoDB-style JSON query expression.
type Expression map[string]any

// Filter is a pure function that evaluates a JSON record string.
type Filter interface {
	// Eval evaluates the JSON record and returns true if it matches the filter.
	Eval(jsonRecord string) bool
	// MarshalJSON serializes the filter into a MongoDB-style JSON expression.
	MarshalJSON() ([]byte, error)
}

type filterFunc struct {
	eval func(jsonRecord string) bool
	expr Expression
}

func (f *filterFunc) Eval(jsonRecord string) bool {
	return f.eval(jsonRecord)
}

func (f *filterFunc) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.expr)
}

// And returns a Filter that passes if all provided filters pass.
func And(filters ...Filter) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			for _, f := range filters {
				if !f.Eval(jsonRecord) {
					return false
				}
			}
			return true
		},
		expr: Expression{"$and": filters},
	}
}

// Or returns a Filter that passes if at least one of the provided filters passes.
func Or(filters ...Filter) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			for _, f := range filters {
				if f.Eval(jsonRecord) {
					return true
				}
			}
			return false
		},
		expr: Expression{"$or": filters},
	}
}

// Not returns a Filter that passes if the provided filter fails.
func Not(f Filter) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			return !f.Eval(jsonRecord)
		},
		expr: Expression{"$not": f},
	}
}

// SECTION: Comparator

// Eq returns a filter that passes if the JSON record's field equals the provided value.
func Eq(field string, value any) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() {
				return false
			}
			return rs.Value() == value
		},
		expr: Expression{field: Expression{"$eq": value}},
	}
}

// Neq returns a filter that passes if the JSON record's field does not equal the provided value.
func Neq(field string, value any) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() {
				return true
			}
			return rs.Value() != value
		},
		expr: Expression{field: Expression{"$neq": value}},
	}
}

// Gt returns a filter that passes if the JSON record's field is strictly greater than the provided value.
func Gt(field string, value float64) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.Number {
				return false
			}
			return rs.Float() > value
		},
		expr: Expression{field: Expression{"$gt": value}},
	}
}

// Gte returns a filter that passes if the JSON record's field is greater than or equal to the provided value.
func Gte(field string, value float64) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.Number {
				return false
			}
			return rs.Float() >= value
		},
		expr: Expression{field: Expression{"$gte": value}},
	}
}

// Lt returns a filter that passes if the JSON record's field is strictly less than the provided value.
func Lt(field string, value float64) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.Number {
				return false
			}
			return rs.Float() < value
		},
		expr: Expression{field: Expression{"$lt": value}},
	}
}

// Lte returns a filter that passes if the JSON record's field is less than or equal to the provided value.
func Lte(field string, value float64) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.Number {
				return false
			}
			return rs.Float() <= value
		},
		expr: Expression{field: Expression{"$lte": value}},
	}
}

// Contains returns a filter that passes if the JSON record's field is a string and contains the provided substring.
func Contains(field string, substring string) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.String {
				return false
			}
			return strings.Contains(rs.String(), substring)
		},
		expr: Expression{field: Expression{"$contains": substring}},
	}
}

// In returns a filter that passes if the JSON record's field equals any of the provided values.
func In[T comparable](field string, values ...T) Filter {
	anyValues := lo.Map(values, func(v T, _ int) any { return v })

	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() {
				return false
			}
			val := rs.Value()
			for _, v := range anyValues {
				if val == v {
					return true
				}
			}
			return false
		},
		expr: Expression{field: Expression{"$in": anyValues}},
	}
}

// SECTION: Mutation

// Mutation describes one JSON path update used by UPDATE flows.
type Mutation interface {
	Path() string
	Value() any
}

// ScalarType limits Set values to the scalar types currently supported by
// redisx update operations.
type ScalarType interface {
	~int | ~int32 | ~int64 | ~float32 | ~float64 | ~string | ~bool
}

type pair[T ScalarType] struct {
	path string
	val  T
}

func (v pair[T]) Path() string { return v.path }

func (v pair[T]) Value() any { return v.val }

// Set creates one JSON path update payload.
//
// Example:
//
//	x.Set("status", "active")
//	x.Set("profile.age", 18)
func Set[T ScalarType](path string, value T) Mutation {
	return pair[T]{path: path, val: value}
}

// SECTION: Document And Index

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
	// When true, the final key is automatically prefixed with "_m_" before
	// [Schema.Namespace], so:
	//
	//	user:1   -> _m_user:1
	Mem() bool
	// KeyAttrs returns the ordered JSON paths used to derive the key suffix,
	// such as []string{"tenant", "id"}.
	//
	// Their resolved values are joined with ":" in order.
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

// StorageKey resolves one full storage key directly from the document.
//
// Each path declared by [Document.KeyAttrs] must exist in [Document.RawJSON].
// When multiple attributes are declared, their resolved values are joined with
// ":" to form the key value part. Boolean key attrs are normalized to "1" and
// "0" so generated keys stay compact and stable.
func StorageKey[D Document](d D) (string, error) {
	paths := d.KeyAttrs()
	if len(paths) == 0 {
		return StorageKeyValue[D](""), nil
	}

	parts := make([]string, 0, len(paths))
	for _, path := range paths {
		if path == "" {
			return "", fmt.Errorf("key attr path is empty")
		}

		result := gjson.Get(d.RawJSON(), path)
		if !result.Exists() {
			return "", fmt.Errorf("missing key attr: %s", path)
		}

		parts = append(parts, keyAttrValue(result))
	}

	return StorageKeyValue[D](strings.Join(parts, StorageKeySeparator)), nil
}

// keyAttrValue normalizes one extracted key attr into its storage-key form.
//
// Booleans are encoded as "1" and "0" to keep generated keys compact and
// stable.
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
	if ns == "" {
		panic("document namespace is required")
	}
	if strings.ContainsAny(ns, StorageKeySeparator+"*?_") {
		panic("document namespace must not contain reserved characters")
	}
	for _, p := range d.KeyAttrs() {
		if p == "" {
			panic("document key attr path is empty")
		}
	}
	if d.Mem() {
		return MemNsPrefix + ns
	}
	return ns
}

// StorageKeyValue joins a resolved key value with the document namespace
// derived from the document metadata type.
func StorageKeyValue[D Document](v string) string {
	storageNs := validateStorageNs[D]()
	if v == "" {
		return storageNs
	}
	return storageNs + StorageKeySeparator + v
}

// MemKey returns a key routed to the memory-only storage layer.
//
// The returned key uses the reserved "_m_" prefix. If key already uses that
// prefix, it is returned unchanged.
func MemKey(key string) string {
	if strings.HasPrefix(key, MemNsPrefix) {
		return key
	}
	return MemNsPrefix + key
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
func (d Index) Name() string {
	return d.name
}

// KeyPattern returns the full storage-key pattern bound to this index.
func (d Index) KeyPattern() string {
	return d.keyPattern
}

// Path returns the normalized JSON path used by the backend index.
func (d Index) Path() string {
	return d.path
}

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
func IdxFullName[D Document](idxName string) string {
	if idxName == "" {
		panic("index name is required")
	}
	fullName := strings.ToLower(idxName)
	storageNs := validateStorageNs[D]()
	if storageNs != "" {
		fullName = strings.ToLower(storageNs) + "_" + fullName
	}
	return fullName
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
	if strings.HasPrefix(keyPattern, StorageKeySeparator) {
		panic("index key pattern must not start with separator")
	}
	if jsonPath == "" {
		panic("index json path is required")
	}
	jsonPath = strings.ReplaceAll(jsonPath, ".", "_")

	return Index{
		name:       IdxFullName[D](idxName),
		keyPattern: StorageKeyValue[D](keyPattern),
		path:       jsonPath,
	}
}
