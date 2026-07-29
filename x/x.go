package x

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kcmvp/redisx/x/contract"
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

// Document declares only document metadata.
//
// It describes:
//   - the key namespace prefix used by this document type
//   - which JSON attributes are used to build the storage key
//   - the default TTL used by higher-level write helpers
//   - the raw JSON payload used by helpers that need to resolve a storage key
//     directly from the document
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
//
// Documents are JSON string aliases. The `~string` constraint guarantees typed
// helpers can cast loaded raw JSON payloads back to the concrete document type.
type Document interface {
	~string
	// Namespace returns the storage namespace for this document type,
	// such as "user".
	//
	// The returned namespace is treated as stable metadata of the document type
	// itself. It is used by [StorageKeyValue] to construct the final storage key.
	Namespace() string
	// Mem reports where this document type is stored.
	//
	// When it returns false, the document is stored in the regular persistent
	// namespace.
	//
	// When it returns true, the document is stored in the in-memory namespace,
	// and [StorageKeyValue] automatically prepends [contract.MemKeyPrefix]
	// before [Document.Namespace].
	Mem() bool
	// KeyAttrs returns the ordered JSON attribute paths used to derive the key
	// value from [Document.RawJSON], such as []string{"tenant", "id"}.
	//
	// The order is significant. When multiple attributes are declared, their
	// resolved values are joined in order with ":" to form the key value part.
	KeyAttrs() []string
	// RawJSON returns the raw JSON payload represented by this document value.
	//
	// [StorageKey] reads this payload together with [Document.KeyAttrs] to
	// resolve the final storage key for the current document instance.
	RawJSON() string
	// TTL returns the default TTL metadata used by higher-level document write
	// helpers.
	//
	// The TTL is not involved in key derivation. It only describes the default
	// expiration policy of this document type.
	TTL() time.Duration
}

type documentMeta struct {
	typeName         string
	storageNamespace string
	keyAttrs         []string
	ttl              time.Duration
}

var (
	documentRegistry     sync.Map
	documentTypeRegistry sync.Map
)

func validateDocumentNamespace(namespace string) {
	if namespace == "" {
		panic("document namespace is required")
	}
	if strings.ContainsAny(namespace, contract.StorageKeySeparator+"*?_") {
		panic("document namespace must not contain reserved characters")
	}
}

func storageNamespace[D Document](d D) string {
	if d.Mem() {
		return MemKey(d.Namespace())
	}
	return d.Namespace()
}

func documentMetaFor[D Document]() documentMeta {
	typeKey := reflect.TypeFor[D]()
	if cached, ok := documentTypeRegistry.Load(typeKey); ok {
		return cached.(documentMeta)
	}

	var d D

	meta := documentMeta{
		typeName:         typeKey.String(),
		storageNamespace: storageNamespace(d),
		keyAttrs:         append([]string(nil), d.KeyAttrs()...),
		ttl:              d.TTL(),
	}

	validateDocumentNamespace(d.Namespace())
	for _, path := range meta.keyAttrs {
		if path == "" {
			panic("document key attr path is empty")
		}
	}

	cached, _ := documentTypeRegistry.LoadOrStore(typeKey, meta)
	return cached.(documentMeta)
}

func requireRegistered[D Document]() documentMeta {
	meta := documentMetaFor[D]()

	existing, loaded := documentRegistry.LoadOrStore(meta.storageNamespace, meta)
	if !loaded {
		return meta
	}

	registered := existing.(documentMeta)
	if registered.storageNamespace == meta.storageNamespace &&
		slices.Equal(registered.keyAttrs, meta.keyAttrs) &&
		registered.ttl == meta.ttl {
		return meta
	}

	panic(fmt.Sprintf(
		"document namespace %q already registered by %s, incompatible with %s",
		meta.storageNamespace,
		registered.typeName,
		meta.typeName,
	))
}

// StorageKey resolves one full storage key directly from the document.
//
// Each path declared by [Document.KeyAttrs] must exist in [Document.RawJSON].
// When multiple attributes are declared, their resolved values are joined with
// ":" to form the key value part.
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

		parts = append(parts, result.String())
	}

	return StorageKeyValue[D](strings.Join(parts, contract.StorageKeySeparator)), nil
}

// StorageKeyValue joins a resolved key value with the document namespace
// derived from the document metadata type.
func StorageKeyValue[D Document](v string) string {
	meta := requireRegistered[D]()
	if v == "" {
		return meta.storageNamespace
	}
	return meta.storageNamespace + contract.StorageKeySeparator + v
}

// MemKey returns a key routed to the memory-only storage layer.
//
// The returned key uses the reserved "_m_" prefix. If key already uses that
// prefix, it is returned unchanged.
func MemKey(key string) string {
	if strings.HasPrefix(key, contract.MemKeyPrefix) {
		return key
	}
	return contract.MemKeyPrefix + key
}

type Index struct {
	name       string
	keyPattern string
	path       string
}

func (d Index) Name() string {
	return d.name
}

func (d Index) KeyPattern() string {
	return d.keyPattern
}

func (d Index) Path() string {
	return d.path
}

// IdxFullName returns the fully-qualified index name for one document type and
// logical index name.
func IdxFullName[D Document](idxName string) string {
	if idxName == "" {
		panic("index name is required")
	}
	fullName := strings.ToLower(idxName)
	meta := requireRegistered[D]()
	if meta.storageNamespace != "" {
		fullName = strings.ToLower(meta.storageNamespace) + "_" + fullName
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
	if strings.HasPrefix(keyPattern, contract.StorageKeySeparator) {
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
