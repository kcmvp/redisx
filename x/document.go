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

// Package x is the single source of truth for the runtime-facing contracts of
// redisx: typed document schemas, key derivation helpers, index declarations,
// filter combinators, and the UPDATE wire codec. It does NOT hold registry
// metadata (that lives inside the server package as private docSpec/idxSpec).
//
// Core routing rule — Doc vs KV pattern — decided purely by the shape of arg1
// at the RESP command layer (never by content inspection, never by registry
// lookups):
//
//  1. If arg1 does NOT contain ':' → Doc Pattern. arg1 is treated as a
//     logical document namespace and MUST have been registered via
//     server.Start(schemas…) or the REGSCH admin command. A bare no-colon
//     key that is not a registered namespace is rejected outright; the
//     system never falls back to plain-KV mode for it.
//  2. If arg1 contains ':' → KV Pattern. arg1 is treated as the literal
//     full storage key and is passed straight through to the storage layer
//     with no JSON parsing. KV Pattern enforces one additional strict
//     gate: the full key MUST contain a ':' i.e. every KV key must be
//     namespaced (user rule: "key always has a namespace"). A bare
//     no-colon KV key such as "mykey" is rejected with "ERR missing
//     namespace".
//
// All naming decisions (storage ns shape, ns:pk separator, pk join char,
// internal meta-key prefixes) live inside internal/naming and are exposed
// through naming-package getter functions ONLY — never via exported
// constants and never via raw string concatenation from callers.

// Schema declares the metadata contract for one JSON document type.
// Implement it on the zero-value of your ~string alias type (e.g.
// UserDoc("")); the receiver value is never read — it exists purely for
// interface dispatch.
//
// Schema is intentionally free of type parameters so it can be stored in
// slices, passed variadic to server.Start, etc. For a typed version that
// includes RawJSON() (needed to build storage keys from a populated
// payload), see [Document].
//
// Example — disk-backed user document (cold data, survives restart):
//
//	type UserDoc string
//	func (UserDoc) Namespace() string  { return "user" }
//	func (UserDoc) Mem() bool          { return false }                 // lives on disk layer
//	func (UserDoc) KeyAttrs() []string { return []string{"tenant","id"} } // composite pk
//	func (UserDoc) TTL() time.Duration { return 24 * time.Hour }         // default TTL used by auto-TTL
//
// Example — in-memory session document (hot data, dropped on restart):
//
//	type SessionDoc string
//	func (SessionDoc) Namespace() string  { return "session" }
//	func (SessionDoc) Mem() bool          { return true }                  // lives on mem layer (prefix "_m_:")
//	func (SessionDoc) KeyAttrs() []string { return []string{"sid"} }       // single-segment pk
//	func (SessionDoc) TTL() time.Duration { return 30 * time.Minute }
//
// Usage at boot:
//
//	server.Start(cfg, UserDoc(""), SessionDoc(""))
type Schema interface {
	// Namespace returns the canonical logical document namespace, e.g.
	// "user". It is combined with Mem() to produce the storage namespace
	// ("user" for disk, "_m_:user" for mem).
	//
	// Constraints: lowercase letter followed by 0-62 lowercase letters or
	// digits; ':' '*' '?' '-' '_' are all reserved and forbidden here.
	Namespace() string
	// Mem reports whether documents of this type live in the memory-only
	// layer. When true: storageNs = "_m_:<namespace>", data keys and
	// _doc_/_idx_ metadata all live on the mem store, and everything is
	// discarded on process restart. When false: everything stays on the
	// disk store and is durable across restarts.
	Mem() bool
	// KeyAttrs returns the ordered list of JSON paths whose values are
	// joined with the canonical pk-join character ('_') to form the key
	// suffix. Format + concrete example:
	//   KeyAttrs = ["tenant","id"]
	//     → suffix format:        "{tenant}_{id}"
	//                            for {"tenant":"acme","id":"7"} → suffix value "acme_7"
	//     → full key format:      "{storageNs}:{tenant}_{id}"
	//                            for storageNs="user"            → full key value "user:acme_7"
	//
	// A pk value that itself contains ':' after joining is rejected by
	// Strict Gate (validatePKSuffixNoColon) so callers never need to
	// escape ':' inside attribute values.
	KeyAttrs() []string
	// TTL returns the default document TTL. It is applied automatically by
	// SetWithTtl / SetNXWithTtl whenever the caller passes an explicit
	// TTL of 0. Pass 0 to mean "no default TTL".
	TTL() time.Duration
}

// Document is the fully-typed variant of [Schema]: it adds ~string (so
// loaded JSON payloads can be cast back to the alias) and RawJSON() (so
// [StorageKey] can derive full keys from a populated instance).
//
// Any string-alias that implements Document automatically satisfies
// Schema; there is no extra work beyond the five methods shown in the
// Schema examples.
type Document interface {
	~string
	Schema
	// RawJSON returns the raw JSON payload carried by this document
	// instance. For a zero-value alias (e.g. UserDoc("")) it returns "".
	RawJSON() string
}

// Index describes one declared JSON BTree index. Instances are normally
// produced by [Idx] or [RawIndex] and then passed to
// server.Start / DB.CreateIndex at boot; the Admin REGIDX command does
// not use this struct — it writes idxSpec metadata directly via
// applyIndexSpec.
//
// An Index is always bound to one storage layer (disk XOR mem) via the
// owner document's Mem() flag; cross-layer index searches are rejected
// at the Strict Gate.
type Index struct {
	name       string
	keyPattern string
	paths      []string
}

// Name returns the fully-qualified runtime index name, i.e.
// "<storageNs>:<logical>" (e.g. "user:age" or "_m_:session:last").
func (d Index) Name() string { return d.name }

// KeyPattern returns the storage-key glob pattern the index is scoped to
// (e.g. "user:*" or "_m_:session:*").
func (d Index) KeyPattern() string { return d.keyPattern }

// Paths returns the normalized JSON paths used by the BTree indexer, with
// '.' already replaced by '_' per BuntDB's IndexJSON convention. For
// single-field indexes the returned slice has one element; for composite
// (multi-field) indexes it has >=2 elements in ORDER-BY priority.
func (d Index) Paths() []string { return d.paths }

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
func RawIndex(name string, keyPattern string, jsonPaths ...string) Index {
	if name == "" {
		panic("RawIndex: name is required")
	}
	if keyPattern == "" {
		panic("RawIndex: keyPattern is required")
	}
	if len(jsonPaths) == 0 {
		panic("RawIndex: at least one jsonPath is required")
	}
	paths := make([]string, 0, len(jsonPaths))
	for _, p := range jsonPaths {
		if p == "" {
			panic("RawIndex: jsonPaths entry must not be empty")
		}
		paths = append(paths, strings.ReplaceAll(p, ".", "_"))
	}
	return Index{
		name:       strings.ToLower(name),
		keyPattern: keyPattern,
		paths:      paths,
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
//	namespace:idxname
//
// where namespace comes from D, idxName is lowercased, and the final result is
// entirely lowercase.
//
// For composite (multi-field) indexes, pass multiple jsonPaths in ORDER-BY
// priority; buntdb.IndexJSON natively consumes the variadic path list to
// build a compound BTree key.
func Idx[D Document](idxName string, keyPattern string, jsonPaths ...string) Index {
	if keyPattern == "" {
		panic("index key pattern is required")
	}
	if strings.HasPrefix(keyPattern, ":") {
		panic("index key pattern must not start with separator")
	}
	if len(jsonPaths) == 0 {
		panic("at least one index json path is required")
	}
	paths := make([]string, 0, len(jsonPaths))
	for _, p := range jsonPaths {
		if p == "" {
			panic("index jsonPaths entry must not be empty")
		}
		paths = append(paths, strings.ReplaceAll(p, ".", "_"))
	}

	return Index{
		name:       IdxFullName[D](idxName),
		keyPattern: StorageNsKeyPattern[D](keyPattern),
		paths:      paths,
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
	colonPrefix := fullNamespace + naming.StorageKeySeparator()
	underscorePrefix := fullNamespace + naming.KeyAttrsJoin()
	if strings.HasPrefix(lowerName, colonPrefix) || strings.HasPrefix(lowerName, underscorePrefix) {
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

// ============================================================
// RESP Wire Cheatsheet — two concrete document types
// ============================================================
//
// Reference types used below (identical to the Schema examples above):
//
//   - UserDoc:    Namespace="user",    Mem=false, KeyAttrs=["tenant","id"], TTL=24h
//                 → storageNs:                  "user"                      (disk layer, durable across restart)
//                 → full key format:            "user:{tenant}_{id}"           (multi-segment pk JOINED by '_' only WITHIN the pk suffix)
//                                                 for {"tenant":"acme","id":"7"}  → value "user:acme_7"
//                 → doc meta key format:        "_doc_:{storageNs}"
//                                                 for storageNs="user"          → value "_doc_:user"
//                 → index "age" full name:      "{storageNs}:age"              (ALL hierarchical levels JOINED by ':')
//                                                 for storageNs="user"          → value "user:age"
//                 → composite index "tenantage": "{storageNs}:tenantage"
//                   (REGIDX user tenantage tenant,age)
//                                                 for storageNs="user"          → value "user:tenantage"
//                                                 ordered by (tenant ASC, age ASC) via buntdb.IndexJSON("tenant","age")
//                 → index meta key format:      "_idx_:{indexFullName}"
//                                                 for indexFullName="user:age"  → value "_idx_:user:age"
//
//   - SessionDoc: Namespace="session", Mem=true,  KeyAttrs=["sid"],          TTL=30m
//                 → storageNs:                  "_m_:session"               (mem layer, dropped on restart)
//                 → full key format:            "_m_:session:{sid}"
//                                                 for {"sid":"abc"}            → value "_m_:session:abc"
//                 → doc meta key format:        "_doc_:{storageNs}"
//                                                 for storageNs="_m_:session"  → value "_doc_:_m_:session"
//                 → index "last" full name:     "{storageNs}:last"           (consistent colon-based hierarchy)
//                                                 for storageNs="_m_:session"  → value "_m_:session:last"
//                 → composite index "uidlast":  "{storageNs}:uidlast"
//                   (REGIDX _m_:session uidlast user_id,last_ts)
//                                                 for storageNs="_m_:session"  → value "_m_:session:uidlast"
//                                                 ordered by (user_id ASC, last_ts ASC)
//                 → index meta key format:      "_idx_:{indexFullName}"
//                                                 for indexFullName="_m_:session:last"
//                                                                               → value "_idx_:_m_:session:last"
//
// Single mental model, no character mixing ever:
//   ┌─ ':'  = hierarchical boundary (layer / namespace / index-logical / pk prefix)
//   └─ '_'  = flat concatenation only:
//               (a) inside the pk suffix when KeyAttrs has >=2 fields (e.g. "acme_7")
//               (b) the internal '_m_' / '_doc_' / '_idx_' sentinel tokens
//                   which are treated as opaque identifiers, never split on '_'
//
// ❌ Why the namespace "product_bundle" is rejected by ValidateDocLogicalNamespace
//    ──────────────────────────────────────────────────────────────────────────
//    If doc logical namespaces were allowed to contain '_', the reader would
//    constantly have to disambiguate:
//      "product_bundle:acme_7"
//        ├─ is '_' a LAYER separator? (→ product/bundle/acme_7 ?)
//        └─ or is '_' the pk-JOIN marker inside the suffix? (→ product_bundle/acme_7)
//    To keep the single-mental-model promise ('_' = flat, ':' = layer), '_'
//    is RESERVED EXCLUSIVELY for the two flat-concatenation cases above.
//    Namespace "product_bundle" therefore violates the rule. Use "productbundle"
//    (flat) or split into layers explicitly and route via SET/KV pattern.
//
// ------------------------------------------------------------
// 1. Doc Pattern (arg1 has NO ':' → treated as logical namespace)
// ------------------------------------------------------------
// Rule: arg1 is a namespace name. It MUST already be registered; otherwise
// the command is rejected outright (no KV-pattern fallback).
//
//  (a) SET — single or batched JSON object / array-of-objects.
//      For each object pk attrs are resolved to derive the full storage
//      key, then setBatchAtomic writes them all-or-nothing on the layer
//      dictated by spec.Mem.
//
//      SET user {"tenant":"acme","id":"7","age":30,"status":"cold"}
//        → writes "user:acme_7" on disk layer, TTL=24h (autoTTL from DocSpec)
//        → returns "+OK"
//
//      SET session [{"sid":"abc","user_id":1,"last_ts":1000},{"sid":"xyz","user_id":2,"last_ts":999}]
//        → writes "_m_:session:abc" + "_m_:session:xyz" on mem layer, TTL=30m
//        → returns "+OK"
//        → all-or-nothing atomic; if any key derivation fails NONE is written
//
//      SETNX user [{"tenant":"acme","id":"7"}, {"tenant":"acme","id":"8"}]
//        → NX semantics: if EITHER "user:acme_7" OR "user:acme_8" already
//          exists, NEITHER row is written and the command returns Null.
//
//  (b) GET — requires exactly "<namespace> <pk-suffix>" (two args after the
//      command). A lone namespace argument is rejected — use SEARCHKEY or
//      the Admin-only KEYS command to enumerate.
//
//      GET user acme_7            → bulk {"tenant":"acme","id":"7",...}
//      GET session abc           → bulk {"sid":"abc",...}   (reads mem layer)
//      GET user                  → ERR "ns alone is not a query; use SEARCHKEY"
//
//  (c) DEL — zero or more pk-suffix args. No args + namespace alone is
//      rejected (same reasoning as GET). Multiple suffixes are deleted
//      atomically on the storage layer dictated by spec.Mem.
//
//      DEL user acme_7 acme_8    → deletes "user:acme_7" + "user:acme_8" (atomic)
//      DEL session abc xyz       → deletes both mem sessions (atomic)
//      DEL user                  → ERR "DEL ns alone is not supported; use SEARCHKEY"
//
//  (d) UPDATE — anchor = "<storageNs>:*" or "<storageNs>:<suffix>".
//      The anchor is resolved back to a storageNs and thence to the
//      registered docSpec. If any $set mutation touches a pk attr and
//      the re-derived full storage key changes, the entire UPDATE is
//      rolled back and returns "ERR pk mutations are not allowed".
//
//      UPDATE user:* "tenant=acme" '{"$set":{"status":"banned"}}'
//        → OK, all matching user docs get status overwritten, pk attrs untouched
//
//      UPDATE user:* "" '{"$set":{"id":"9"}}'
//        → ERR "pk mutations are not allowed" (id ∈ KeyAttrs, full key would change)
//
//  (e) SEARCHKEY / SEARCHINDEX — anchor must NOT start with wildcard and
//      must resolve to a registered storageNs. A bare "*" pivot is
//      rejected to force clients to anchor to a namespace (use the
//      Admin-only KEYS command for a global scan).
//
//      SEARCHKEY user:* ""          → matches "user:*" on disk layer
//      SEARCHKEY * ""               → ERR "SEARCHKEY must be anchored to a namespace; use SEARCHINDEX for cross-ns scans"
//      SEARCHINDEX user_age user:* "" | filter | desc  → runs on user_age index (disk)
//      SEARCHINDEX unknown_idx user:* ""  → ERR "index not registered: unknown_idx"
//
// ------------------------------------------------------------
// 2. KV Pattern (arg1 HAS ':' → literal full storage key)
// ------------------------------------------------------------
// Rule: arg1 is the raw full storage key. No JSON parsing, no registry
// lookup, zero introspection. The key is routed to mem or disk purely
// by the "_m_:" prefix (via layerForKey).
//
// ADDITIONAL USER RULE: Every KV full key MUST contain ':' somewhere
// (enforced by validateKVFullKey at the command gate). A bare no-colon
// key is NOT KV-Pattern (it would be Doc-Pattern instead, and fail at
// registry lookup unless that no-colon name is a registered namespace).
//
//  (a) SET / SETEX / SETNX — straight through, autoTTL only applies if
//      the caller passes explicit TTL==0 AND the full key matches a
//      registered docSpec storageNs prefix; otherwise TTL is as given.
//
//      SET app:config:theme "dark"           → writes "app:config:theme" on disk layer  (KV, no JSON parsed)
//      SET _m_:cache:hot:123 "blurb" EX 60   → writes mem layer, TTL=60s
//      SET mykey "val"                       → ERR KV "missing namespace (required ':')" — mykey has no colon
//      SET _doc_:user "payload"              → ERR internal storage ns reserved
//
//  (b) GET / DEL — same routing + same "full key must contain ':'" gate.
//
//      GET app:config:theme           → bulk "dark"
//      GET _m_:cache:hot:123          → bulk "blurb" (mem layer)
//      GET mykey                      → ERR KV "missing namespace"
//      DEL app:config:theme           → OK, deleted
//
// ------------------------------------------------------------
// 3. Symmetric persistence (Doc/Index meta follow their storage layer)
// ------------------------------------------------------------
//   - Restart behavior for UserDoc (disk):
//       data "user:acme_7"         → still on disk, present
//       _doc_:user meta           → still on disk → docSpec re-registered automatically
//       _idx_:user_age meta       → still on disk → BTree rebuilt, backfill works
//
//   - Restart behavior for SessionDoc (mem):
//       data "_m_:session:abc"    → mem store is new buntdb.Open(":memory:") → GONE
//       _doc_:_m_:session meta    → mem store gone → docSpec NOT auto-registered
//                                      caller MUST re-declare at Start(..., SessionDoc(""))
//       _idx_:_m_:session_last  → mem store gone → idxSpec NOT auto-registered
//                                      caller MUST re-declare at Start or re-run REGIDX
//
//   This symmetry is intentional: "if a doc lives on the mem layer and
//   its data is gone on restart, then its registry metadata is gone
//   too" so the system never ends up with a dangling empty registry
//   pointing at an empty mem-layer namespace.
