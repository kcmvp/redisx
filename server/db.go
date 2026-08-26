package server

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/match"
	"github.com/tidwall/sjson"

	"github.com/kcmvp/redisx/x"
)

// ——— Type / Struct definitions ———

type storageLayer uint8

const (
	storageDisk storageLayer = iota
	storageMem
)

// docSpec stores a single registered typed-document schema (the server-
// internal enriched version of x.Schema). It always contains exactly one live
// copy per storageNs (the "no multi-version" invariant). The three internal
// fields Version/CreatedAt/UpdatedAt are excluded from the canonical MD5
// fingerprint, so metadata-only drifts do not invalidate version identity.
type docSpec struct {
	Namespace string        `json:"namespace"`
	Mem       bool          `json:"mem"`
	KeyAttrs  []string      `json:"key_attrs"`
	TTL       time.Duration `json:"ttl_ns"`

	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// idxSpec stores a single registered secondary index (server-internal version
// of x.Index). Same "exactly one live copy per fullName" invariant as docSpec.
// Composite indexes are buntdb-native JSON-path comparators (see
// indexJSONComposite) that are re-created whenever the canonical fingerprint
// changes.
//
// UnmarshalJSON accepts both "path" (single) and "paths" (array) keys for
// backwards-compatibility with legacy JSON payloads; "path" is rewritten into
// a single-element "paths" array.
type idxSpec struct {
	OwnerNs    string   `json:"owner_ns"`
	Logical    string   `json:"logical"`
	KeyPattern string   `json:"key_pattern"`
	Paths      []string `json:"paths"`

	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// DB is a lightweight two-layer BuntDB wrapper (disk + memory). Memory layer
// keys are routed via the "_m_:" storageNs prefix. All typed-doc registry
// logic (docRegSpec / idxRegSpec maps) live in-process alongside storage so
// registry lookups are zero I/O.
type DB struct {
	disk *buntdb.DB
	mem  *buntdb.DB

	docRegMu   sync.RWMutex
	docRegSpec map[string]docSpec

	idxRegMu   sync.RWMutex
	idxRegSpec map[string]idxSpec
}

// batchedWrite is the per-item tuple used by setBatchAtomic (the underlying
// implementation for SET/SETEX/SETNX when multiple JSON payloads are sent via
// doc-path colon-routing form). TTL=0 means persist forever.
type batchedWrite struct {
	Key   string
	Value string
	TTL   time.Duration
}

var errNxPreconditionFailed = errors.New("setBatchAtomic: nx precondition failed — one or more keys already exist")

// ——— Public (exported) struct methods ———

// StorageNs returns the actual storage-layer namespace used for meta + data
// keys: it is either "<ns>" (disk) or "_m_:<ns>" (memory). See Rule1 in
// checker.go / naming package for the full Spec-vs-MetaKey-vs-x.Schema map.
func (p docSpec) StorageNs() string {
	return naming.BuildStorageNs(p.Namespace, p.Mem)
}

// FullName returns the canonical composite key used for idx meta-keys and the
// buntdb native index handle: "<ownerNs>!_!<logical>".
func (i idxSpec) FullName() string { return naming.BuildIdxFullName(i.OwnerNs, i.Logical) }

func (i *idxSpec) UnmarshalJSON(b []byte) error {
	type rawIdx idxSpec
	aux := &struct {
		Path string `json:"path"`
		*rawIdx
	}{
		rawIdx: (*rawIdx)(i),
	}
	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}
	if len(aux.Paths) == 0 && aux.Path != "" {
		aux.Paths = []string{aux.Path}
	}
	return nil
}

// ——— Public (exported) DB methods ———

// ——— DB raw access + lifecycle ———

// Raw exposes the primary (disk) BuntDB instance. For "_m_:" keys use RawMem.
func (db *DB) Raw() *buntdb.DB {
	return db.disk
}

// RawMem exposes the in-memory-only storage layer. Keys here are NOT persisted
// across server restarts.
func (db *DB) RawMem() *buntdb.DB {
	return db.mem
}

// Close closes both storage layers and returns the first error, if any.
func (db *DB) Close() error {
	var firstErr error
	if db.mem != nil {
		closeErr := db.mem.Close()
		if closeErr != nil {
			firstErr = closeErr
		}
	}
	if db.disk != nil {
		closeErr := db.disk.Close()
		if closeErr != nil && firstErr == nil {
			firstErr = closeErr
		}
	}
	return firstErr
}

// CreateIndex is a small alias over registerIndexes; see registerIndexes for
// the actual x.Index → idxSpec conversion + writeIndexSpec persistence flow.
func (db *DB) CreateIndex(idx x.Index) error {
	return db.registerIndexes(idx)
}

// ——— Typed-doc query & update API ———

// Update is TYPED-DOC ONLY (there is no KV-style UPDATE bypass per the colon-
// routing SSOT; the cmd.go UPDATE handler already resolved the keyrange anchor
// to a single storageNs, verified it is registered (non-internal) and rejected
// KV-shaped bare patterns). Caller provides the key-range + filter that pins
// to exactly one storage layer; mutations are sjson path writes executed
// inside a single BuntDB Update tx so either all matched rows change or none
// do. Primary keys derived from KeyAttrs are re-derived post-mutation; if any
// pk would drift the whole tx is aborted with zero writes.
func (db *DB) Update(kr x.KeyRange, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	layerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		l, ok, lerr := resolveLayer(k)
		if lerr != nil || !ok {
			return fmt.Errorf("Update: key %q is not a concrete key (err=%v constrained=%v)", k, lerr, ok)
		}
		return l
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	layer, ok := layerObj.(storageLayer)
	if !ok {
		if lerr, isErr := layerObj.(error); isErr {
			return mo.Err[[]string](lerr)
		}
		return mo.Err[[]string](fmt.Errorf("resolveLayer returned non-storageLayer type %T for key range routing", layerObj))
	}
	var updatedKeys []string
	var err error
	err = db.store(layer).Update(func(tx *buntdb.Tx) error {
		var matchedKeys []string
		scanErr := applyKeyRange(tx, kr, x.RangeAsc, func(key, value string) bool {
			if filter == nil || filter.Eval(value) {
				matchedKeys = append(matchedKeys, key)
			}
			return true
		})
		if scanErr != nil {
			return scanErr
		}

		for _, key := range matchedKeys {
			val, getErr := tx.Get(key)
			if getErr != nil {
				return getErr
			}
			ttl, ttlErr := tx.TTL(key)
			if ttlErr != nil && !errors.Is(ttlErr, buntdb.ErrNotFound) {
				return ttlErr
			}

			newVal := val
			for _, vp := range values {
				newVal, err = sjson.Set(newVal, vp.Path(), vp.Value())
				if err != nil {
					err = fmt.Errorf("failed to set %s: %w", vp.Path(), err)
					slog.Error("failed to update document", "key", key, "error", err)
					return err
				}
			}

			storageNs, _, splitErr := naming.SplitStorageKey(key)
			if splitErr == nil && !naming.IsInternalStorageNs(storageNs) {
				doc, regOK := db.docRegSpec[storageNs]
				if regOK {
					oldDK, dkErr := deriveDocKey(doc, storageNs, val)
					if dkErr == nil {
						newDK, dkErr2 := deriveDocKey(doc, storageNs, newVal)
						if dkErr2 == nil && oldDK.FullStorageKey != newDK.FullStorageKey {
							return fmt.Errorf("UPDATE would change primary key of %q from %q to %q — pk mutations are not allowed", key, oldDK.FullStorageKey, newDK.FullStorageKey)
						}
					}
				}
			}

			if newVal != val {
				_, _, err = tx.Set(key, newVal, setOptionsForTTL(ttl))
				if err != nil {
					return err
				}
			}
			updatedKeys = append(updatedKeys, key)
		}

		return nil
	})
	if err != nil {
		return mo.Err[[]string](err)
	}

	sort.Strings(updatedKeys)
	return mo.Ok(updatedKeys)
}

// SearchIndex scans via a buntdb-native secondary index handle. The index
// name must already be registered (REGIDX / boot registerSchemas seed or
// runtime REGIDX); an unregistered name returns Err "index not found". Key-
// range layer and index ownerNs layer must match (cross-layer searches are
// parameter-ERR). Matched rows are optionally narrowed by filter.Eval; the
// SEARCHINDEX command also respects colon-routing (cmd.go searchIndexCommand
// — index name is always logical, KV-passthrough route is SEARCHKEY only),
// per the typed-doc-SSOT for registry-bound queries.
func (db *DB) SearchIndex(indexName string, kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if indexName == "" {
		return mo.Err[[]string](errors.New("index name is required"))
	}
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	idxSpec, ok := db.idxRegSpec[indexName]
	if !ok {
		return mo.Err[[]string](fmt.Errorf("index not found: %s", indexName))
	}
	layer, constrained, lerr := resolveLayer(idxSpec.OwnerNs)
	if lerr != nil || !constrained {
		return mo.Err[[]string](fmt.Errorf("SearchIndex: owner_ns %q is not a concrete ns (err=%v constrained=%v)", idxSpec.OwnerNs, lerr, constrained))
	}
	krLayerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		kl, kok, klerr := resolveLayer(k)
		if klerr != nil || !kok {
			return fmt.Errorf("SearchIndex: key %q is not a concrete key (err=%v constrained=%v)", k, klerr, kok)
		}
		return kl
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	krLayer, ok := krLayerObj.(storageLayer)
	if !ok {
		if klerr, isErr := krLayerObj.(error); isErr {
			return mo.Err[[]string](klerr)
		}
		return mo.Err[[]string](fmt.Errorf("resolveLayer returned non-storageLayer type %T for key range routing", krLayerObj))
	}
	if krLayer != layer {
		return mo.Err[[]string](fmt.Errorf("key range targets a different storage layer than index %s", indexName))
	}

	idxKeySep := "\x00"
	limit := kr.GetLimit()

	var results []string
	err := db.store(layer).View(func(tx *buntdb.Tx) error {
		iter := tx.Ascend
		if desc {
			iter = tx.Descend
		}
		return iter(indexName, func(ik, value string) bool {
			sepIdx := strings.Index(ik, idxKeySep)
			var storageKey string
			if sepIdx >= 0 {
				storageKey = ik[sepIdx+1:]
			} else {
				storageKey = ik
			}
			if !x.MatchesStorageKey(kr, storageKey) {
				return true
			}
			if !gjson.Valid(value) {
				return true
			}
			if filter == nil || filter.Eval(value) {
				results = append(results, value)
				if limit > 0 && len(results) == limit {
					return false
				}
			}
			return true
		})
	})

	if err != nil {
		if errors.Is(err, buntdb.ErrNotFound) {
			return mo.Err[[]string](fmt.Errorf("index not found: %s", indexName))
		}
		return mo.Err[[]string](err)
	}
	return mo.Ok(results)
}

// SearchKey walks the raw storage-key order (no secondary index) for a key-
// range; used both for typed-doc queries (colon-routing doc-path, KeyRange
// anchor resolves to storageNs) AND for generic KV-pattern passthrough (colon-
// routing KV-path, caller supplies a wildcard pattern on keys that contain
// ':'). The cmd.go searchKeyCommand already handled the 4-branch KEYS-style
// colon-routing before this fn is invoked; this method does NOT consult the
// typed-doc registry for KV-pattern forms.
func (db *DB) SearchKey(kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	layerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		l, ok, lerr := resolveLayer(k)
		if lerr != nil || !ok {
			return fmt.Errorf("SearchKey: key %q is not a concrete key (err=%v constrained=%v)", k, lerr, ok)
		}
		return l
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	layer, ok := layerObj.(storageLayer)
	if !ok {
		if lerr, isErr := layerObj.(error); isErr {
			return mo.Err[[]string](lerr)
		}
		return mo.Err[[]string](fmt.Errorf("resolveLayer returned non-storageLayer type %T for key range routing", layerObj))
	}

	dir := x.RangeAsc
	if desc {
		dir = x.RangeDesc
	}
	var results []string
	err := db.store(layer).View(func(tx *buntdb.Tx) error {
		return applyKeyRange(tx, kr, dir, func(_, value string) bool {
			if !gjson.Valid(value) {
				return true
			}
			if filter == nil || filter.Eval(value) {
				results = append(results, value)
			}
			return true
		})
	})
	if err != nil {
		return mo.Err[[]string](err)
	}
	return mo.Ok(results)
}

// ——— Raw KV passthrough (Set/SetNX/Get/Delete/Keys) ———
//
// These methods are the thin storage-layer counterparts to the RESP KV
// commands. They implement 0 registry gates: if the caller already routed a
// key with ':' (colon-routing KV-path) it reaches here as a full storage
// key; the typed-doc registry / KeyAttrs / JSON schema checks and the
// internal-namespace write-guard (_doc_/_idx_/_auth_ via validateKVMutationKey)
// are all performed ABOVE this layer (cmd.go handlers + checker.go).

// Set stores value under key (unconditional overwrite). "_m_:" prefix routes
// to the in-memory layer. RESP equivalent: SET.
func (db *DB) Set(key string, value string) error {
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return err
	}
	if !constrained {
		return fmt.Errorf("Set: key %q is a pattern, not a concrete key", key)
	}
	return db.store(layer).Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

// SetWithTtl sets key→value with optional positive TTL (zero/negative = no
// TTL, same as Set). autoTTLFromKey injects the doc TTL when the caller
// comes from a typed-doc SET/SETEX/SETNX handler. RESP: SET EX / SETEX.
func (db *DB) SetWithTtl(key string, value string, ttl time.Duration) error {
	ttl = db.autoTTLFromKey(key, ttl)
	opt := setOptionsForTTL(ttl)
	if opt == nil {
		return db.Set(key, value)
	}
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return err
	}
	if !constrained {
		return fmt.Errorf("SetWithTtl: key %q is a pattern, not a concrete key", key)
	}
	return db.store(layer).Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, opt)
		return err
	})
}

// SetNX sets key→value only when key does NOT already exist (Redis SETNX).
// Returns true when the write actually happened.
func (db *DB) SetNX(key string, value string) (bool, error) {
	return db.SetNXWithTtl(key, value, 0)
}

// SetNXWithTtl is SetNX with a positive TTL applied to newly-created keys.
func (db *DB) SetNXWithTtl(key string, value string, ttl time.Duration) (bool, error) {
	ttl = db.autoTTLFromKey(key, ttl)
	var set bool
	opt := setOptionsForTTL(ttl)
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return false, err
	}
	if !constrained {
		return false, fmt.Errorf("SetNXWithTtl: key %q is a pattern, not a concrete key", key)
	}
	err = db.store(layer).Update(func(tx *buntdb.Tx) error {
		_, err := tx.Get(key)
		if err == buntdb.ErrNotFound {
			_, _, err = tx.Set(key, value, opt)
			if err == nil {
				set = true
			}
			return err
		}
		return err
	})
	if err != nil {
		return false, err
	}
	return set, nil
}

// Get returns the value stored under key. Missing key = Err result. RESP: GET.
func (db *DB) Get(key string) mo.Result[string] {
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return mo.Err[string](err)
	}
	if !constrained {
		return mo.Err[string](fmt.Errorf("Get: key %q is a pattern, not a concrete key", key))
	}
	var val string
	err = db.store(layer).View(func(tx *buntdb.Tx) error {
		var innerErr error
		val, innerErr = tx.Get(key)
		return innerErr
	})

	if err != nil {
		return mo.Err[string](err)
	}
	return mo.Ok(val)
}

// Delete removes key from its routed layer. Returns true if the key existed.
// RESP equivalent: DEL single-key form. Multi-key DEL, doc-path DEL ns
// <pk1> [pk2…] and the "multi-KV DEL not allowed" guard are all enforced
// above this layer in cmd.go delCommand.
func (db *DB) Delete(key string) (bool, error) {
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return false, err
	}
	if !constrained {
		return false, fmt.Errorf("Delete: key %q is a pattern, not a concrete key", key)
	}
	var val string
	err = db.store(layer).Update(func(tx *buntdb.Tx) error {
		var innerErr error
		val, innerErr = tx.Delete(key)
		if innerErr == buntdb.ErrNotFound {
			return nil
		}
		return innerErr
	})
	if err != nil {
		return false, err
	}
	return len(val) > 0, nil
}

// Keys returns storage keys matching a single-layer pinned pattern. Pattern
// cannot start with wildcard (resolveLayer enforces single layer).
// The cmd.go keysCommand 4-branch colon-routing logic sits above this call:
// ':' in pattern → KV-glob straight through; no ':' + bare registered ns →
// auto-scoped `<ns>:*`; no ':' + wildcard → ERR. RESP: KEYS.
func (db *DB) Keys(keyPattern string) mo.Result[[]string] {
	layer, constrained, err := resolveLayer(keyPattern)
	if err != nil {
		return mo.Err[[]string](err)
	}
	if !constrained {
		return mo.Err[[]string](errors.New("key pattern cannot start with wildcard"))
	}
	var keys []string
	err = db.store(layer).View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(keyPattern, func(key, value string) bool {
			keys = append(keys, key)
			return true
		})
	})
	if err != nil {
		return mo.Err[[]string](err)
	}
	sort.Strings(keys)
	return mo.Ok(keys)
}

// ——— Private (unexported) helpers ———

// ——— Meta helpers: fingerprint + meta-key load/delete (no multi-version) ———

// md5VersionHex computes a 12-char truncated MD5 over canonical JSON-encoded v.
// Callers always pass an anonymous struct that intentionally excludes Version/
// CreatedAt/UpdatedAt, so fingerprints are semantic-only and stable over time.
func md5VersionHex(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("md5VersionHex: marshal: %w", err)
	}
	sum := md5.Sum(raw)
	return hex.EncodeToString(sum[:])[:12], nil
}

// canonicalDocMD5 returns the canonical MD5 for a docSpec. Only the four
// user-visible x.Schema fields are hashed (Namespace/Mem/KeyAttrs/TTL); the
// three server-internal fields (Version/CreatedAt/UpdatedAt) are deliberately
// excluded.
func canonicalDocMD5(spec docSpec) (string, error) {
	if len(spec.KeyAttrs) == 0 {
		spec.KeyAttrs = []string{}
	}
	return md5VersionHex(struct {
		Namespace string        `json:"namespace"`
		Mem       bool          `json:"mem"`
		KeyAttrs  []string      `json:"key_attrs"`
		TTL       time.Duration `json:"ttl_ns"`
	}{
		Namespace: spec.Namespace,
		Mem:       spec.Mem,
		KeyAttrs:  append([]string(nil), spec.KeyAttrs...),
		TTL:       spec.TTL,
	})
}

// canonicalIdxMD5 mirrors canonicalDocMD5 for idxSpec. Dots inside JSON paths
// are collapsed to underscores before hashing so buntdb comparator key-paths
// are always stable regardless of input spelling.
func canonicalIdxMD5(spec idxSpec) (string, error) {
	paths := lo.Map(spec.Paths, func(p string, _ int) string {
		return strings.ReplaceAll(p, ".", "_")
	})
	if len(paths) == 0 {
		paths = []string{}
	}
	return md5VersionHex(struct {
		OwnerNs    string   `json:"owner_ns"`
		Logical    string   `json:"logical"`
		KeyPattern string   `json:"key_pattern"`
		Paths      []string `json:"paths"`
	}{
		OwnerNs:    spec.OwnerNs,
		Logical:    spec.Logical,
		KeyPattern: spec.KeyPattern,
		Paths:      paths,
	})
}

// ——— Storage routing helpers ———

// hasLeadingWildcard reports a pattern is unbounded (starts with * or ?).
// Unconstrained patterns are treated as "unpinned to any single layer".
func hasLeadingWildcard(keyPattern string) bool {
	return keyPattern != "" && (keyPattern[0] == '*' || keyPattern[0] == '?')
}

// resolveLayer is the single source of truth for storage-layer routing.
// It unifies the previous layerForKey / layerForStorageNs / resolvePatternLayer
// / isMemKey helpers. Callers ALWAYS handle all three returns explicitly:
//   - s == ""                 → (disk, false, err)
//   - s starts with '*'/'?'   → (disk, false, nil)    // pattern is unconstrained
//   - otherwise ("_m_:" pref) → (mem,  true,  nil)
//   - otherwise (flat ns/key) → (disk, true,  nil)
func resolveLayer(s string) (storageLayer, bool, error) {
	if s == "" {
		return storageDisk, false, errors.New("storage key or pattern is required")
	}
	if hasLeadingWildcard(s) {
		return storageDisk, false, nil
	}
	if naming.HasMemPrefix(s) {
		return storageMem, true, nil
	}
	return storageDisk, true, nil
}

// store returns the BuntDB handle for the given layer (disk XOR mem).
func (db *DB) store(layer storageLayer) *buntdb.DB {
	if layer == storageMem {
		return db.mem
	}
	return db.disk
}

// setOptionsForTTL returns nil for TTL<=0 (persist forever) or a buntdb
// SetOptions instance that expires after the provided TTL.
func setOptionsForTTL(ttl time.Duration) *buntdb.SetOptions {
	if ttl <= 0 {
		return nil
	}
	return &buntdb.SetOptions{Expires: true, TTL: ttl}
}

// indexJSONComposite builds a buntdb-index comparator over one or more JSON
// paths. Single path delegates to buntdb.IndexJSON; multiple paths produce a
// lexicographic multi-field comparator that evaluates each gjson path in order
// and returns the first non-equal result.
func indexJSONComposite(paths ...string) func(a, b string) bool {
	if len(paths) == 0 {
		panic("indexJSONComposite: at least one path is required")
	}
	if len(paths) == 1 {
		return buntdb.IndexJSON(paths[0])
	}
	return func(a, b string) bool {
		for _, p := range paths {
			ra := gjson.Get(a, p)
			rb := gjson.Get(b, p)
			if ra.Less(rb, false) {
				return true
			}
			if rb.Less(ra, false) {
				return false
			}
		}
		return false
	}
}

// ——— Meta key read/write + registry write lifecycle ———

// loadDocSpec reads the single persisted doc spec key for a storageNs.
// exists=false means the namespace is not registered (registry fail-closed
// invariant for typed-doc paths). Boot (loadDocSpecs) and writeDocSpec are
// the only writers; this is the single read-path for the "exactly one live
// version" invariant.
func (db *DB) loadDocSpec(storageNs string) (spec docSpec, exists bool, err error) {
	if storageNs == "" {
		return spec, false, errors.New("loadDocSpec: storageNs is required")
	}
	layer, constrained, lerr := resolveLayer(storageNs)
	if lerr != nil || !constrained {
		return spec, false, fmt.Errorf("loadDocSpec: storageNs %q is not a concrete ns (err=%v constrained=%v)", storageNs, lerr, constrained)
	}
	glob := naming.DocMetaGlobFor(storageNs)
	_ = db.store(layer).View(func(tx *buntdb.Tx) error {
		_ = tx.AscendKeys(glob, func(k, raw string) bool {
			if e := json.Unmarshal([]byte(raw), &spec); e != nil {
				err = fmt.Errorf("loadDocSpec: %s unmarshal: %w", k, e)
				return false
			}
			exists = true
			return false
		})
		return nil
	})
	return spec, exists, err
}

// loadIdxSpec mirrors loadDocSpec for an index fullName.
func (db *DB) loadIdxSpec(idxFullName string) (spec idxSpec, exists bool, err error) {
	if idxFullName == "" {
		return spec, false, errors.New("loadIdxSpec: idxFullName is required")
	}
	ownerNs, _, perr := naming.ParseIdxFullName(idxFullName)
	if perr != nil {
		return spec, false, fmt.Errorf("loadIdxSpec: %w", perr)
	}
	layer, constrained, lerr := resolveLayer(ownerNs)
	if lerr != nil || !constrained {
		return spec, false, fmt.Errorf("loadIdxSpec: ownerNs %q is not a concrete ns (err=%v constrained=%v)", ownerNs, lerr, constrained)
	}
	glob := naming.IdxMetaGlobFor(idxFullName)
	_ = db.store(layer).View(func(tx *buntdb.Tx) error {
		_ = tx.AscendKeys(glob, func(k, raw string) bool {
			if e := json.Unmarshal([]byte(raw), &spec); e != nil {
				err = fmt.Errorf("loadIdxSpec: %s unmarshal: %w", k, e)
				return false
			}
			exists = true
			return false
		})
		return nil
	})
	return spec, exists, err
}

// deleteMetaKeys deletes all persisted meta keys that match a glob inside a
// running BuntDB tx. The "what" label is used for error messages (callers pass
// e.g. "deleteDocSpecMeta" / "deleteIdxSpecMeta" so the origin of a delete
// failure is clear). Missing keys (ErrNotFound) are ignored because the
// invariant of "exactly one live version" means zero matches just means
// nothing to delete.
//
// Do NOT call Write operations inside AscendKeys callbacks (BuntDB iter-then-
// write rule); doomed is collected first, then deleted in a second pass.
func deleteMetaKeys(tx *buntdb.Tx, glob string, what string) error {
	var doomed []string
	_ = tx.AscendKeys(glob, func(k, _ string) bool {
		doomed = append(doomed, k)
		return true
	})
	for _, k := range doomed {
		if _, e := tx.Delete(k); e != nil && !errors.Is(e, buntdb.ErrNotFound) {
			return fmt.Errorf("%s: %s: %w", what, k, e)
		}
	}
	return nil
}

// deleteDocSpecMeta deletes the single persisted doc meta key for storageNs
// inside a running BuntDB tx. Exactly one live key per storageNs is the
// invariant; if none exist the call is a no-op (the caller then decides
// whether this is "not registered" via a pre-probe).
func deleteDocSpecMeta(tx *buntdb.Tx, storageNs string) error {
	return deleteMetaKeys(tx, naming.DocMetaGlobFor(storageNs), "deleteDocSpecMeta")
}

// deleteIdxSpecMeta mirrors deleteDocSpecMeta for an index fullName.
func deleteIdxSpecMeta(tx *buntdb.Tx, idxFullName string) error {
	return deleteMetaKeys(tx, naming.IdxMetaGlobFor(idxFullName), "deleteIdxSpecMeta")
}

// ——— Core writers: writeDocSpec / writeIndexSpec ———

// writeDocSpec is the single entry point for doc-spec writes (REGSCH command,
// boot registerSchemas, ctrl seed fixtures). Input spec carries only the
// four user-visible x.Schema fields; Version/CreatedAt/UpdatedAt are computed
// & stamped here internally.
//
// Invariants enforced:
//   - MD5 unchanged → noop.
//   - MD5 changed → atomic BuntDB Update(): deleteDocSpecMeta then Set the
//     new v_<md5> meta key (so the "exactly one live version" invariant is
//     kept within a single tx).
//   - Only after the Update succeeds do we overwrite docRegSpec memory map
//     under docRegMu (no partial state on disk-write failure).
//   - CreatedAt preserved across upgrades; UpdatedAt bumped only on change.
func (db *DB) writeDocSpec(spec docSpec) error {
	if spec.Namespace == "" {
		return errors.New("writeDocSpec: namespace is required")
	}
	if err := naming.ValidateDocLogicalNamespace(spec.Namespace); err != nil {
		return fmt.Errorf("writeDocSpec: %w", err)
	}
	for i, p := range spec.KeyAttrs {
		if p == "" {
			return fmt.Errorf("writeDocSpec: key_attrs[%d] is empty", i)
		}
	}
	storageNs := spec.StorageNs()
	versionHex, err := canonicalDocMD5(spec)
	if err != nil {
		return fmt.Errorf("writeDocSpec %q: md5: %w", storageNs, err)
	}
	oldSpec, hadOld, err := db.loadDocSpec(storageNs)
	if err != nil {
		return err
	}
	if hadOld && oldSpec.Version == versionHex {
		slog.Debug("writeDocSpec: unchanged fingerprint, skipping",
			"ns", spec.Namespace, "storage_ns", storageNs, "version", versionHex)
		return nil
	}

	now := time.Now().UTC()
	if hadOld {
		spec.CreatedAt = oldSpec.CreatedAt
		spec.UpdatedAt = now
	} else {
		spec.CreatedAt = now
		spec.UpdatedAt = time.Time{}
	}
	spec.Version = versionHex

	raw, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("writeDocSpec %q: marshal: %w", storageNs, err)
	}
	layer, constrained, lerr := resolveLayer(storageNs)
	if lerr != nil || !constrained {
		return fmt.Errorf("writeDocSpec %q: storage_ns is not a concrete ns (err=%v constrained=%v)", storageNs, lerr, constrained)
	}
	targetKey := naming.DocMetaKey(storageNs, versionHex)

	if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		if err := deleteDocSpecMeta(tx, storageNs); err != nil {
			return err
		}
		_, _, setErr := tx.Set(targetKey, string(raw), nil)
		return setErr
	}); err != nil {
		return fmt.Errorf("writeDocSpec %q: persist %s: %w", storageNs, targetKey, err)
	}

	db.docRegMu.Lock()
	db.docRegSpec[storageNs] = spec
	db.docRegMu.Unlock()

	if hadOld {
		slog.Info("writeDocSpec: spec upgraded (new fingerprint)",
			"ns", spec.Namespace, "storage_ns", storageNs,
			"old_version", oldSpec.Version, "new_version", versionHex,
			"created_at", spec.CreatedAt.Format(time.RFC3339Nano),
			"updated_at", spec.UpdatedAt.Format(time.RFC3339Nano))
	} else {
		slog.Info("writeDocSpec: new spec persisted",
			"ns", spec.Namespace, "storage_ns", storageNs,
			"version", versionHex,
			"created_at", spec.CreatedAt.Format(time.RFC3339Nano))
	}
	return nil
}

// writeIndexSpec mirrors writeDocSpec for indexes. Extra behaviour vs
// writeDocSpec:
//   - On fingerprint change (paths/order/key_pattern drift) the buntdb native
//     btree index is dropped then re-created before the meta-key is written;
//     the query comparator is thus always in sync with the stored meta-key.
//   - CreateIndex happens OUTSIDE idxRegMu (btree operations are potentially
//     expensive). The lock is held only for the final idxRegSpec map write.
func (db *DB) writeIndexSpec(spec idxSpec) error {
	if spec.OwnerNs == "" {
		return errors.New("writeIndexSpec: owner_ns is required")
	}
	if spec.Logical == "" {
		return errors.New("writeIndexSpec: logical is required")
	}
	fullName := spec.FullName()
	if spec.KeyPattern == "" {
		return fmt.Errorf("writeIndexSpec %q: key_pattern is required", fullName)
	}
	if len(spec.Paths) == 0 {
		return fmt.Errorf("writeIndexSpec %q: at least one path is required", fullName)
	}
	for i, p := range spec.Paths {
		if p == "" {
			return fmt.Errorf("writeIndexSpec %q: paths[%d] is empty", fullName, i)
		}
	}
	patternLayer, constrained, err := resolveLayer(spec.KeyPattern)
	if err != nil {
		return fmt.Errorf("writeIndexSpec %q: %w", fullName, err)
	}
	if !constrained {
		return fmt.Errorf("writeIndexSpec %q: key_pattern cannot start with wildcard", fullName)
	}

	versionHex, err := canonicalIdxMD5(spec)
	if err != nil {
		return fmt.Errorf("writeIndexSpec %q: md5: %w", fullName, err)
	}
	oldSpec, hadOld, err := db.loadIdxSpec(fullName)
	if err != nil {
		return err
	}
	if hadOld && oldSpec.Version == versionHex {
		slog.Debug("writeIndexSpec: unchanged fingerprint, skipping",
			"full", fullName, "version", versionHex)
		return nil
	}

	metaLayer, metaConstrained, metaLerr := resolveLayer(spec.OwnerNs)
	if metaLerr != nil || !metaConstrained {
		return fmt.Errorf("writeIndexSpec %q: owner_ns %q is not a concrete ns (err=%v constrained=%v)", fullName, spec.OwnerNs, metaLerr, metaConstrained)
	}
	if hadOld {
		if err := db.store(metaLayer).DropIndex(fullName); err != nil && !errors.Is(err, buntdb.ErrNotFound) {
			return fmt.Errorf("writeIndexSpec %q: drop prior btree: %w", fullName, err)
		}
	}
	if err := db.store(patternLayer).CreateIndex(fullName, spec.KeyPattern, indexJSONComposite(spec.Paths...)); err != nil {
		return fmt.Errorf("writeIndexSpec %q: create btree: %w", fullName, err)
	}

	now := time.Now().UTC()
	if hadOld {
		spec.CreatedAt = oldSpec.CreatedAt
		spec.UpdatedAt = now
	} else {
		spec.CreatedAt = now
		spec.UpdatedAt = time.Time{}
	}
	spec.Version = versionHex

	raw, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("writeIndexSpec %q: marshal: %w", fullName, err)
	}
	targetKey := naming.IdxMetaKey(fullName, versionHex)

	if err := db.store(metaLayer).Update(func(tx *buntdb.Tx) error {
		if err := deleteIdxSpecMeta(tx, fullName); err != nil {
			return err
		}
		_, _, setErr := tx.Set(targetKey, string(raw), nil)
		return setErr
	}); err != nil {
		return fmt.Errorf("writeIndexSpec %q: persist %s: %w", fullName, targetKey, err)
	}

	db.idxRegMu.Lock()
	db.idxRegSpec[fullName] = spec
	db.idxRegMu.Unlock()

	if hadOld {
		slog.Info("writeIndexSpec: index rebuilt (new fingerprint)",
			"full", fullName, "owner_ns", spec.OwnerNs, "logical", spec.Logical,
			"old_version", oldSpec.Version, "new_version", versionHex,
			"paths", spec.Paths,
			"created_at", spec.CreatedAt.Format(time.RFC3339Nano),
			"updated_at", spec.UpdatedAt.Format(time.RFC3339Nano))
	} else {
		slog.Info("writeIndexSpec: new index persisted",
			"full", fullName, "owner_ns", spec.OwnerNs, "logical", spec.Logical,
			"version", versionHex, "paths", spec.Paths,
			"created_at", spec.CreatedAt.Format(time.RFC3339Nano))
	}
	return nil
}

// openDB constructs a two-layer DB instance (disk file + volatile in-memory
// layer for "_m_:" prefixed keys). Note that the "_m_:<ns>" prefix itself is
// reserved and rejected by HasUnderscorePrefix in the checker layer (above the
// storage layer) — the store simply routes; the semantic gate lives in
// checker.go / cmd.go handlers.
func openDB(path string) *DB {
	if path == "" {
		slog.Error("failed to open storage", "error", "db path is required")
		return nil
	}
	if path == ":memory:" {
		slog.Error("failed to open storage", "path", path, "error", `db path must be a file path; ":memory:" is reserved for buntdb in-memory mode`)
		return nil
	}
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		slog.Error("failed to open storage", "path", path, "error", "db path must be a file path, not a directory")
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		slog.Error("failed to create db directory", "path", path, "error", err)
		return nil
	}

	disk, err := buntdb.Open(path)
	if err != nil {
		slog.Error("failed to open buntdb", "path", path, "error", err)
		return nil
	}

	mem, err := buntdb.Open(":memory:")
	if err != nil {
		slog.Error("failed to open buntdb", "path", ":memory:", "error", err)
		_ = disk.Close()
		return nil
	}

	return &DB{
		disk:       disk,
		mem:        mem,
		docRegSpec: map[string]docSpec{},
		idxRegSpec: map[string]idxSpec{},
	}
}

// registerIndexes converts one or more x.Index values into idxSpec and
// delegates to writeIndexSpec (see writeIndexSpec for the MD5-fingerprint +
// btree lifecycle). Used by: REGIDX cmd, CreateIndex alias and boot fixtures.
func (db *DB) registerIndexes(indexes ...x.Index) error {
	for _, idx := range indexes {
		fullName := idx.Name()
		if fullName == "" {
			return fmt.Errorf("index name is required")
		}
		ownerNs, logical, err := naming.ParseIdxFullName(fullName)
		if err != nil {
			return fmt.Errorf("index %q: parse full name: %w", fullName, err)
		}
		spec := idxSpec{
			OwnerNs:    ownerNs,
			Logical:    logical,
			KeyPattern: idx.KeyPattern(),
			Paths:      idx.Paths(),
		}
		if err := db.writeIndexSpec(spec); err != nil {
			return err
		}
	}
	return nil
}

// loadIndexes scans disk+mem layers for persisted idx meta keys and mounts
// every idxSpec found. Intentionally runs BEFORE serving begins (openDB →
// StartWithConfig bootstrap). Caller holds idxRegMu for the entire pass.
//
// The "no multi-version" invariant applies here: writeIndexSpec always
// deletes the prior meta key before writing the new one, so AscendKeys only
// ever returns 0 or 1 matches per fullName; there is no version-priority
// dedup step because it cannot happen.
func (db *DB) loadIndexes() error {
	type kv struct {
		metaKey string
		raw     string
	}
	patterns := []struct {
		layer storageLayer
		glob  string
	}{
		{layer: storageDisk, glob: naming.IdxMetaGlob()},
		{layer: storageMem, glob: naming.IdxMetaGlob()},
	}
	db.idxRegMu.Lock()
	defer db.idxRegMu.Unlock()
	for _, p := range patterns {
		var collected []kv
		err := db.store(p.layer).View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(p.glob, func(metaKey, value string) bool {
				collected = append(collected, kv{metaKey: metaKey, raw: value})
				return true
			})
		})
		if err != nil {
			return err
		}
		for _, c := range collected {
			var spec idxSpec
			if err := json.Unmarshal([]byte(c.raw), &spec); err != nil {
				return fmt.Errorf("index %s: unmarshal idxSpec: %w", c.metaKey, err)
			}
			if spec.OwnerNs == "" || spec.Logical == "" {
				return fmt.Errorf("index %s: missing owner_ns/logical", c.metaKey)
			}
			fullName := spec.FullName()
			layer, constrained, err := resolveLayer(spec.KeyPattern)
			if err != nil {
				return fmt.Errorf("index %s: %w", fullName, err)
			}
			if !constrained {
				return fmt.Errorf("index %s: key_pattern cannot start with wildcard", fullName)
			}
			if len(spec.Paths) == 0 {
				return fmt.Errorf("index %s: at least one json path is required", fullName)
			}
			if err := db.store(layer).CreateIndex(fullName, spec.KeyPattern, indexJSONComposite(spec.Paths...)); err != nil {
				return fmt.Errorf("index %s: create btree: %w", fullName, err)
			}
			db.idxRegSpec[fullName] = spec
		}
	}
	slog.Debug("index registry loaded", "count", len(db.idxRegSpec))
	return nil
}

// loadDocSpecs mirrors loadIndexes for the doc-spec registry. Same "exactly-
// one-version-per-storageNs invariant applies; AscendKeys dedup step is unnecessary.
func (db *DB) loadDocSpecs() error {
	type kv struct {
		metaKey string
		raw     string
	}
	patterns := []struct {
		layer storageLayer
		glob  string
	}{
		{layer: storageDisk, glob: naming.DocMetaGlob()},
		{layer: storageMem, glob: naming.DocMetaGlob()},
	}
	db.docRegMu.Lock()
	defer db.docRegMu.Unlock()
	for _, p := range patterns {
		var collected []kv
		err := db.store(p.layer).View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(p.glob, func(metaKey, value string) bool {
				collected = append(collected, kv{metaKey: metaKey, raw: value})
				return true
			})
		})
		if err != nil {
			return err
		}
		for _, c := range collected {
			var spec docSpec
			if err := json.Unmarshal([]byte(c.raw), &spec); err != nil {
				return fmt.Errorf("doc %s: unmarshal docSpec: %w", c.metaKey, err)
			}
			if spec.Namespace == "" {
				return fmt.Errorf("doc %s: empty namespace", c.metaKey)
			}
			db.docRegSpec[spec.StorageNs()] = spec
		}
	}
	slog.Debug("doc registry loaded", "count", len(db.docRegSpec))
	return nil
}

// registerSchemas converts x.Schema values into docSpec and delegates to
// writeDocSpec (the single persistence entry point). Used by: REGSCH cmd,
// boot fixtures. x.Schema only provides the 4 user-visible fields; the 3
// internal fields (Version/CreatedAt/UpdatedAt) are stamped inside
// writeDocSpec.
func (db *DB) registerSchemas(schemas ...x.Schema) error {
	for _, sch := range schemas {
		spec := docSpec{
			Namespace: sch.Namespace(),
			Mem:       sch.Mem(),
			KeyAttrs:  append([]string(nil), sch.KeyAttrs()...),
			TTL:       sch.TTL(),
		}
		if err := db.writeDocSpec(spec); err != nil {
			return err
		}
	}
	return nil
}

// ——— Key-range iteration + apply pipeline ———

func withLimit(limit int, fn func(key, value string) bool) func(key, value string) bool {
	if limit <= 0 {
		return fn
	}
	consumed := 0
	return func(key, value string) bool {
		if !fn(key, value) {
			return false
		}
		consumed++
		return consumed < limit
	}
}

func nsGuard(pivot string, fn func(k, v string) bool) func(k, v string) bool {
	storageNs, _, err := naming.SplitStorageKey(pivot)
	if err != nil || storageNs == "" {
		return fn
	}
	scope := naming.StorageNsScope(storageNs)
	n := len(scope)
	return func(k, v string) bool {
		if len(k) <= n || k[:n] != scope {
			return true
		}
		return fn(k, v)
	}
}

func matchFilter(predicate func(key string) bool, fn func(key, value string) bool) func(key, value string) bool {
	return func(key, value string) bool {
		if !predicate(key) {
			return true
		}
		return fn(key, value)
	}
}

func upperBoundCutoff(hi string, fn func(key, value string) bool) func(key, value string) bool {
	if hi == "" {
		return fn
	}
	return func(key, value string) bool {
		if key >= hi {
			return false
		}
		return fn(key, value)
	}
}

func lowerBoundCutoff(lo string, fn func(key, value string) bool) func(key, value string) bool {
	if lo == "" {
		return fn
	}
	return func(key, value string) bool {
		if key < lo {
			return false
		}
		return fn(key, value)
	}
}

func applyBtRange(tx *buntdb.Tx, ge, lt string, limit int, dir x.RangeDirection, fn func(key, value string) bool) error {
	cb := withLimit(limit, fn)
	if x.IsLiteral(ge) && x.IsLiteral(lt) {
		cb = nsGuard(ge, cb)
		if dir == x.RangeAsc {
			return tx.AscendRange("", ge, lt, cb)
		}
		return tx.DescendLessOrEqual("", x.NextLex(lt), func(k, v string) bool {
			if k >= lt {
				return true
			}
			if k < ge {
				return false
			}
			return cb(k, v)
		})
	}
	loGe, _ := match.Allowable(ge)
	_, hiLt := match.Allowable(lt)
	pred := func(k string) bool {
		geOK := k >= ge || !x.IsLiteral(ge) && match.Match(k, ge)
		ltOK := k < lt || !x.IsLiteral(lt) && match.Match(k, lt)
		return geOK && ltOK
	}
	cb = matchFilter(pred, cb)
	if dir == x.RangeAsc {
		return tx.AscendGreaterOrEqual("", loGe, upperBoundCutoff(hiLt, cb))
	}
	return tx.DescendLessOrEqual("", hiLt, lowerBoundCutoff(loGe, cb))
}

func applySingleBoundaryLiteralASC(tx *buntdb.Tx, op string, pivot string, cb func(k, v string) bool) error {
	cb = nsGuard(pivot, cb)
	switch op {
	case "gt":
		return tx.AscendGreaterOrEqual("", x.NextLex(pivot), cb)
	case "gte":
		return tx.AscendGreaterOrEqual("", pivot, cb)
	case "lt":
		return tx.AscendLessThan("", pivot, cb)
	case "lte":
		return tx.AscendLessThan("", x.NextLex(pivot), cb)
	}
	panic("applySingleBoundaryLiteralASC: unknown op " + op)
}

func applySingleBoundaryLiteralDESC(tx *buntdb.Tx, op string, pivot string, cb func(k, v string) bool) error {
	cb = nsGuard(pivot, cb)
	switch op {
	case "gt":
		return tx.DescendRange("", "\xFF\xFF\xFF\xFF", pivot, cb)
	case "gte":
		return tx.DescendLessOrEqual("", "\xFF\xFF\xFF\xFF",
			lowerBoundCutoff(pivot, cb))
	case "lt":
		return tx.DescendLessOrEqual("", x.NextLex(pivot), func(k, v string) bool {
			if k >= pivot {
				return true
			}
			return cb(k, v)
		})
	case "lte":
		return tx.DescendLessOrEqual("", pivot, cb)
	}
	panic("applySingleBoundaryLiteralDESC: unknown op " + op)
}

func applySingleBoundaryPattern(tx *buntdb.Tx, dir x.RangeDirection, op string, pivot string, limit int, fn func(k, v string) bool) error {
	lo, hi := match.Allowable(pivot)
	var pred func(k string) bool
	switch op {
	case "gt":
		pred = func(k string) bool { return k > pivot && match.Match(k, pivot) }
	case "gte":
		pred = func(k string) bool { return k >= pivot && match.Match(k, pivot) }
	case "lt":
		pred = func(k string) bool { return k < pivot && match.Match(k, pivot) }
	case "lte":
		pred = func(k string) bool { return k <= pivot && match.Match(k, pivot) }
	default:
		panic("applySingleBoundaryPattern: unknown op " + op)
	}
	if dir == x.RangeAsc {
		return tx.AscendGreaterOrEqual("", lo,
			upperBoundCutoff(hi, matchFilter(pred, withLimit(limit, fn))))
	}
	return tx.DescendLessOrEqual("", hi,
		lowerBoundCutoff(lo, matchFilter(pred, withLimit(limit, fn))))
}

func applySingleBoundary(tx *buntdb.Tx, op string, pivot string, limit int, dir x.RangeDirection, fn func(key, value string) bool) error {
	if x.IsLiteral(pivot) {
		cb := withLimit(limit, fn)
		if dir == x.RangeAsc {
			return applySingleBoundaryLiteralASC(tx, op, pivot, cb)
		}
		return applySingleBoundaryLiteralDESC(tx, op, pivot, cb)
	}
	return applySingleBoundaryPattern(tx, dir, op, pivot, limit, fn)
}

func applyPatternRange(tx *buntdb.Tx, p string, limit int, dir x.RangeDirection, fn func(key, value string) bool) error {
	if dir == x.RangeAsc {
		if p == "" {
			return nil
		}
		if p[0] == '*' {
			if p == "*" {
				return tx.Ascend("", withLimit(limit, fn))
			}
			cb := matchFilter(func(k string) bool { return match.Match(k, p) }, withLimit(limit, fn))
			return tx.Ascend("", cb)
		}
		min, max := match.Allowable(p)
		cb := upperBoundCutoff(max,
			matchFilter(func(k string) bool { return match.Match(k, p) },
				withLimit(limit, fn)))
		return tx.AscendGreaterOrEqual("", min, cb)
	}
	if p == "" {
		return nil
	}
	if p[0] == '*' {
		if p == "*" {
			return tx.Descend("", withLimit(limit, fn))
		}
		cb := matchFilter(func(k string) bool { return match.Match(k, p) }, withLimit(limit, fn))
		return tx.Descend("", cb)
	}
	min, max := match.Allowable(p)
	cb := lowerBoundCutoff(min,
		matchFilter(func(k string) bool { return match.Match(k, p) },
			withLimit(limit, fn)))
	return tx.DescendLessOrEqual("", max, cb)
}

func applyKeyRange(tx *buntdb.Tx, kr x.KeyRange, dir x.RangeDirection, fn func(key, value string) bool) error {
	kind, pa, pb, limit := x.InspectKeyRange(kr)
	switch kind {
	case x.KeyRangeBt:
		return applyBtRange(tx, pa, pb, limit, dir, fn)
	case x.KeyRangeGt:
		return applySingleBoundary(tx, "gt", pa, limit, dir, fn)
	case x.KeyRangeGte:
		return applySingleBoundary(tx, "gte", pa, limit, dir, fn)
	case x.KeyRangeLt:
		return applySingleBoundary(tx, "lt", pa, limit, dir, fn)
	case x.KeyRangeLte:
		return applySingleBoundary(tx, "lte", pa, limit, dir, fn)
	case x.KeyRangePattern:
		return applyPatternRange(tx, pa, limit, dir, fn)
	}
	panic("applyKeyRange: unhandled KeyRangeKind " + kind.String())
}

// ——— Registry drop + atomic batched writes ———
//
// Registry-drop semantics:
//   - DROPIDX = dropIndexByFullName (deletes btree handle + idx meta key +
//     idxRegSpec map entry; coarse idxRegMu lock held for the entire op,
//     symmetric with DROPSCH).
//   - DROPSCH = dropSchemaByLogicalNs. Extra guard vs DROPIDX: attached
//     index check — any idxRegSpec ownerNs matching logicalNs (disk XOR mem)
//     → hard ERR listing the names, caller must DROPIDX first.
//
// Batch atomicity invariants (setBatchAtomic / deleteBatchAtomic):
//   - All keys in one batch target the SAME storage layer (disk XOR mem),
//     i.e. the caller already ensured a consistent Mem flag across payloads.
//   - The whole batch runs inside a single BuntDB Update tx; any sub-error
//     aborts the tx so no half-written state survives.

// dropIndexByFullName is the DROPIDX command implementation (1 arg = fullName
// or 2 args = ownerNs + logical, resolved in cmd.go dropIdxCommand). idxRegMu
// is held for the entire op (spec read → btree DropIndex → meta probe/delete
// → map delete). Ctrl ops are low frequency so coarse lock scope keeps the
// reasoning symmetric with DROPSCH.
func (db *DB) dropIndexByFullName(fullName string) error {
	if db == nil {
		return errors.New("dropIndexByFullName: db is nil")
	}
	if fullName == "" {
		return errors.New("dropIndexByFullName: full_name is required")
	}
	ownerNs, _, err := naming.ParseIdxFullName(fullName)
	if err != nil {
		return fmt.Errorf("dropIndexByFullName: %w", err)
	}
	layer, constrained, lerr := resolveLayer(ownerNs)
	if lerr != nil || !constrained {
		return fmt.Errorf("dropIndexByFullName: ownerNs %q is not a concrete ns (err=%v constrained=%v)", ownerNs, lerr, constrained)
	}
	// Symmetric with dropSchemaByLogicalNs: acquire the registry write-lock up
	// front for the entire operation (spec lookup → btree DropIndex → meta
	// probe/delete → map delete). Ctrl registry ops are low frequency so the
	// coarse lock scope keeps reasoning simple and symmetric with DROPSCH.
	db.idxRegMu.Lock()
	defer db.idxRegMu.Unlock()
	spec, ok := db.idxRegSpec[fullName]
	if !ok {
		return fmt.Errorf("dropIndexByFullName: index %q not registered", fullName)
	}
	if err := db.store(layer).DropIndex(fullName); err != nil && !errors.Is(err, buntdb.ErrNotFound) {
		return fmt.Errorf("dropIndexByFullName: drop btree: %w", err)
	}
	glob := naming.IdxMetaGlobFor(fullName)
	hasAnyMeta := false
	if err := db.store(layer).View(func(tx *buntdb.Tx) error {
		_ = tx.AscendKeys(glob, func(_, _ string) bool {
			hasAnyMeta = true
			return false
		})
		return nil
	}); err != nil {
		return fmt.Errorf("dropIndexByFullName: probe %s meta: %w", fullName, err)
	}
	if hasAnyMeta {
		if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
			return deleteIdxSpecMeta(tx, fullName)
		}); err != nil {
			return fmt.Errorf("dropIndexByFullName: delete %s meta: %w", fullName, err)
		}
	}
	delete(db.idxRegSpec, fullName)
	slog.Debug("index dropped",
		"name", fullName, "owner_ns", spec.OwnerNs,
		"logical", spec.Logical, "mem", naming.HasMemPrefix(spec.OwnerNs))
	return nil
}

// dropSchemaByLogicalNs is the DROPSCH command implementation. It checks for
// attached indexes first (hard ERR with names when count>0; caller drops them
// via DROPIDX first), then deletes meta keys + docRegSpec entries on disk
// AND mem layers (both candidates probed). Coarse docRegMu lock is held for
// the candidates loop (symmetric with DROPIDX).
func (db *DB) dropSchemaByLogicalNs(logicalNs string) error {
	if db == nil {
		return errors.New("dropSchemaByLogicalNs: db is nil")
	}
	if logicalNs == "" {
		return errors.New("dropSchemaByLogicalNs: logical ns is required")
	}
	if err := naming.ValidateDocLogicalNamespace(logicalNs); err != nil {
		return fmt.Errorf("dropSchemaByLogicalNs: %w", err)
	}
	memStorageNs := naming.BuildStorageNs(logicalNs, true)
	diskStorageNs := naming.BuildStorageNs(logicalNs, false)
	db.idxRegMu.RLock()
	attachedNames := lo.FilterMap(lo.Entries(db.idxRegSpec), func(e lo.Entry[string, idxSpec], _ int) (string, bool) {
		base, _ := naming.StripMemPrefixIfMem(e.Value.OwnerNs)
		return e.Key, base == logicalNs || e.Value.OwnerNs == memStorageNs || e.Value.OwnerNs == diskStorageNs
	})
	db.idxRegMu.RUnlock()
	if len(attachedNames) > 0 {
		sort.Strings(attachedNames)
		return fmt.Errorf("doc %q still has %d attached index(es): %s — drop indexes first via DROPIDX", logicalNs, len(attachedNames), strings.Join(attachedNames, ","))
	}
	candidates := []struct {
		storageNs string
		layer     storageLayer
	}{
		{storageNs: diskStorageNs, layer: storageDisk},
		{storageNs: memStorageNs, layer: storageMem},
	}
	db.docRegMu.Lock()
	defer db.docRegMu.Unlock()
	anyFound := false
	for _, cand := range candidates {
		foundOnLayer := false
		if viewErr := db.store(cand.layer).View(func(tx *buntdb.Tx) error {
			_ = tx.AscendKeys(naming.DocMetaGlobFor(cand.storageNs), func(_, _ string) bool {
				foundOnLayer = true
				return false
			})
			return nil
		}); viewErr != nil {
			return fmt.Errorf("dropSchemaByLogicalNs: probe %s meta: %w", cand.storageNs, viewErr)
		}
		if foundOnLayer {
			anyFound = true
			if upErr := db.store(cand.layer).Update(func(tx *buntdb.Tx) error {
				return deleteDocSpecMeta(tx, cand.storageNs)
			}); upErr != nil {
				return fmt.Errorf("dropSchemaByLogicalNs: delete %s meta: %w", cand.storageNs, upErr)
			}
		}
		if _, ok := db.docRegSpec[cand.storageNs]; ok {
			anyFound = true
			delete(db.docRegSpec, cand.storageNs)
		}
	}
	if !anyFound {
		return fmt.Errorf("dropSchemaByLogicalNs: doc %q not registered (disk+mem both empty)", logicalNs)
	}
	slog.Debug("doc dropped", "logical_ns", logicalNs)
	return nil
}

// setBatchAtomic writes many (Key, Value, TTL) tuples inside a SINGLE BuntDB
// Update tx so either all tuples commit or none do. Used by cmd.go
// setCommand/setExCommand/setNxCommand for doc-path multi-JSON mode.
//
// Enforced invariants:
//   - One storage layer only (disk XOR mem) — if the batch mixes "_m_:" keys
//     with regular keys the caller gets a "cannot span storage layers" ERR.
//   - No duplicate keys within a single batch (pk collision on doc-path same
//     namespace).
//   - nxMode=true (SETNX): ALL keys must be not-found before this call; any
//     single existing key → errNxPreconditionFailed (SETNX all-or-nothing
//     semantics, cmd.go returns integer count of actually-set keys = 0).
func (db *DB) setBatchAtomic(batch []batchedWrite, nxMode bool) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	layers := make(map[storageLayer][]batchedWrite, 2)
	for _, w := range batch {
		l, constrained, lerr := resolveLayer(w.Key)
		if lerr != nil || !constrained {
			return 0, fmt.Errorf("setBatchAtomic: key %q is not a concrete key (err=%v constrained=%v)", w.Key, lerr, constrained)
		}
		layers[l] = append(layers[l], w)
	}
	if len(layers) != 1 {
		return 0, errors.New("atomic batch writes cannot span storage layers; pick consistent Mem flag across payloads")
	}
	var layer storageLayer
	var writes []batchedWrite
	for l, ws := range layers {
		layer = l
		writes = ws
	}
	applied := 0
	err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		seen := make(map[string]struct{}, len(writes))
		if lo.ContainsBy(writes, func(w batchedWrite) bool { return w.Key == "" }) {
			return errors.New("batch: empty key")
		}
		for _, w := range writes {
			if _, dup := seen[w.Key]; dup {
				return fmt.Errorf("batch: duplicate key %q in single SET", w.Key)
			}
			seen[w.Key] = struct{}{}
		}
		if nxMode {
			for _, w := range writes {
				_, err := tx.Get(w.Key)
				if err == nil {
					return errNxPreconditionFailed
				}
				if err != buntdb.ErrNotFound {
					return err
				}
			}
		}
		for _, w := range writes {
			opt := setOptionsForTTL(w.TTL)
			_, _, err := tx.Set(w.Key, w.Value, opt)
			if err != nil {
				return err
			}
			applied++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return applied, nil
}

// deleteBatchAtomic deletes many keys inside a single BuntDB Update tx
// (all-or-nothing). Used by cmd.go delCommand for doc-path multi-pk mode:
// `DEL <registered-ns> <pk1> [pk2 …]`.
//
// Same invariants as setBatchAtomic: single storage layer, no duplicate keys
// within the batch. The "KV-path multi-DEL forbidden" guard (argc≥3 +
// classifyArg1(arg1) has ':') is enforced above this in cmd.go delCommand.
func (db *DB) deleteBatchAtomic(keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	layers := make(map[storageLayer][]string, 2)
	for _, k := range keys {
		l, constrained, lerr := resolveLayer(k)
		if lerr != nil || !constrained {
			return 0, fmt.Errorf("deleteBatchAtomic: key %q is not a concrete key (err=%v constrained=%v)", k, lerr, constrained)
		}
		layers[l] = append(layers[l], k)
	}
	if len(layers) != 1 {
		return 0, errors.New("atomic batch deletes cannot span storage layers")
	}
	var layer storageLayer
	var ks []string
	for l, list := range layers {
		layer = l
		ks = list
	}
	seen := make(map[string]struct{}, len(ks))
	for _, k := range ks {
		if _, dup := seen[k]; dup {
			return 0, fmt.Errorf("batch: duplicate key %q in single DEL", k)
		}
		seen[k] = struct{}{}
	}
	deleted := 0
	err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		for _, k := range ks {
			val, err := tx.Delete(k)
			if err != nil && err != buntdb.ErrNotFound {
				return err
			}
			if err == nil && len(val) > 0 {
				deleted++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}
