package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	naming "github.com/kcmvp/redisx/internal/naming"
	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/match"
	"github.com/tidwall/sjson"

	"github.com/kcmvp/redisx/x"
)

type storageLayer uint8

const (
	storageDisk storageLayer = iota
	storageMem
)

type docSpec struct {
	Namespace string        `json:"namespace"`
	Mem       bool          `json:"mem"`
	KeyAttrs  []string      `json:"key_attrs"`
	TTL       time.Duration `json:"ttl_ns"`
	TypeName  string        `json:"type_name"`
}

func (p docSpec) StorageNs() string {
	return naming.BuildStorageNs(p.Namespace, p.Mem)
}

type idxSpec struct {
	OwnerNs    string   `json:"owner_ns"`
	Logical    string   `json:"logical"`
	KeyPattern string   `json:"key_pattern"`
	Paths      []string `json:"paths"`
}

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

// SECTION: Open

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

// DB is a lightweight two-layer BuntDB wrapper for the high-frequency
// operations exposed by redisx.
//
// It is designed for two in-process use cases:
//   - direct embedded access from the same application
//   - the backing implementation behind RESP X commands
//
// redisx always opens two underlying BuntDB instances:
//   - one primary layer from dbPath
//   - one dedicated memory-only layer for keys prefixed with "_m_"
//
// For lower-level or BuntDB-specific operations, use [DB.Raw].
type DB struct {
	disk *buntdb.DB
	mem  *buntdb.DB

	docRegMu   sync.RWMutex
	docRegSpec map[string]docSpec

	idxRegMu   sync.RWMutex
	idxRegSpec map[string]idxSpec
}

// SECTION: Core DB

// Raw exposes the primary (disk) BuntDB instance for advanced/native use.
// This is the layer opened from dbPath.
func (db *DB) Raw() *buntdb.DB {
	return db.disk
}

// RawMem exposes the in-memory storage layer used by keys with the "_m_"
// prefix.
//
// Keys routed to this layer are not persisted across restarts.
func (db *DB) RawMem() *buntdb.DB {
	return db.mem
}

// Close closes both the persistent and in-memory storage layers.
//
// The first close error is returned, if any.
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

// SECTION: Storage Routing

func isMemKey(key string) bool {
	return naming.HasMemPrefix(key)
}

func hasLeadingWildcard(keyPattern string) bool {
	return keyPattern != "" && (keyPattern[0] == '*' || keyPattern[0] == '?')
}

func layerForKey(key string) storageLayer {
	if isMemKey(key) {
		return storageMem
	}
	return storageDisk
}

// resolvePatternLayer reports whether keyPattern alone pins the target layer.
// A leading wildcard is treated as unconstrained, not an error.
func resolvePatternLayer(keyPattern string) (storageLayer, bool, error) {
	if keyPattern == "" {
		return storageDisk, false, errors.New("key pattern is required")
	}
	if hasLeadingWildcard(keyPattern) {
		return storageDisk, false, nil
	}
	return layerForKey(keyPattern), true, nil
}

func layerForStorageNs(storageNs string) storageLayer {
	if naming.HasMemPrefix(storageNs) {
		return storageMem
	}
	return storageDisk
}

// store returns the underlying storage handle for layer.
func (db *DB) store(layer storageLayer) *buntdb.DB {
	if layer == storageMem {
		return db.mem
	}
	return db.disk
}

func setOptionsForTTL(ttl time.Duration) *buntdb.SetOptions {
	if ttl <= 0 {
		return nil
	}
	return &buntdb.SetOptions{Expires: true, TTL: ttl}
}

// SECTION: Query And Update

// Update applies one or more x.Mutation values to every doc matched by kr
// and filter. Each mutation uses sjson path semantics. keyPattern must be
// one full storage-key glob (cannot start with wildcard).
func (db *DB) Update(kr x.KeyRange, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	layerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		return layerForKey(k)
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	layer, ok := layerObj.(storageLayer)
	if !ok {
		return mo.Err[[]string](fmt.Errorf("layerForKey returned non-storageLayer type %T for key range routing", layerObj))
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

// applyIndexSpec is the atomic executor that applies a single idxSpec to
// the running DB: it creates one BuntDB BTree index, records the spec in
// idxRegSpec, and optionally persists it as "_idx_:*" metadata.
//
// It is used by both the boot-time load path (loadIndexes, which skips
// persistence since the key already exists on disk) and the runtime
// regidx admin path (which writes the key).
//
// Calling it twice with the identical idxSpec on the same DB is a
// no-op (idempotent). Conflicting specs for the same FullName return a
// descriptive error.
func (db *DB) applyIndexSpec(spec idxSpec, persist bool) error {
	if spec.OwnerNs == "" {
		return errors.New("index spec: owner_ns is required")
	}
	if spec.Logical == "" {
		return errors.New("index spec: logical is required")
	}
	fullName := spec.FullName()
	if spec.KeyPattern == "" {
		return fmt.Errorf("index %q: key_pattern is required", fullName)
	}
	if len(spec.Paths) == 0 {
		return fmt.Errorf("index %q: at least one json path is required (single-field or composite multi-field)", fullName)
	}
	for i, p := range spec.Paths {
		if p == "" {
			return fmt.Errorf("index %q: paths[%d] is empty", fullName, i)
		}
	}
	layer, constrained, err := resolvePatternLayer(spec.KeyPattern)
	if err != nil {
		return fmt.Errorf("index %q: %w", fullName, err)
	}
	if !constrained {
		return fmt.Errorf("index %q: key_pattern cannot start with wildcard", fullName)
	}

	db.idxRegMu.Lock()
	defer db.idxRegMu.Unlock()

	if _, exists := db.idxRegSpec[fullName]; exists {
		slog.Debug("applyIndexSpec: index already in registry, skipping", "name", fullName)
		return nil
	}

	if err := db.store(layer).CreateIndex(fullName, spec.KeyPattern, indexJSONComposite(spec.Paths...)); err != nil {
		return fmt.Errorf("index %q: create btree: %w", fullName, err)
	}
	db.idxRegSpec[fullName] = spec

	if persist {
		metaKey := naming.IdxMetaKey(fullName)
		raw, err := json.Marshal(spec)
		if err != nil {
			return fmt.Errorf("index %q: marshal idxSpec: %w", fullName, err)
		}
		raw, err = sjson.SetBytes(raw, "created_at", time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return fmt.Errorf("index %q: inject created_at: %w", fullName, err)
		}
		metaLayer := layerForStorageNs(spec.OwnerNs)
		if err := db.store(metaLayer).Update(func(tx *buntdb.Tx) error {
			_, _, setErr := tx.Set(metaKey, string(raw), nil)
			return setErr
		}); err != nil {
			return fmt.Errorf("index %q: persist %s: %w", fullName, metaKey, err)
		}
	}

	slog.Debug("index registered",
		"name", fullName, "owner_ns", spec.OwnerNs,
		"logical", spec.Logical, "mem", naming.HasMemPrefix(spec.OwnerNs),
		"pattern", spec.KeyPattern, "paths", spec.Paths)
	return nil
}

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

// CreateIndex converts legacy x.Index declarations into idxSpec
// and registers them without persisting "_idx_:*" meta keys. It exists for
// test fixtures; production indexes are Admin-CLI-managed "_idx_:*" records
// reloaded on every boot via loadIndexes.
func (db *DB) CreateIndex(idx x.Index) error {
	return db.registerIndexes(idx)
}

// registerIndexes converts legacy x.Index declarations into idxSpec
// and forwards them to applyIndexSpec without persisting "_idx_:*" meta
// keys. Duplicate specs are rejected: a fixture that attempts to register
// the same index name twice must drop it first or fix the fixture schema.
func (db *DB) registerIndexes(indexes ...x.Index) error {
	for _, idx := range indexes {
		fullName := idx.Name()
		if fullName == "" {
			return fmt.Errorf("index name is required")
		}
		db.idxRegMu.RLock()
		_, already := db.idxRegSpec[fullName]
		db.idxRegMu.RUnlock()
		if already {
			return fmt.Errorf("index already declared: %s", fullName)
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
		if err := db.applyIndexSpec(spec, false); err != nil {
			return err
		}
	}
	return nil
}

// loadIndexes rebuilds BuntDB indexes from persisted "_idx_:*" meta records.
// A corrupt "_idx_:*" record is a hard error. applyIndexSpec is intentionally
// invoked outside the View transaction to avoid deadlocking on nested
// write-transaction requirements of CreateIndex inside an AscendKeys callback.
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
			if err := db.applyIndexSpec(spec, false); err != nil {
				return fmt.Errorf("index %s: apply: %w", c.metaKey, err)
			}
		}
	}
	slog.Debug("index registry loaded", "count", len(db.idxRegSpec))
	return nil
}

// loadDocSpecs rebuilds the in-memory doc registry from persisted "_doc_:*"
// meta records. A corrupt "_doc_:*" record is a hard error. Runtime type
// information is intentionally not restored (reflect.Type of the caller's
// ~string is only available at the process that eagerly registers via
// Start's schemas argument); the registries' schemas are sufficient for
// the strict gate. applyDocSpec is invoked outside the View transaction
// to avoid deadlocking on the nested persist-write transaction.
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
			if err := db.applyDocSpec(spec, nil); err != nil {
				return fmt.Errorf("doc %s: apply: %w", c.metaKey, err)
			}
		}
	}
	slog.Debug("doc registry loaded", "count", len(db.docRegSpec))
	return nil
}

// applyDocSpec is the atomic executor that applies a single docSpec to
// the running DB: it performs schema conflict checks against docRegSpec,
// records the spec in docRegSpec, and persists the spec as "_doc_:*"
// metadata on the correct storage layer (disk or memory) dictated by
// the spec itself.
//
// rt is optional: if non-nil it must be a ~string type and is used to
// fill spec.TypeName when the caller had compile-time type info
// (e.g. Start's schemas variadic). When restoring from persisted meta
// via loadDocSpecs, rt is nil and type-name derivation is skipped.
func (db *DB) applyDocSpec(spec docSpec, rt reflect.Type) error {
	if rt != nil && rt.Kind() != reflect.String {
		return fmt.Errorf("doc %q: schema implementors must be ~string types, got %s",
			spec.Namespace, rt.Kind())
	}
	if spec.Namespace == "" {
		return errors.New("doc schema: namespace is required")
	}
	if err := naming.ValidateDocLogicalNamespace(spec.Namespace); err != nil {
		return fmt.Errorf("doc %q: %w", spec.Namespace, err)
	}
	for i, p := range spec.KeyAttrs {
		if p == "" {
			return fmt.Errorf("doc %q: key_attrs[%d] is empty", spec.Namespace, i)
		}
	}
	storageNs := spec.StorageNs()
	metaKey := naming.DocMetaKey(storageNs)

	if spec.TypeName == "" && rt != nil {
		spec.TypeName = rt.String()
	}

	db.docRegMu.Lock()
	defer db.docRegMu.Unlock()

	if _, exists := db.docRegSpec[storageNs]; exists {
		slog.Debug("applyDocSpec: namespace already in registry, skipping", "storage_ns", storageNs)
		return nil
	}

	if spec.TypeName == "" {
		spec.TypeName = fmt.Sprintf("unknown<ns=%s>", storageNs)
	}

	raw, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("doc %q: marshal docSpec: %w", storageNs, err)
	}
	raw, err = sjson.SetBytes(raw, "created_at", time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("doc %q: inject created_at: %w", storageNs, err)
	}
	layer := layerForStorageNs(storageNs)
	if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		_, _, setErr := tx.Set(metaKey, string(raw), nil)
		return setErr
	}); err != nil {
		return fmt.Errorf("doc %q: persist %s: %w", storageNs, metaKey, err)
	}

	db.docRegSpec[storageNs] = spec
	slog.Debug("doc schema applied",
		"namespace", spec.Namespace, "storage_ns", storageNs,
		"mem", spec.Mem, "keyattrs", spec.KeyAttrs,
		"ttl", spec.TTL, "type", spec.TypeName)
	return nil
}

// registerSchemas eagerly applies a batch of Schema values at server boot
// by converting each Schema interface into a docSpec and forwarding it to
// applyDocSpec. Each schema value is normally the zero-value of a ~string
// Document alias (e.g. UserDoc("")). When a namespace is already present
// in the runtime registry (restored from persisted "_doc_:*" meta), we
// skip applying the in-process schema copy and emit a warning. Conflicting
// specs across restarts must be resolved via DESDOC / DROPDOC first.
func (db *DB) registerSchemas(schemas ...x.Schema) error {
	for _, sch := range schemas {
		rt := reflect.TypeOf(sch)
		spec := docSpec{
			Namespace: sch.Namespace(),
			Mem:       sch.Mem(),
			KeyAttrs:  append([]string(nil), sch.KeyAttrs()...),
			TTL:       sch.TTL(),
		}
		if rt != nil {
			spec.TypeName = rt.String()
		}
		storageNs := spec.StorageNs()

		db.docRegMu.RLock()
		_, already := db.docRegSpec[storageNs]
		db.docRegMu.RUnlock()
		if already {
			slog.Warn("schema namespace already registered; "+
				"skipping boot-time schema copy (persisted _doc_:* meta wins). "+
				"Drop the namespace first if you want to re-register with different schema",
				"namespace", spec.Namespace, "storage_ns", storageNs, "mem", spec.Mem)
			continue
		}
		if err := db.applyDocSpec(spec, rt); err != nil {
			return err
		}
	}
	return nil
}

// SearchIndex scans one registered index on its bound storage layer,
// narrows by kr and optional filter, and returns matched JSON documents in
// index order. The kr KeyRange cannot start with wildcard and must target
// the same layer as the index.
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
	layer := storageDisk
	if naming.HasMemPrefix(idxSpec.OwnerNs) {
		layer = storageMem
	}
	krLayerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		return layerForKey(k)
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	krLayer, ok := krLayerObj.(storageLayer)
	if !ok {
		return mo.Err[[]string](fmt.Errorf("layerForKey returned non-storageLayer type %T for key range routing", krLayerObj))
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

// SearchKey resolves one sealed x.KeyRange to a single storage layer, walks
// the matching default storage-index key range using native BTree iterators,
// applies the optional filter, and returns matched JSON documents in ASC or
// DESC key order. The KeyRange must pin exactly one storage layer.
func (db *DB) SearchKey(kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	layerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		return layerForKey(k)
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	layer, ok := layerObj.(storageLayer)
	if !ok {
		return mo.Err[[]string](fmt.Errorf("layerForKey returned non-storageLayer type %T for key range routing", layerObj))
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

// SECTION: Key Value

// Set stores one string value under key.
//
// Routing rules:
//   - keys prefixed with "_m_" are stored in the in-memory layer
//   - all other keys are stored in the persistent layer
//
// key must be the full storage key.
//
// RESP equivalent:
//
//	SET <key> <value>
func (db *DB) Set(key string, value string) error {
	return db.store(layerForKey(key)).Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

// SetWithTtl stores one string value under key with an optional TTL.
//
// A positive ttl makes the key expire automatically. A zero or negative ttl
// behaves the same as [DB.Set].
//
// key must be the full storage key.
//
// RESP equivalents:
//
//	SET <key> <value> EX <seconds>
//	SETEX <key> <seconds> <value>
func (db *DB) SetWithTtl(key string, value string, ttl time.Duration) error {
	ttl = db.autoTTLFromKey(key, ttl)
	opt := setOptionsForTTL(ttl)
	if opt == nil {
		return db.Set(key, value)
	}
	return db.store(layerForKey(key)).Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, opt)
		return err
	})
}

// SetNX stores value only when key does not already exist.
//
// The returned boolean reports whether the write happened.
//
// key must be the full storage key.
//
// RESP equivalent:
//
//	SETNX <key> <value>
func (db *DB) SetNX(key string, value string) (bool, error) {
	return db.SetNXWithTtl(key, value, 0)
}

// SetNXWithTtl stores value only when key does not already exist, and applies
// the provided TTL when it is positive.
//
// The returned boolean reports whether the write happened.
//
// key must be the full storage key.
func (db *DB) SetNXWithTtl(key string, value string, ttl time.Duration) (bool, error) {
	ttl = db.autoTTLFromKey(key, ttl)
	var set bool
	opt := setOptionsForTTL(ttl)
	err := db.store(layerForKey(key)).Update(func(tx *buntdb.Tx) error {
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

// Get returns the string value stored under key.
//
// A missing key is returned as an error result.
//
// key must be the full storage key.
//
// RESP equivalent:
//
//	GET <key>
func (db *DB) Get(key string) mo.Result[string] {
	var val string
	err := db.store(layerForKey(key)).View(func(tx *buntdb.Tx) error {
		var innerErr error
		val, innerErr = tx.Get(key)
		return innerErr
	})

	if err != nil {
		return mo.Err[string](err)
	}
	return mo.Ok(val)
}

// Delete removes key from its routed storage layer.
//
// The returned boolean reports whether the key existed.
//
// key must be the full storage key.
//
// RESP equivalent:
//
//	DEL <key>
func (db *DB) Delete(key string) (bool, error) {
	var val string
	err := db.store(layerForKey(key)).Update(func(tx *buntdb.Tx) error {
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

// Keys resolves one full storage-key pattern to one storage layer first, then
// returns all matching keys from that layer.
//
// keyPattern uses key glob matching such as "*" and "?".
//
// RESP equivalent:
//
//	KEYS <key_pattern>
func (db *DB) Keys(keyPattern string) mo.Result[[]string] {
	layer, constrained, err := resolvePatternLayer(keyPattern)
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

// ============================================================
// Internal helpers for atomic batched writes + index drop.
// Batch atomicity enforces single storage layer (disk XOR mem).
// ============================================================

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
	layer := layerForStorageNs(ownerNs)
	db.idxRegMu.Lock()
	defer db.idxRegMu.Unlock()
	spec, ok := db.idxRegSpec[fullName]
	if !ok {
		return fmt.Errorf("dropIndexByFullName: index %q not registered", fullName)
	}
	if err := db.store(layer).DropIndex(fullName); err != nil && !errors.Is(err, buntdb.ErrNotFound) {
		return fmt.Errorf("dropIndexByFullName: drop btree: %w", err)
	}
	metaKey := naming.IdxMetaKey(fullName)
	if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		_, delErr := tx.Delete(metaKey)
		if delErr != nil && !errors.Is(delErr, buntdb.ErrNotFound) {
			return delErr
		}
		return nil
	}); err != nil {
		return fmt.Errorf("dropIndexByFullName: delete %s: %w", metaKey, err)
	}
	delete(db.idxRegSpec, fullName)
	slog.Debug("index dropped",
		"name", fullName, "owner_ns", spec.OwnerNs,
		"logical", spec.Logical, "mem", naming.HasMemPrefix(spec.OwnerNs))
	return nil
}

type batchedWrite struct {
	Key   string
	Value string
	TTL   time.Duration
}

var errNxPreconditionFailed = errors.New("setBatchAtomic: nx precondition failed — one or more keys already exist")

func (db *DB) setBatchAtomic(batch []batchedWrite, nxMode bool) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	layers := make(map[storageLayer][]batchedWrite, 2)
	for _, w := range batch {
		l := layerForKey(w.Key)
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
		for _, w := range writes {
			if w.Key == "" {
				return errors.New("batch: empty key")
			}
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

func (db *DB) deleteBatchAtomic(keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	layers := make(map[storageLayer][]string, 2)
	for _, k := range keys {
		l := layerForKey(k)
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
