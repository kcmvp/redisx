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

	"github.com/kcmvp/redisx/internal"
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
		disk:        disk,
		mem:         mem,
		indexLayers: map[string]storageLayer{},
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
	disk        *buntdb.DB
	mem         *buntdb.DB
	indexLayers map[string]storageLayer

	docRegMu   sync.Mutex
	docRegSpec map[string]x.PersistentDocSpec
	docRegType map[string]string

	idxRegMu   sync.Mutex
	idxRegSpec map[string]x.PersistentIndexSpec
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
	return strings.HasPrefix(key, x.MemNsPrefix)
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

func (db *DB) ensureDocReg() {
	if db.docRegSpec == nil {
		db.docRegSpec = map[string]x.PersistentDocSpec{}
	}
	if db.docRegType == nil {
		db.docRegType = map[string]string{}
	}
}

func (db *DB) ensureIdxReg() {
	if db.idxRegSpec == nil {
		db.idxRegSpec = map[string]x.PersistentIndexSpec{}
	}
	if db.indexLayers == nil {
		db.indexLayers = map[string]storageLayer{}
	}
}

// registerIndexFromSpec creates exactly one BuntDB index and persists the
// PersistentIndexSpec to storage. It is used by both the boot-time rebuild
// path (buildIndexes, which skips persistence since the key already
// exists) and the runtime regidx admin path (which writes the key).
//
// Calling it twice with the identical PersistentIndexSpec on the same DB is a
// no-op (idempotent). Conflicting specs for the same FullName return a
// descriptive error.
func (db *DB) registerIndexFromSpec(spec x.PersistentIndexSpec, persist bool) error {
	if spec.FullName == "" {
		return errors.New("index spec: full_name is required")
	}
	if spec.KeyPattern == "" {
		return fmt.Errorf("index %q: key_pattern is required", spec.FullName)
	}
	if spec.Path == "" {
		return fmt.Errorf("index %q: path is required", spec.FullName)
	}
	layer, constrained, err := resolvePatternLayer(spec.KeyPattern)
	if err != nil {
		return fmt.Errorf("index %q: %w", spec.FullName, err)
	}
	if !constrained {
		return fmt.Errorf("index %q: key_pattern cannot start with wildcard", spec.FullName)
	}

	db.idxRegMu.Lock()
	defer db.idxRegMu.Unlock()
	db.ensureIdxReg()

	existing, ok := db.idxRegSpec[spec.FullName]
	if ok {
		identical := existing.FullName == spec.FullName &&
			existing.OwnerNs == spec.OwnerNs &&
			existing.Logical == spec.Logical &&
			existing.OwnerMem == spec.OwnerMem &&
			existing.KeyPattern == spec.KeyPattern &&
			existing.Path == spec.Path
		if identical {
			return nil
		}
		return fmt.Errorf(
			"index %q already registered with owner_ns=%q pattern=%q path=%q; "+
				"conflicting registration requested: owner_ns=%q pattern=%q path=%q",
			spec.FullName,
			existing.OwnerNs, existing.KeyPattern, existing.Path,
			spec.OwnerNs, spec.KeyPattern, spec.Path)
	}

	if err := db.store(layer).CreateIndex(spec.FullName, spec.KeyPattern, buntdb.IndexJSON(spec.Path)); err != nil {
		return fmt.Errorf("index %q: create btree: %w", spec.FullName, err)
	}
	db.idxRegSpec[spec.FullName] = spec
	db.indexLayers[spec.FullName] = layer

	if persist {
		metaKey := x.IdxMetaNsPrefix + x.StorageKeySeparator + spec.FullName
		raw, err := json.Marshal(spec)
		if err != nil {
			return fmt.Errorf("index %q: marshal PersistentIndexSpec: %w", spec.FullName, err)
		}
		if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
			_, _, setErr := tx.Set(metaKey, string(raw), nil)
			return setErr
		}); err != nil {
			return fmt.Errorf("index %q: persist %s: %w", spec.FullName, metaKey, err)
		}
	}

	slog.Debug("index registered",
		"name", spec.FullName, "owner_ns", spec.OwnerNs,
		"logical", spec.Logical, "mem", spec.OwnerMem,
		"pattern", spec.KeyPattern, "path", spec.Path)
	return nil
}

// CreateIndex converts legacy x.Index declarations into PersistentIndexSpec
// and registers them without persisting "_idx_:*" meta keys. It exists for
// test fixtures; production indexes are Admin-CLI-managed "_idx_:*" records
// rebuilt on every boot via buildIndexes.
func (db *DB) CreateIndex(idx x.Index) error {
	return db.registerIndexes(idx)
}

// registerIndexes converts legacy x.Index declarations into PersistentIndexSpec
// and forwards them to registerIndexFromSpec without persisting "_idx_:*" meta
// keys. Duplicate specs are rejected (idempotent: only exact duplicates pass).
func (db *DB) registerIndexes(indexes ...x.Index) error {
	db.ensureIdxReg()
	for _, idx := range indexes {
		fullName := idx.Name()
		if fullName == "" {
			return fmt.Errorf("index name is required")
		}
		if _, ok := db.idxRegSpec[fullName]; ok {
			return fmt.Errorf("index already declared: %s", fullName)
		}
		ownerNs, logical, err := internal.ParseIndexFullName(fullName)
		if err != nil {
			return fmt.Errorf("index %q: parse full name: %w", fullName, err)
		}
		ownerMem := strings.HasPrefix(idx.KeyPattern(), x.MemNsPrefix+x.StorageKeySeparator) ||
			strings.HasPrefix(ownerNs, x.MemNsPrefix)
		spec := x.PersistentIndexSpec{
			FullName:   fullName,
			OwnerNs:    ownerNs,
			Logical:    logical,
			OwnerMem:   ownerMem,
			KeyPattern: idx.KeyPattern(),
			Path:       idx.Path(),
		}
		if err := db.registerIndexFromSpec(spec, false); err != nil {
			return err
		}
	}
	return nil
}

// buildIndexes rebuilds BuntDB indexes from persisted "_idx_:*" meta records.
// A corrupt "_idx_:*" record is a hard error.
func (db *DB) buildIndexes() error {
	patterns := []struct {
		layer storageLayer
		glob  string
	}{
		{layer: storageDisk, glob: x.IdxMetaNsPrefix + x.StorageKeySeparator + "*"},
		{layer: storageMem, glob: x.IdxMetaNsPrefix + x.StorageKeySeparator + "*"},
	}
	for _, p := range patterns {
		err := db.store(p.layer).View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(p.glob, func(metaKey, value string) bool {
				var spec x.PersistentIndexSpec
				if err := json.Unmarshal([]byte(value), &spec); err != nil {
					err = fmt.Errorf("index meta %q: decode PersistentIndexSpec: %w", metaKey, err)
					slog.Error(err.Error())
					// Close handled at the top-level via fatal flow; signal upward via non-nil layer error
					return false
				}
				if spec.FullName == "" {
					err := fmt.Errorf("index meta %q: spec has empty full_name (corrupt _idx_ record)", metaKey)
					slog.Error(err.Error())
					return false
				}
				if err := db.registerIndexFromSpec(spec, false); err != nil {
					slog.Error("failed to rebuild index from _idx_ record", "meta_key", metaKey, "error", err)
					return false
				}
				return true
			})
		})
		if err != nil {
			return err
		}
	}
	slog.Debug("index registry rebuilt", "count", len(db.idxRegSpec))
	return nil
}

func (db *DB) registerDocFromSpec(spec x.PersistentDocSpec, rt reflect.Type) error {
	if rt != nil && rt.Kind() != reflect.String {
		return fmt.Errorf("doc %q: schema implementors must be ~string types, got %s",
			spec.Namespace, rt.Kind())
	}
	if spec.Namespace == "" {
		return errors.New("doc schema: namespace is required")
	}
	if strings.ContainsAny(spec.Namespace, x.StorageKeySeparator+"*?_") {
		return fmt.Errorf("doc %q: namespace must not contain reserved characters %q",
			spec.Namespace, x.StorageKeySeparator+"*?_")
	}
	for i, p := range spec.KeyAttrs {
		if p == "" {
			return fmt.Errorf("doc %q: key_attrs[%d] is empty", spec.Namespace, i)
		}
	}
	storageNs := spec.StorageNs()
	metaKey := x.DocMetaNsPrefix + x.StorageKeySeparator + storageNs

	db.docRegMu.Lock()
	defer db.docRegMu.Unlock()
	db.ensureDocReg()

	existing, registered := db.docRegSpec[storageNs]
	if registered {
		identical := existing.Namespace == spec.Namespace &&
			existing.Mem == spec.Mem &&
			reflect.DeepEqual(existing.KeyAttrs, spec.KeyAttrs) &&
			existing.TTL == spec.TTL
		if !identical {
			return fmt.Errorf(
				"namespace %q already registered by %q (keyattrs=%v ttl=%s); "+
					"incompatible with %q (keyattrs=%v ttl=%s)",
				storageNs, existing.TypeName, existing.KeyAttrs, existing.TTL,
				spec.TypeName, spec.KeyAttrs, spec.TTL)
		}
		return nil
	}

	if spec.TypeName == "" && rt != nil {
		spec.TypeName = rt.String()
	}
	if spec.TypeName == "" {
		spec.TypeName = fmt.Sprintf("unknown<ns=%s>", storageNs)
	}

	raw, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("doc %q: marshal PersistentDocSpec: %w", storageNs, err)
	}
	layer := storageDisk
	if spec.Mem {
		layer = storageMem
	}
	if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		_, _, setErr := tx.Set(metaKey, string(raw), nil)
		return setErr
	}); err != nil {
		return fmt.Errorf("doc %q: persist %s: %w", storageNs, metaKey, err)
	}

	db.docRegSpec[storageNs] = spec
	db.docRegType[storageNs] = spec.TypeName
	slog.Debug("doc schema registered",
		"namespace", spec.Namespace, "storage_ns", storageNs,
		"mem", spec.Mem, "keyattrs", spec.KeyAttrs,
		"ttl", spec.TTL, "type", spec.TypeName)
	return nil
}

// registerSchemas eagerly registers a batch of Schema values at server boot.
// Each schema value is normally the zero-value of a ~string Document alias
// (e.g. UserDoc("")). It enforces the ~string guard, detects namespace
// conflicts, writes PersistentDocSpec to "_doc_:<storage_ns>" on the correct
// storage layer (disk or memory-layer for Mem=true docs), and is idempotent:
// re-registering the identical schema is a no-op.
func (db *DB) registerSchemas(schemas ...x.Schema) error {
	for _, sch := range schemas {
		rt := reflect.TypeOf(sch)
		spec := x.PersistentDocSpec{
			Namespace: sch.Namespace(),
			Mem:       sch.Mem(),
			KeyAttrs:  append([]string(nil), sch.KeyAttrs()...),
			TTL:       sch.TTL(),
		}
		if rt != nil {
			spec.TypeName = rt.String()
		}
		if err := db.registerDocFromSpec(spec, rt); err != nil {
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
	layer, ok := db.indexLayers[indexName]
	if !ok {
		return mo.Err[[]string](fmt.Errorf("index not found: %s", indexName))
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
	i := strings.IndexByte(pivot, ':')
	if i < 0 {
		return fn
	}
	scope := pivot[:i+1]
	n := i + 1
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
