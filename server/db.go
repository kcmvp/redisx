package server

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kcmvp/redisx/internal"
	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/kcmvp/redisx/x"
	"github.com/kcmvp/redisx/x/contract"
)

type storageLayer uint8

const (
	storageDisk storageLayer = iota
	storageMem
)

// SECTION: Open

// openDB creates one hybrid DB instance backed by:
//   - one primary layer opened from path
//   - one dedicated memory-only layer used by keys prefixed with "_m_"
//
// path is passed through to buntdb.Open(path).
//
// Use one real database file path such as "/tmp/redisx.db". The special value
// ":memory:" is rejected because redisx already opens its own dedicated
// memory-only layer for keys prefixed with "_m_". For filesystem paths,
// missing parent directories are created automatically, and the database file
// itself is created on first open when it does not already exist. An empty
// path is invalid. Directory paths are rejected.
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
}

// DBX binds one document type to an existing DB.
//
// It keeps the DB-side API symmetric with client/doc while avoiding passing the
// same DB value on every call.
type DBX[D x.Document] DB

// SECTION: Core DB

// Raw exposes the primary storage layer for advanced use cases.
//
// This is useful when your application needs direct transactions, indexes,
// or iteration APIs that are intentionally not re-exposed by redisx.
//
// This is the layer opened from dbPath.
//
// Example:
//
//	raw := db.Raw()
//	err := raw.Update(func(tx *buntdb.Tx) error {
//		_, _, err := tx.Set("native:key", "value", nil)
//		return err
//	})
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

// TODO(Go 1.27): when generic methods are available in the project toolchain,
// fold this helper into (*DB).As[D]() so the typed view is exposed directly
// from DB instead of a package-level helper.
//
// As returns a typed view over db without creating a new wrapper object.
func As[D x.Document](db *DB) *DBX[D] {
	return (*DBX[D])(db)
}

// SECTION: Storage Routing

func isMemKey(key string) bool {
	return strings.HasPrefix(key, contract.MemKeyPrefix)
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

// resolvePatternLayer parses one full storage-key pattern and reports whether
// the pattern itself constrains the operation to a concrete storage layer.
//
// It answers the single routing question shared by all key/index operations:
// can this pattern alone determine the target layer?
//
// The return values mean:
//   - layer: the resolved layer when constrained is true
//   - constrained: whether keyPattern itself pins the operation to one layer
//   - error: syntactic validation failure such as an empty pattern
//
// A leading wildcard does not identify one layer, so it is treated as
// unconstrained rather than as an error. Callers that require a single layer
// must reject constrained == false themselves.
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

type kvPair struct {
	key   string
	value string
}

func setOptionsForTTL(ttl time.Duration) *buntdb.SetOptions {
	if ttl <= 0 {
		return nil
	}
	return &buntdb.SetOptions{Expires: true, TTL: ttl}
}

// SECTION: Query And Update

// The target values must be valid JSON documents. Each x.Mutation is applied
// with sjson semantics, so the path may address top-level or nested fields.
// keyPattern must be one full storage-key pattern, using key glob matching such
// as "*" and "?".
//
// RESP equivalent:
//
//	UPDATE <key_pattern> <json_filter> <update_json>
//
// Example:
//
//	res := db.Update(
//		"user:*",
//		x.And(
//			x.Gte("age", 18),
//			x.Eq("status", "pending"),
//		),
//		x.Set("status", "active"),
//		x.Set("verified", true),
//	)
//	updatedKeys := res.MustGet()
func (db *DB) Update(keyPattern string, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	layer, constrained, err := resolvePatternLayer(keyPattern)
	if err != nil {
		return mo.Err[[]string](err)
	}
	if !constrained {
		return mo.Err[[]string](errors.New("key pattern cannot start with wildcard"))
	}
	var updatedKeys []string
	err = db.store(layer).Update(func(tx *buntdb.Tx) error {
		var matchedKeys []string
		scanErr := tx.AscendKeys(keyPattern, func(key, value string) bool {
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

// registerIndexes creates all declared SEARCHINDEX indexes on the correct
// storage layer.
//
// Routing rules:
//   - key patterns starting with "_m_" are created on the in-memory layer
//   - all other patterns are created on the persistent layer
//
// Index key patterns must not start with "*" or "?" because the target layer
// must be decided at startup time without scanning both layers.
func (db *DB) registerIndexes(defs ...x.Index) error {
	if db.indexLayers == nil {
		db.indexLayers = map[string]storageLayer{}
	}
	for _, def := range defs {
		if def.Name() == "" {
			return errors.New("index name is required")
		}
		if def.KeyPattern() == "" {
			return errors.New("index key pattern is required")
		}
		if def.Path() == "" {
			return errors.New("index json path is required")
		}
		layer, constrained, err := resolvePatternLayer(def.KeyPattern())
		if err != nil {
			return fmt.Errorf("index %w", err)
		}
		if !constrained {
			return errors.New("index key pattern cannot start with wildcard")
		}
		if _, exists := db.indexLayers[def.Name()]; exists {
			return fmt.Errorf("index already declared: %s", def.Name())
		}
		if err := db.store(layer).CreateIndex(def.Name(), def.KeyPattern(), buntdb.IndexJSON(def.Path())); err != nil {
			return err
		}
		db.indexLayers[def.Name()] = layer
	}

	return nil
}

// SearchIndex scans one registered full index name on its bound storage layer,
// narrows results by one full storage-key pattern, applies the optional filter,
// and returns the matched JSON documents ordered by that index.
//
// The index must be declared during server startup with x.Idx[D](...). The
// indexName argument must be the internal full index name, while keyPattern
// must be one full storage-key pattern. indexName chooses the storage layer
// first; keyPattern only narrows matches within that layer. A concrete
// keyPattern that points at a different layer is rejected.
//
// Ordering rules:
//   - ascending when desc is false
//   - descending when desc is true
//
// RESP equivalent:
//
//	SEARCHINDEX <index_name> <key_pattern> <json_filter> [ASC|DESC]
//
// Example:
//
//	res := db.SearchIndex(
//		"idx_user_age",
//		"user:*",
//		x.And(
//			x.Gte("age", 18),
//			x.Eq("status", "active"),
//		),
//		false,
//	)
//	users := res.MustGet()
func (db *DB) SearchIndex(indexName string, keyPattern string, filter x.Filter, desc bool) mo.Result[[]string] {
	if indexName == "" {
		return mo.Err[[]string](errors.New("index name is required"))
	}
	if keyPattern == "" {
		return mo.Err[[]string](errors.New("key pattern is required"))
	}
	layer, ok := db.indexLayers[indexName]
	if !ok {
		return mo.Err[[]string](fmt.Errorf("index not found: %s", indexName))
	}
	patternLayer, constrained, err := resolvePatternLayer(keyPattern)
	if err != nil {
		return mo.Err[[]string](err)
	}
	if constrained && patternLayer != layer {
		return mo.Err[[]string](fmt.Errorf("key pattern targets a different storage layer than index %s", indexName))
	}

	var results []string
	err = db.store(layer).View(func(tx *buntdb.Tx) error {
		iter := tx.Ascend
		if desc {
			iter = tx.Descend
		}
		return iter(indexName, func(key, value string) bool {
			if !buntdb.Match(key, keyPattern) {
				return true
			}
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
		if errors.Is(err, buntdb.ErrNotFound) {
			return mo.Err[[]string](fmt.Errorf("index not found: %s", indexName))
		}
		return mo.Err[[]string](err)
	}
	return mo.Ok(results)
}

// SearchKey resolves one sealed x.KeyRange to one storage layer first, then
// walks the matching default storage-index key range using buntdb's native
// six BTree iterators, applies the optional x.Filter to each JSON value, and
// returns matched documents in key order (ASC or DESC). LIMIT truncation is
// applied inside the native iterator callback via kr.GetLimit() when set.
//
// The KeyRange itself must pin exactly one storage layer: either a literal
// anchor via its Bounds().Lo, or a non-leading-wildcard glob pattern via
// Pattern(). Unanchored KeyRanges (leading-wildcard pattern such as "*foo")
// are rejected with an error exactly as the legacy string-keyPattern path
// used to reject them via resolvePatternLayer.
//
// RESP equivalent:
//
//	SEARCHKEY <keyrange_json> <json_filter> [ASC|DESC] [LIMIT count]
//
// Example:
//
//	res := db.SearchKey(
//		x.KeysBt("user:engineering:0100", "user:engineering:0200").Limit(50),
//		x.Eq("region", "us"),
//		true,
//	)
//	users := res.MustGet()
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
	layer := layerObj.(storageLayer)

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

// SECTION: Typed Document View

// Get loads one document by its document-level key value.
//
// The input key is the resolved JSON-layer key value, not the final storage
// key. The storage namespace is derived from D automatically.
func (dbx *DBX[D]) Get(key string) (D, error) {
	var zero D
	if dbx == nil {
		return zero, errors.New("db is nil")
	}

	res := (*DB)(dbx).Get(x.StorageKeyValue[D](key))
	if res.IsError() {
		return zero, res.Error()
	}
	return D(res.MustGet()), nil
}

// Set stores one document using its derived storage key and document TTL.
func (dbx *DBX[D]) Set(d D) error {
	if dbx == nil {
		return errors.New("db is nil")
	}

	key, err := x.StorageKey(d)
	if err != nil {
		return err
	}
	return (*DB)(dbx).SetWithTtl(key, d.RawJSON(), d.TTL())
}

// SetNX stores one document only when its key does not already exist, using
// the document TTL.
func (dbx *DBX[D]) SetNX(d D) (bool, error) {
	if dbx == nil {
		return false, errors.New("db is nil")
	}

	key, err := x.StorageKey(d)
	if err != nil {
		return false, err
	}
	return (*DB)(dbx).SetNXWithTtl(key, d.RawJSON(), d.TTL())
}

// Delete removes one document by its derived storage key.
func (dbx *DBX[D]) Delete(d D) (bool, error) {
	if dbx == nil {
		return false, errors.New("db is nil")
	}

	key, err := x.StorageKey(d)
	if err != nil {
		return false, err
	}
	return (*DB)(dbx).Delete(key)
}

// Keys returns keys matching the document namespace and key sub-pattern.
func (dbx *DBX[D]) Keys(keyPattern string) mo.Result[[]string] {
	if dbx == nil {
		return mo.Err[[]string](errors.New("db is nil"))
	}
	fullKeyPattern, err := internal.ValidateKeyPattern[D](keyPattern)
	if err != nil {
		return mo.Err[[]string](err)
	}
	return (*DB)(dbx).Keys(fullKeyPattern)
}

// SearchIndex delegates to the underlying DB index search.
func (dbx *DBX[D]) SearchIndex(idxName string, keyPattern string, filter x.Filter, desc bool) mo.Result[[]D] {
	if dbx == nil {
		return mo.Err[[]D](errors.New("db is nil"))
	}

	fullIdxName, err := internal.ValidateIdxName[D](idxName)
	if err != nil {
		return mo.Err[[]D](err)
	}

	fullKeyPattern, err := internal.ValidateKeyPattern[D](keyPattern)
	if err != nil {
		return mo.Err[[]D](err)
	}

	res := (*DB)(dbx).SearchIndex(fullIdxName, fullKeyPattern, filter, desc)
	if res.IsError() {
		return mo.Err[[]D](res.Error())
	}

	raws := res.MustGet()
	out := make([]D, 0, len(raws))
	for _, raw := range raws {
		out = append(out, D(raw))
	}
	return mo.Ok(out)
}

// SearchKey returns documents matching the prefixed scoped key range and filter.
func (dbx *DBX[D]) SearchKey(scopedKR x.KeyRange, filter x.Filter, desc bool) mo.Result[[]D] {
	if dbx == nil {
		return mo.Err[[]D](errors.New("db is nil"))
	}
	if scopedKR == nil {
		return mo.Err[[]D](errors.New("key range is required"))
	}
	fullKR, err := internal.ScopeKeyRange[D](scopedKR)
	if err != nil {
		return mo.Err[[]D](err)
	}

	res := (*DB)(dbx).SearchKey(fullKR, filter, desc)
	if res.IsError() {
		return mo.Err[[]D](res.Error())
	}

	raws := res.MustGet()
	out := make([]D, 0, len(raws))
	for _, raw := range raws {
		out = append(out, D(raw))
	}
	return mo.Ok(out)
}

// Update applies mutations to documents matching the prefixed key pattern.
func (dbx *DBX[D]) Update(keyPattern string, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if dbx == nil {
		return mo.Err[[]string](errors.New("db is nil"))
	}
	fullKeyPattern, err := internal.ValidateKeyPattern[D](keyPattern)
	if err != nil {
		return mo.Err[[]string](err)
	}
	return (*DB)(dbx).Update(fullKeyPattern, filter, values...)
}
