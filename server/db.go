package server

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/kcmvp/redisx/internal"
	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/kcmvp/redisx/x"
)

const keySeparator = ":"

type storageLayer uint8

const (
	storageDisk storageLayer = iota
	storageMem
)

// IndexDef describes one startup index declaration for SEARCHINDEX.
type IndexDef struct {
	// name is the index name used by SEARCHINDEX.
	name string
	// keyPattern is the key glob pattern used to select which keys participate
	// in this index. It supports glob matching such as "*" and "?".
	keyPattern string
	// path is the JSON path used as the indexed value.
	path string
}

// Index declares one JSON index to be created during server startup.
func Index(name, keyPattern, jsonPath string) IndexDef {
	return IndexDef{name: name, keyPattern: keyPattern, path: jsonPath}
}

// Idx declares one JSON index with an auto-generated name.
//
// The generated name uses the "idx_" prefix and derives the suffix from the
// JSON path. For example:
//
//	Idx("user:*", "age")         -> "idx_age"
//	Idx("user:*", "profile.age") -> "idx_profile_age"
func Idx(keyPattern, jsonPath string) IndexDef {
	return Index(idxName(jsonPath), keyPattern, jsonPath)
}

func idxName(jsonPath string) string {
	replacer := strings.NewReplacer(".", "_")
	return "idx_" + replacer.Replace(jsonPath)
}

// resetStorage resets DB-related process state for tests.
//
// The current implementation is intentionally empty because DB instances no
// longer rely on package-level storage singletons.
func resetStorage() {
	// Reset is intentionally a no-op after removing singleton storage state.
}

// openDB creates one hybrid DB instance backed by:
//   - one persistent layer opened from path
//   - one in-memory layer used for keys prefixed with "_m_"
//
// Use ":memory:" when the persistent layer should also remain in memory.
// An empty path is invalid.
func openDB(path string) *DB {
	if path == "" {
		slog.Error("failed to open storage", "error", "db path is required")
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

// DB is a lightweight BuntDB wrapper for the high-frequency operations exposed
// by redisx.
//
// It is designed for two in-process use cases:
//   - direct embedded access from the same application
//   - the backing implementation behind RESP X commands
//
// For lower-level or BuntDB-specific operations, use [DB.Raw].
type DB struct {
	disk        *buntdb.DB
	mem         *buntdb.DB
	indexLayers map[string]storageLayer
}

// Raw exposes the persistent storage layer for advanced use cases.
//
// This is useful when your application needs direct transactions, indexes,
// or iteration APIs that are intentionally not re-exposed by redisx.
//
// Example:
//
//	raw := db.Raw()
//	err := raw.Update(func(tx *buntdb.Tx) error {
//		_, _, err := tx.Set("native:key", "value", nil)
//		return err
//	})
func (x *DB) Raw() *buntdb.DB {
	return x.disk
}

// RawMem exposes the in-memory storage layer used by keys with the "_m_"
// prefix.
//
// Keys routed to this layer are not persisted across restarts.
func (x *DB) RawMem() *buntdb.DB {
	return x.mem
}

// Close closes both the persistent and in-memory storage layers.
//
// The first close error is returned, if any.
func (x *DB) Close() error {
	var err error
	if x.mem != nil {
		if closeErr := x.mem.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if x.disk != nil {
		if closeErr := x.disk.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func isMemKey(key string) bool {
	return strings.HasPrefix(key, internal.MemKeyPrefix)
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

func layersForPattern(keyPattern string) []storageLayer {
	if strings.HasPrefix(keyPattern, internal.MemKeyPrefix) {
		return []storageLayer{storageMem}
	}
	if hasLeadingWildcard(keyPattern) {
		return []storageLayer{storageDisk, storageMem}
	}
	return []storageLayer{storageDisk}
}

func layerForIndexPattern(keyPattern string) (storageLayer, error) {
	if keyPattern == "" {
		return storageDisk, errors.New("index key pattern is required")
	}
	if hasLeadingWildcard(keyPattern) {
		return storageDisk, errors.New("index key pattern cannot start with wildcard")
	}
	return layerForKey(keyPattern), nil
}

// store returns the underlying storage handle for layer.
func (x *DB) store(layer storageLayer) *buntdb.DB {
	if layer == storageMem {
		return x.mem
	}
	return x.disk
}

// storeForKey returns the underlying storage handle selected by key.
func (x *DB) storeForKey(key string) *buntdb.DB {
	return x.store(layerForKey(key))
}

type kvPair struct {
	key   string
	value string
}

func appendUnique(items []string, seen map[string]struct{}, value string) []string {
	if _, ok := seen[value]; ok {
		return items
	}
	seen[value] = struct{}{}
	return append(items, value)
}

// Update applies JSON path updates to all values matched by a key glob pattern
// and an optional filter.
//
// The target values must be valid JSON documents. Each x.Mutation is applied with
// sjson semantics, so the path may address top-level or nested fields.
// keyPattern uses key glob matching such as "*" and "?".
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
func (x *DB) Update(keyPattern string, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if keyPattern == "" {
		return mo.Err[[]string](errors.New("pattern is required"))
	}
	var updatedKeys []string
	seen := map[string]struct{}{}

	for _, layer := range layersForPattern(keyPattern) {
		err := x.store(layer).Update(func(tx *buntdb.Tx) error {
			var matchedKeys []string
			err := tx.AscendKeys(keyPattern, func(key, value string) bool {
				if filter == nil || filter.Eval(value) {
					matchedKeys = append(matchedKeys, key)
				}
				return true
			})

			if err != nil {
				return err
			}

			for _, key := range matchedKeys {
				val, err := tx.Get(key)
				if err != nil {
					return err
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
					_, _, err = tx.Set(key, newVal, nil)
					if err != nil {
						return err
					}
				}
				updatedKeys = appendUnique(updatedKeys, seen, key)
			}

			return nil
		})
		if err != nil {
			return mo.Err[[]string](err)
		}
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
func (x *DB) registerIndexes(defs ...IndexDef) error {
	if x.indexLayers == nil {
		x.indexLayers = map[string]storageLayer{}
	}
	for _, def := range defs {
		if def.name == "" {
			return errors.New("index name is required")
		}
		if def.keyPattern == "" {
			return errors.New("index key pattern is required")
		}
		if def.path == "" {
			return errors.New("index json path is required")
		}
		layer, err := layerForIndexPattern(def.keyPattern)
		if err != nil {
			return err
		}
		if _, exists := x.indexLayers[def.name]; exists {
			return fmt.Errorf("index already declared: %s", def.name)
		}
		if err := x.store(layer).CreateIndex(def.name, def.keyPattern, buntdb.IndexJSON(def.path)); err != nil {
			return err
		}
		x.indexLayers[def.name] = layer
	}

	return nil
}

// SearchIndex scans a registered index, applies the optional filter, and
// returns the matched JSON documents ordered by that index.
//
// The index must be declared during server startup with Index(...) or Idx(...).
//
// Ordering rules:
//   - ascending when desc is false
//   - descending when desc is true
//
// RESP equivalent:
//
//	SEARCHINDEX <index_name> <json_filter> [ASC|DESC]
//
// Example:
//
//			res := db.SearchIndex(
//	             "idx_age",
//				x.And(
//					x.Gte("age", 18),
//					x.Eq("status", "active"),
//				),
//				false,
//			)
//		     users := res.MustGet()
func (xdb *DB) SearchIndex(indexName string, filter x.Filter, desc bool) mo.Result[[]string] {
	if indexName == "" {
		return mo.Err[[]string](errors.New("index name is required"))
	}
	layer, ok := xdb.indexLayers[indexName]
	if !ok {
		return mo.Err[[]string](fmt.Errorf("index not found: %s", indexName))
	}

	var results []string
	err := xdb.store(layer).View(func(tx *buntdb.Tx) error {
		iter := tx.Ascend
		if desc {
			iter = tx.Descend
		}
		return iter(indexName, func(key, value string) bool {
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

// SearchKey matches keys by key glob pattern, applies the optional filter to
// each JSON value, and returns the matched JSON documents in key order.
//
// keyPattern uses key glob matching such as "*" and "?".
//
// RESP equivalent:
//
//	SEARCHKEY <key_pattern> <json_filter> [ASC|DESC]
//
// Example:
//
//	res := db.SearchKey(
//		"user:engineering:*",
//		x.Eq("region", "us"),
//		true,
//	)
//	users := res.MustGet()
func (xdb *DB) SearchKey(keyPattern string, filter x.Filter, desc bool) mo.Result[[]string] {
	if keyPattern == "" {
		return mo.Err[[]string](errors.New("pattern is required"))
	}
	var entries []kvPair
	for _, layer := range layersForPattern(keyPattern) {
		err := xdb.store(layer).View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(keyPattern, func(key, value string) bool {
				if filter == nil || filter.Eval(value) {
					entries = append(entries, kvPair{key: key, value: value})
				}
				return true
			})
		})
		if err != nil {
			return mo.Err[[]string](err)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		if desc {
			return entries[i].key > entries[j].key
		}
		return entries[i].key < entries[j].key
	})

	results := make([]string, 0, len(entries))
	for _, entry := range entries {
		results = append(results, entry.value)
	}
	return mo.Ok(results)
}

// Set stores one string value under key.
//
// Routing rules:
//   - keys prefixed with "_m_" are stored in the in-memory layer
//   - all other keys are stored in the persistent layer
//
// RESP equivalent:
//
//	SET <key> <value>
func (x *DB) Set(key string, value string) error {
	return x.storeForKey(key).Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

// SetWithTtl stores one string value under key with an optional TTL.
//
// A positive ttl makes the key expire automatically. A zero or negative ttl
// behaves the same as [DB.Set].
//
// RESP equivalents:
//
//	SET <key> <value> EX <seconds>
//	SETEX <key> <seconds> <value>
func (x *DB) SetWithTtl(key string, value string, ttl time.Duration) error {
	if ttl > 0 {
		opt := &buntdb.SetOptions{Expires: true, TTL: ttl}
		return x.storeForKey(key).Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, value, opt)
			return err
		})

	} else {
		return x.Set(key, value)
	}
}

// SetNX stores value only when key does not already exist.
//
// The returned boolean reports whether the write happened.
//
// RESP equivalent:
//
//	SETNX <key> <value>
func (x *DB) SetNX(key string, value string) (bool, error) {
	var set bool
	err := x.storeForKey(key).Update(func(tx *buntdb.Tx) error {
		_, err := tx.Get(key)
		if err == buntdb.ErrNotFound {
			_, _, err = tx.Set(key, value, nil)
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
// RESP equivalent:
//
//	GET <key>
func (x *DB) Get(key string) mo.Result[string] {
	var val string
	err := x.storeForKey(key).View(func(tx *buntdb.Tx) error {
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
// RESP equivalent:
//
//	DEL <key>
func (x *DB) Delete(key string) (bool, error) {
	var val string
	err := x.storeForKey(key).Update(func(tx *buntdb.Tx) error {
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

// Keys returns all keys matching keyPattern across the relevant storage layers.
//
// Routing rules:
//   - patterns starting with "_m_" only scan the in-memory layer
//   - patterns starting with any other concrete prefix only scan the persistent layer
//   - patterns starting with "*" or "?" scan both layers and merge the results
//
// keyPattern uses key glob matching such as "*" and "?".
//
// RESP equivalent:
//
//	KEYS <key_pattern>
func (x *DB) Keys(keyPattern string) mo.Result[[]string] {
	var keys []string
	seen := map[string]struct{}{}
	for _, layer := range layersForPattern(keyPattern) {
		err := x.store(layer).View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(keyPattern, func(key, value string) bool {
				keys = appendUnique(keys, seen, key)
				return true
			})
		})
		if err != nil {
			return mo.Err[[]string](err)
		}
	}
	sort.Strings(keys)
	return mo.Ok(keys)
}
