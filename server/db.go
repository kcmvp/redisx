package server

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/kcmvp/redisx/x"
)

const keySeparator = ":"

// IndexDef describes one startup index declaration for SEARCHINDEX.
type IndexDef struct {
	name    string
	pattern string
	path    string
}

// Index declares one JSON index to be created during server startup.
func Index(name, keyPattern, jsonPath string) IndexDef {
	return IndexDef{name: name, pattern: keyPattern, path: jsonPath}
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

// resetStorage is used for testing purposes to reset the storage state.
func resetStorage() {
	// Reset is intentionally a no-op after removing singleton storage state.
}

// openDB creates a new storage instance.
// Use ":memory:" for an in-memory DB, otherwise pass an explicit file path.
func openDB(path string) *DB {
	if path == "" {
		path = ":memory:"
	}
	if path != ":memory:" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			slog.Error("failed to create data directory", "path", dir, "error", err)
			return nil
		}
	}

	raw, err := buntdb.Open(path)
	if err != nil {
		slog.Error("failed to open buntdb", "path", path, "error", err)
		return nil
	}
	return &DB{kv: raw}
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
	kv *buntdb.DB
}

// Raw exposes the underlying BuntDB handle for advanced use cases.
//
// This is useful when your application needs native BuntDB transactions,
// indexes, or iteration APIs that are intentionally not re-exposed by redisx.
//
// Example:
//
//	raw := db.Raw()
//	err := raw.Update(func(tx *buntdb.Tx) error {
//		_, _, err := tx.Set("native:key", "value", nil)
//		return err
//	})
func (x *DB) Raw() *buntdb.DB {
	return x.kv
}

func (x *DB) Close() error {
	if x.kv == nil {
		return nil
	}
	return x.kv.Close()
}

// Update applies JSON path updates to all values matched by a key pattern and
// an optional filter.
//
// The target values must be valid JSON documents. Each x.Mutation is applied with
// sjson semantics, so the path may address top-level or nested fields.
//
// RESP equivalent:
//
//	UPDATE <pattern> <json_filter> <update_json>
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
func (x *DB) Update(pattern string, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if pattern == "" {
		return mo.Err[[]string](errors.New("pattern is required"))
	}
	var updatedKeys []string

	err := x.kv.Update(func(tx *buntdb.Tx) error {
		var matchedKeys []string
		err := tx.AscendKeys(pattern, func(key, value string) bool {
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
			updatedKeys = append(updatedKeys, key)
		}

		return nil
	})

	if err != nil {
		return mo.Err[[]string](err)
	}

	return mo.Ok(updatedKeys)
}

func (x *DB) registerIndexes(defs ...IndexDef) error {
	for _, def := range defs {
		if def.name == "" {
			return errors.New("index name is required")
		}
		if def.pattern == "" {
			return errors.New("index key pattern is required")
		}
		if def.path == "" {
			return errors.New("index json path is required")
		}
		if err := x.kv.CreateIndex(def.name, def.pattern, buntdb.IndexJSON(def.path)); err != nil {
			return err
		}
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

	var results []string
	err := xdb.kv.View(func(tx *buntdb.Tx) error {
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

// SearchKey matches keys by glob pattern, applies the optional filter to each
// JSON value, and returns the matched JSON documents in key order.
//
// The pattern uses BuntDB key glob semantics such as "*" and "?".
//
// RESP equivalent:
//
//	SEARCHKEY <pattern> <json_filter> [ASC|DESC]
//
// Example:
//
//	res := db.SearchKey(
//		"user:engineering:*",
//		x.Eq("region", "us"),
//		true,
//	)
//	users := res.MustGet()
func (xdb *DB) SearchKey(pattern string, filter x.Filter, desc bool) mo.Result[[]string] {
	if pattern == "" {
		return mo.Err[[]string](errors.New("pattern is required"))
	}
	var results []string

	err := xdb.kv.View(func(tx *buntdb.Tx) error {
		if desc {
			return tx.DescendKeys(pattern, func(key, value string) bool {
				if filter == nil || filter.Eval(value) {
					results = append(results, value)
				}
				return true
			})
		}
		return tx.AscendKeys(pattern, func(key, value string) bool {
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

func (x *DB) Set(key string, value string) error {
	return x.kv.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

func (x *DB) SetWithTtl(key string, value string, ttl time.Duration) error {
	if ttl > 0 {
		opt := &buntdb.SetOptions{Expires: true, TTL: ttl}
		return x.kv.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, value, opt)
			return err
		})

	} else {
		return x.Set(key, value)
	}
}

func (x *DB) SetNX(key string, value string) (bool, error) {
	var set bool
	err := x.kv.Update(func(tx *buntdb.Tx) error {
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

func (x *DB) Get(key string) mo.Result[string] {
	var val string
	err := x.kv.View(func(tx *buntdb.Tx) error {
		var innerErr error
		val, innerErr = tx.Get(key)
		return innerErr
	})

	if err != nil {
		return mo.Err[string](err)
	}
	return mo.Ok(val)
}

func (x *DB) Delete(key string) (bool, error) {
	var val string
	err := x.kv.Update(func(tx *buntdb.Tx) error {
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

func (x *DB) Keys(pattern string) mo.Result[[]string] {
	var keys []string
	err := x.kv.View(func(tx *buntdb.Tx) error {
		// Keys uses AscendKeys which supports simple glob matching (*, ?)
		// In buntdb, AscendKeys iterates over keys matching the pattern
		return tx.AscendKeys(pattern, func(key, value string) bool {
			keys = append(keys, key)
			return true
		})
	})

	if err != nil {
		return mo.Err[[]string](err)
	}
	return mo.Ok(keys)
}
