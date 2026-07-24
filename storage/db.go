package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/kcmvp/redisx/x"
)

const KeySeparator = ":"

// Reset is used for testing purposes to reset the singleton state.
func Reset() {
	// Reset is intentionally a no-op after removing singleton storage state.
}

// Open creates a new storage instance.
// Use ":memory:" for an in-memory DB, otherwise pass an explicit file path.
func Open(path string) DB {
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
	return &xdb{kv: raw}
}

type seal struct{}

// DB defines the core interface for the BuntDB-backed storage engine.
type DB interface {
	// Set writes a raw key-value pair directly to the underlying BuntDB.
	// It is generally recommended to use Save for schema-based documents.
	// [Redis Compatible: SET]
	Set(key string, value string) error

	// SetWithTtl writes a raw key-value pair with an expiration time.
	// [Redis Compatible: SETEX / SET with EX]
	SetWithTtl(key string, value string, ttl time.Duration) error

	// SetNX writes a raw key-value pair only if the key does not already exist.
	// Returns true if the key was set, false otherwise.
	// [Redis Compatible: SETNX]
	SetNX(key string, value string) (bool, error)

	// Get retrieves a JSON document by its exact key.
	// Returns a mo.Result containing the JSON string or an error if not found.
	// [Redis Compatible: GET]
	Get(key string) mo.Result[string]

	// Delete removes a key-value pair by its exact key.
	// Returns true if the key was deleted, false if it was not found.
	// [Redis Compatible: DEL]
	Delete(key string) (bool, error)

	// Keys retrieves a list of keys that match the specified glob pattern.
	// Returns a mo.Result containing a slice of matching keys.
	// [Redis Compatible: KEYS]
	Keys(pattern string) mo.Result[[]string]

	// Update modifies existing JSON documents that match the provided key pattern and filter.
	Update(pattern string, filter x.Filter, values ...JsonPair) mo.Result[[]string]

	// SearchIndex performs a query against a specific JSON attribute across all keys.
	// - indexAttr: The JSON path attribute used for sorting and filtering candidates.
	// - filter: An optional x.Filter to apply further conditions (can be nil).
	// - desc: If true, returns results in descending order.
	SearchIndex(indexAttr string, filter x.Filter, desc bool) mo.Result[[]string]

	// SearchKey performs a query by matching document keys against a glob pattern.
	// - pattern: The glob pattern to match keys (e.g., "*", "user_*").
	// - filter: An optional x.Filter to apply further conditions (can be nil).
	// - desc: If true, returns results in descending order.
	SearchKey(pattern string, filter x.Filter, desc bool) mo.Result[[]string]

	// Close shuts down the storage engine and releases resources.
	Close() error

	// mark is an unexported method that implements the sealed interface pattern.
	// It prevents external packages from implementing this interface.
	mark() seal
}

type JsonPair interface {
	Path() string
	Value() any
	mark() seal
}

type Type interface {
	~int | ~int32 | ~int64 | ~float32 | ~float64 | ~string | ~bool
}

type pair[T Type] struct {
	path string
	val  T
}

// Path implements [JsonPair].
func (v pair[T]) Path() string {
	return v.path
}

// Value implements [JsonPair].
func (v pair[T]) Value() any {
	return v.val
}

// mark implements [JsonPair].
func (v pair[T]) mark() seal { return seal{} }

// Pair creates a new JsonPair. The value will be automatically formatted
// into a valid JSON representation based on its type.
func Pair[T Type](path string, value T) JsonPair {
	return pair[T]{path: path, val: value}
}

type xdb struct {
	kv *buntdb.DB
}

func (x *xdb) mark() seal {
	return seal{}
}

func (x *xdb) Close() error {
	if x.kv == nil {
		return nil
	}
	return x.kv.Close()
}

func (x *xdb) Update(pattern string, filter x.Filter, values ...JsonPair) mo.Result[[]string] {
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

type searchIndexEntry struct {
	key   string
	value string
	attr  gjson.Result
}

func attrTypeRank(v gjson.Result) int {
	switch v.Type {
	case gjson.Number:
		return 0
	case gjson.String:
		return 1
	case gjson.False, gjson.True:
		return 2
	default:
		return 3
	}
}

func attrLess(a, b gjson.Result) bool {
	if rankA, rankB := attrTypeRank(a), attrTypeRank(b); rankA != rankB {
		return rankA < rankB
	}
	switch a.Type {
	case gjson.Number:
		return a.Float() < b.Float()
	case gjson.String:
		return a.String() < b.String()
	case gjson.False, gjson.True:
		return !a.Bool() && b.Bool()
	default:
		return a.Raw < b.Raw
	}
}

func (xdb *xdb) SearchIndex(indexAttr string, filter x.Filter, desc bool) mo.Result[[]string] {
	if indexAttr == "" {
		return mo.Err[[]string](errors.New("index attribute is required"))
	}

	var entries []searchIndexEntry
	err := xdb.kv.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys("*", func(key, value string) bool {
			if !gjson.Valid(value) {
				return true
			}
			attr := gjson.Get(value, indexAttr)
			if !attr.Exists() {
				return true
			}
			if filter == nil || filter.Eval(value) {
				entries = append(entries, searchIndexEntry{key: key, value: value, attr: attr})
			}
			return true
		})
	})

	if err != nil {
		return mo.Err[[]string](err)
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].attr.Raw == entries[j].attr.Raw {
			if desc {
				return entries[i].key > entries[j].key
			}
			return entries[i].key < entries[j].key
		}
		if desc {
			return attrLess(entries[j].attr, entries[i].attr)
		}
		return attrLess(entries[i].attr, entries[j].attr)
	})

	results := make([]string, 0, len(entries))
	for _, entry := range entries {
		results = append(results, entry.value)
	}
	return mo.Ok(results)
}

func (xdb *xdb) SearchKey(pattern string, filter x.Filter, desc bool) mo.Result[[]string] {
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

func (x *xdb) Set(key string, value string) error {
	return x.kv.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

func (x *xdb) SetWithTtl(key string, value string, ttl time.Duration) error {
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

func (x *xdb) SetNX(key string, value string) (bool, error) {
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

func (x *xdb) Get(key string) mo.Result[string] {
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

func (x *xdb) Delete(key string) (bool, error) {
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

func (x *xdb) Keys(pattern string) mo.Result[[]string] {
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
