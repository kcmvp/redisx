package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	_ "modernc.org/sqlite"

	"github.com/kcmvp/indx/x"
)

const KeySeparator = ":"

type JsonIndex string

// Schema represents the configuration required to create a JSON index in the backend storage (BuntDB).
// By defining an index, you enable the respx server to perform highly efficient lookups on specific JSON fields
// across your stored values using the BYINDEX/BYKEY commands.
type Schema interface {
	// Name returns the unique identifier for the index.
	// This name is used by the client when executing a BYINDEX/BYKEY command to specify which schema to search against.
	// This name is also used as key prefix, eg if Name() returns "user" the key prefix will be "user:"
	Name() string

	// Prefixes returns the json paths for the key prefix for each entry in this schema.
	Prefixes() []string

	Indexes() []JsonIndex

	// Ttl define the TTL of the data in this schema, if Ttl() returns 0 the data will be stored forever, otherwise it will be stored for the duration of Ttl()
	Ttl() time.Duration
}

// SchemaBuilder is the interface for fluently building a Schema.
type SchemaBuilder interface {
	Schema
	PrefixAttr(attrs ...string) SchemaBuilder
	Index(attr string) SchemaBuilder
}

type defaultSchema lo.Tuple4[string, []string, []JsonIndex, time.Duration]

func (s *defaultSchema) Name() string         { return s.A }
func (s *defaultSchema) Prefixes() []string   { return s.B }
func (s *defaultSchema) Indexes() []JsonIndex { return s.C }
func (s *defaultSchema) Ttl() time.Duration   { return s.D }

func (s *defaultSchema) PrefixAttr(attrs ...string) SchemaBuilder {
	s.B = append(s.B, attrs...)
	return s
}

func (s *defaultSchema) Index(attr string) SchemaBuilder {
	s.C = append(s.C, JsonIndex(attr))
	return s
}

func JsonSchema(name string, ttl time.Duration) SchemaBuilder {
	s := defaultSchema(lo.T4(name, []string{}, []JsonIndex{}, ttl))
	return &s
}

var (
	dbInstance atomic.Pointer[xdb]
	dbOnce     sync.Once
)

// Reset is used for testing purposes to reset the singleton state.
func Reset() {
	if old := dbInstance.Swap(nil); old != nil {
		_ = old.Close()
	}
	dbOnce = sync.Once{}
}

func createIndexes(raw *buntdb.DB, schema Schema) error {
	var err error
	lo.ForEachWhile(schema.Indexes(), func(index JsonIndex, _ int) bool {
		// The key pattern for the index must be restricted to this schema's prefix
		// e.g., if schema.Name() is "user", pattern should be "user:*" (using KeySeparator)
		keyPattern := fmt.Sprintf("%s%s*", schema.Name(), KeySeparator)

		valuePath := strings.ToLower(strings.ReplaceAll(string(index), ".", "_"))
		indexName := fmt.Sprintf("%s_%s", strings.ToLower(schema.Name()), valuePath)

		err = raw.CreateIndex(indexName, keyPattern, buntdb.IndexJSON(string(index)))
		return err == nil
	})
	return err
}

// If persistent is false, it creates a new in-memory database instance on every call.
// Otherwise, it initializes and returns a persistent singleton database at the default path.
func Open(persistent bool, schemas ...Schema) DB {
	if !persistent {
		raw, err := buntdb.Open(":memory:")
		if err != nil {
			slog.Error("failed to open in-memory buntdb", "error", err)
			return nil
		}

		if len(schemas) > 0 {
			if duplicated := lo.FindDuplicatesBy(schemas, func(idx Schema) string {
				return idx.Name()
			}); len(duplicated) > 0 {
				slog.Error("duplicate schema provided", "schema", duplicated[0])
				_ = raw.Close()
				return nil
			}
			for _, schema := range schemas {
				if err := createIndexes(raw, schema); err != nil {
					_ = raw.Close()
					return nil
				}
			}
		}

		return &xdb{kv: raw, persistent: false}
	}

	dbOnce.Do(func() {
		var err error
		var raw *buntdb.DB

		home, err := os.UserHomeDir()
		if err != nil {
			home = os.TempDir()
		}
		hotDir := filepath.Join(home, ".sd", "hot")
		coldDir := filepath.Join(home, ".sd", "cold")

		if err = os.MkdirAll(hotDir, 0o755); err != nil {
			slog.Error("failed to create hot data directory", "path", hotDir, "error", err)
			return
		}
		if err = os.MkdirAll(coldDir, 0o755); err != nil {
			slog.Error("failed to create cold data directory", "path", coldDir, "error", err)
			return
		}

		hotPath := filepath.Join(hotDir, "kv.db")
		raw, err = buntdb.Open(hotPath)
		if err != nil {
			slog.Error("failed to open buntdb", "path", hotPath, "error", err)
			return
		}

		if len(schemas) > 0 {
			if duplicated := lo.FindDuplicatesBy(schemas, func(idx Schema) string {
				return idx.Name()
			}); len(duplicated) > 0 {
				slog.Error("duplicate schema provided", "schema", duplicated[0])
				_ = raw.Close()
				return
			}
			for _, schema := range schemas {
				if err := createIndexes(raw, schema); err != nil {
					_ = raw.Close()
					return
				}
			}
		}

		coldPath := filepath.Join(coldDir, "sql.db")
		sqlDB, err := sql.Open("sqlite", coldPath)
		if err != nil {
			slog.Error("failed to open sqlite db", "path", coldPath, "error", err)
			_ = raw.Close()
			return
		}

		dbInstance.Store(&xdb{kv: raw, sqlDB: sqlDB, persistent: true})
	})

	if instance := dbInstance.Load(); instance != nil {
		return instance
	}
	return nil
}

type seal struct{}

// DB defines the core interface for the document-oriented storage engine.
// It supports high-performance hot data operations (via BuntDB) and cold data
// queries (via SQLite) when running in persistent mode.
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

	// Save persists a JSON document according to the provided schema.
	// The schema defines how the document's key is generated and which fields are indexed.
	// Returns a mo.Result containing the generated key on success.
	Save(schema Schema, jsonValue string) mo.Result[string]

	// Update modifies existing documents that match the provided filter within a schema.
	// It applies the provided ValuePairs (key-value updates) to the matching documents.
	Update(schema string, filter x.Filter, values ...JsonPair) mo.Result[[]string]

	// SearchIndex performs a query against a specific JSON index defined in the schema.
	// - schemaName: The logical namespace of the data.
	// - indexAttr: The JSON path attribute that was indexed.
	// - filter: An optional x.Filter to apply further conditions (can be nil).
	// - desc: If true, returns results in descending order.
	SearchIndex(schemaName string, indexAttr string, filter x.Filter, desc bool) mo.Result[[]string]

	// SearchKey performs a query by matching document keys against a glob pattern.
	// - schemaName: The logical namespace of the data.
	// - pattern: The glob pattern to match keys (e.g., "*", "user_*").
	// - filter: An optional x.Filter to apply further conditions (can be nil).
	// - desc: If true, returns results in descending order.
	SearchKey(schemaName string, pattern string, filter x.Filter, desc bool) mo.Result[[]string]

	// Query executes a raw SQL query against the cold data storage (SQLite).
	// This method is only supported when the DB is opened in persistent mode.
	// Returns a slice of strings (typically JSON rows) or an error.
	Query(query string, args ...any) ([]string, error)

	// Close shuts down both the hot and cold storage engines and releases resources.
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
	kv         *buntdb.DB
	sqlDB      *sql.DB
	persistent bool
}

func (x *xdb) mark() seal {
	return seal{}
}

func (x *xdb) Close() error {
	var errs []error
	if x.kv != nil {
		if err := x.kv.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if x.sqlDB != nil {
		if err := x.sqlDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (x *xdb) Query(query string, args ...any) ([]string, error) {
	if !x.persistent {
		return nil, errors.New("query is not supported in non-persistent mode")
	}
	if x.sqlDB == nil {
		return nil, errors.New("sql db is not initialized")
	}

	rows, err := x.sqlDB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var results []string
	// we expect the query to return json string or a single text column
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		results = append(results, val)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (x *xdb) Save(schema Schema, jsonValue string) mo.Result[string] {
	if !gjson.Valid(jsonValue) {
		return mo.Err[string](errors.New("invalid json"))
	}

	parsed := gjson.Parse(jsonValue)
	if !parsed.IsObject() {
		return mo.Err[string](errors.New("root must be a json object"))
	}

	var nestedErr error
	parsed.ForEach(func(key, value gjson.Result) bool {
		if value.IsObject() || value.IsArray() {
			nestedErr = errors.New("nested json is not supported")
			return false
		}
		return true
	})

	if nestedErr != nil {
		return mo.Err[string](nestedErr)
	}

	key, err := lo.ReduceErr(schema.Prefixes(), func(agg string, prefix string, _ int) (string, error) {
		rs := gjson.Get(jsonValue, prefix)
		if !rs.Exists() {
			err := fmt.Errorf("json path %s does not exist in json %s", prefix, schema.Name())
			slog.Error("failed to save document", "error", err)
			return "", err
		}
		return fmt.Sprintf("%s%s%s", agg, KeySeparator, rs.String()), nil
	}, schema.Name())

	if err != nil {
		return mo.Err[string](err)
	}

	if err := x.SetWithTtl(key, jsonValue, schema.Ttl()); err != nil {
		return mo.Err[string](err)
	}
	return mo.Ok(key)
}

func (x *xdb) Update(schema string, filter x.Filter, values ...JsonPair) mo.Result[[]string] {
	var updatedKeys []string
	keyPattern := fmt.Sprintf("%s%s*", schema, KeySeparator)

	err := x.kv.Update(func(tx *buntdb.Tx) error {
		// Collect matching keys first to avoid modifying while iterating
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

		// Now process the updates for all matched keys
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

			// If the value changed, save it back
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

func (xdb *xdb) SearchIndex(schemaName string, indexAttr string, filter x.Filter, desc bool) mo.Result[[]string] {
	var results []string

	// Reconstruct the physical index name
	valuePath := strings.ToLower(strings.ReplaceAll(indexAttr, ".", "_"))
	indexName := fmt.Sprintf("%s_%s", strings.ToLower(schemaName), valuePath)

	err := xdb.kv.View(func(tx *buntdb.Tx) error {
		iter := lo.If(desc, tx.Descend).Else(tx.Ascend)
		err := iter(indexName, func(key, value string) bool {
			// If no filter is provided, or the filter passes on the full JSON value, add it
			if filter == nil || filter.Eval(value) {
				results = append(results, value)
			}
			return true
		})

		return err
	})

	if err == buntdb.ErrNotFound {
		err := fmt.Errorf("index %s not found for schema %s", indexAttr, schemaName)
		slog.Error("failed to search index", "error", err)
		return mo.Err[[]string](err)
	}
	if err != nil {
		return mo.Err[[]string](err)
	}

	return mo.Ok(results)
}

func (xdb *xdb) SearchKey(schemaName string, pattern string, filter x.Filter, desc bool) mo.Result[[]string] {
	var results []string

	keyPattern := fmt.Sprintf("%s%s%s", schemaName, KeySeparator, pattern)

	err := xdb.kv.View(func(tx *buntdb.Tx) error {
		iter := lo.If(desc, tx.DescendKeys).Else(tx.AscendKeys)
		return iter(keyPattern, func(key, value string) bool {
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
