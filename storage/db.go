package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"

	"github.com/kcmvp/respx/x"
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
	dbInstance DB
	dbOnce     sync.Once
	dbMu       sync.RWMutex
)

// Reset is used for testing purposes to reset the singleton state.
func Reset() {
	dbMu.Lock()
	defer dbMu.Unlock()
	if dbInstance != nil {
		_ = dbInstance.Close()
		dbInstance = nil
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
		return &xdb{raw}
	}

	dbOnce.Do(func() {
		var err error
		var raw *buntdb.DB

		home, err := os.UserHomeDir()
		if err != nil {
			home = os.TempDir()
		}
		path := fmt.Sprintf("%s/.respx/data.db", home)
		dir := filepath.Dir(path)
		if err = os.MkdirAll(dir, 0o755); err != nil {
			slog.Error("failed to create data directory", "path", dir, "error", err)
			return
		}

		raw, err = buntdb.Open(path)
		if err != nil {
			slog.Error("failed to open buntdb", "path", path, "error", err)
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

		dbMu.Lock()
		dbInstance = &xdb{raw}
		dbMu.Unlock()
	})

	dbMu.RLock()
	defer dbMu.RUnlock()
	return dbInstance
}

type seal struct{}

type DB interface {
	Set(key string, value string) error
	SetWithTtl(key string, value string, ttl time.Duration) error
	SetNX(key string, value string) (bool, error)
	Get(key string) mo.Result[string]
	Delete(key string) (bool, error)
	Keys(pattern string) mo.Result[[]string]
	Save(schema Schema, jsonValue string) mo.Result[string]
	// ByIndex queries the database using a specific schema index and filter.
	ByIndex(schemaName string, indexAttr string, filter x.Filter, desc bool) mo.Result[[]string]

	// ByKey queries the database using a key pattern and filter.
	ByKey(schemaName string, pattern string, filter x.Filter, desc bool) mo.Result[[]string]
	Close() error
	mark() seal
}

type xdb struct {
	*buntdb.DB
}

func (x *xdb) mark() seal {
	return seal{}
}

func (x *xdb) Close() error {
	return x.DB.Close()
}

func (x *xdb) Save(schema Schema, jsonValue string) mo.Result[string] {
	if !gjson.Valid(jsonValue) {
		return mo.Err[string](errors.New("invalid json"))
	}

	key, err := lo.ReduceErr(schema.Prefixes(), func(agg string, prefix string, _ int) (string, error) {
		rs := gjson.Get(jsonValue, prefix)
		if !rs.Exists() {
			return "", fmt.Errorf("json path %s does not exist in json %s", prefix, schema.Name())
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

func (xdb *xdb) ByIndex(schemaName string, indexAttr string, filter x.Filter, desc bool) mo.Result[[]string] {
	var results []string

	// Reconstruct the physical index name
	valuePath := strings.ToLower(strings.ReplaceAll(indexAttr, ".", "_"))
	indexName := fmt.Sprintf("%s_%s", strings.ToLower(schemaName), valuePath)

	err := xdb.View(func(tx *buntdb.Tx) error {
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
		return mo.Err[[]string](fmt.Errorf("index %s not found for schema %s", indexAttr, schemaName))
	}
	if err != nil {
		return mo.Err[[]string](err)
	}

	return mo.Ok(results)
}

func (xdb *xdb) ByKey(schemaName string, pattern string, filter x.Filter, desc bool) mo.Result[[]string] {
	var results []string

	keyPattern := fmt.Sprintf("%s%s%s", schemaName, KeySeparator, pattern)

	err := xdb.View(func(tx *buntdb.Tx) error {
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
	return x.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

func (x *xdb) SetWithTtl(key string, value string, ttl time.Duration) error {
	if ttl > 0 {
		opt := &buntdb.SetOptions{Expires: true, TTL: ttl}
		return x.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, value, opt)
			return err
		})

	} else {
		return x.Set(key, value)
	}
}

func (x *xdb) SetNX(key string, value string) (bool, error) {
	var set bool
	err := x.Update(func(tx *buntdb.Tx) error {
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
	err := x.View(func(tx *buntdb.Tx) error {
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
	err := x.Update(func(tx *buntdb.Tx) error {
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
	err := x.View(func(tx *buntdb.Tx) error {
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
