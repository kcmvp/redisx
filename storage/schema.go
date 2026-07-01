package storage

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/buntdb"
)

var (
	db   DB
	once sync.Once
	_    DB = (*xdb)(nil)
	_    DB = (*mem)(nil)
)

// Schema represents the configuration required to create a JSON index in the backend storage (BuntDB).
// By defining an index, you enable the respx server to perform highly efficient lookups on specific JSON fields
// across your stored values using the QueryX command.
type Schema interface {
	// Name returns the unique identifier for the index.
	// This name is used by the client when executing a QueryX command to specify which index to search against.
	Name() string

	// Pattern returns the pattern that determines which keys should be included in this index.
	// For example, returning "user:*" will only index JSON values whose keys start with "user:".
	// Returning "*" will index all keys in the database.
	Pattern() string

	// Path returns the specific path within the JSON document that should be indexed.
	// For example, returning "age" or "address.city" will index those specific fields from the JSON values.
	Path() string

	// Ttl define the TTL of the data in this schema
	Ttl() time.Duration
}

func Start(schemas ...Schema) {
	once.Do(func() {
		if len(schemas) == 0 {
			db = mem{raw: map[string]string{}}
		} else {
			var err error
			homeDir, err := os.UserHomeDir()
			if err != nil {
				slog.Error("failed to resolve user home directory", "error", err)
				os.Exit(1)
				return
			}

			dataDir := filepath.Join(homeDir, ".respx")
			if err = os.MkdirAll(dataDir, 0o755); err != nil {
				slog.Error("failed to create data directory", "path", dataDir, "error", err)
				os.Exit(1)
				return
			}

			dbPath := filepath.Join(dataDir, "data.db")
			raw, err := buntdb.Open(dbPath)
			if err != nil {
				slog.Error("failed to open buntdb", "path", dbPath, "error", err)
				os.Exit(1)
				return
			}

			db = &xdb{raw: raw}

			if duplicated := lo.FindDuplicatesBy(schemas, func(idx Schema) string {
				return idx.Name()
			}); len(duplicated) > 0 {
				slog.Error("duplicate schema provided", "schema", duplicated[0])
				os.Exit(1)
				return
			}
			for _, schema := range schemas {
				_ = db.Create(schema)
			}
		}
	})
}

func Get() DB {
	return db
}

type DB interface {
	Set(key string, value string) error
	SetWithTtl(key string, value string, ttl time.Duration) error
	Get(key string) (string, error)
	Delete(key string) (bool, error)

	Create(schema Schema) error
	Raw() any
}

type mem struct {
	raw map[string]string
}

func (d mem) Raw() any {
	return d.raw
}

func (d mem) Create(schema Schema) error {
	return fmt.Errorf("not supported on mem")
}

func (d mem) Set(key string, value string) error {
	//TODO implement me
	panic("implement me")
}

func (d mem) SetWithTtl(key string, value string, ttl time.Duration) error {
	return d.Set(key, value)
}

func (d mem) Get(key string) (string, error) {
	//TODO implement me
	panic("implement me")
}
func (d mem) Delete(key string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

type xdb struct {
	raw *buntdb.DB
}

func (x *xdb) Raw() any {
	return x.raw
}

func (x *xdb) Create(schema Schema) error {
	err := x.raw.CreateIndex(schema.Name(), schema.Pattern(), lo.If(len(schema.Path()) > 0, buntdb.IndexJSON(schema.Path())).Else(buntdb.IndexString))
	if err != nil {
		slog.Error("failed to create schema", "schema", schema.Name(), "error", err)
	}
	return err
}

func (x *xdb) Set(key string, value string) error {
	return x.raw.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

func (x *xdb) SetWithTtl(key string, value string, ttl time.Duration) error {
	if ttl > 0 {
		opt := &buntdb.SetOptions{Expires: true, TTL: ttl}
		return x.raw.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(key, value, opt)
			return err
		})

	} else {
		return x.Set(key, value)
	}
}

func (x *xdb) Get(key string) (string, error) {
	var val string
	var err error
	_ = x.raw.View(func(tx *buntdb.Tx) error {
		val, err = tx.Get(key)
		return err
	})
	return val, err
}
func (x *xdb) Delete(key string) (bool, error) {
	var val string
	var err error
	_ = x.raw.Update(func(tx *buntdb.Tx) error {
		val, err = tx.Delete(key)
		return nil
	})
	return len(val) > 0, err
}
