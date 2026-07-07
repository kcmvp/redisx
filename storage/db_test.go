package storage

import (
	"os"
	"testing"
	"time"

	"github.com/kcmvp/respx/x"
	"github.com/stretchr/testify/assert"
)

func setupTestDB(t *testing.T, schemas ...Schema) DB {
	Reset()
	db := Open(false, schemas...)
	assert.NotNil(t, db)
	t.Cleanup(func() {
		_ = db.Close()
		Reset()
	})
	return db
}

func TestDB_Lifecycle(t *testing.T) {
	t.Run("In-Memory DB", func(t *testing.T) {
		Reset()
		db := Open(false)
		assert.NotNil(t, db)
		err := db.Set("key1", "val1")
		assert.NoError(t, err)

		res := db.Get("key1")
		assert.True(t, res.IsOk())
		assert.Equal(t, "val1", res.MustGet())

		_ = db.Close()
	})

	t.Run("Duplicate Schemas rejected", func(t *testing.T) {
		Reset()
		s1 := JsonSchema("user", 0)
		s2 := JsonSchema("user", 0)
		db := Open(false, s1, s2)
		assert.Nil(t, db)
	})

	t.Run("Persistent DB singleton", func(t *testing.T) {
		Reset()
		// Use a temp home dir to avoid polluting actual user dir
		origHome := os.Getenv("HOME")
		defer func() { _ = os.Setenv("HOME", origHome) }()

		tempDir := t.TempDir()
		_ = os.Setenv("HOME", tempDir)

		db1 := Open(true)
		assert.NotNil(t, db1)

		db2 := Open(true)
		assert.Equal(t, db1, db2, "Should return singleton instance")
		
		_ = db1.Close()
	})
	
	t.Run("Persistent DB singleton duplicate schemas", func(t *testing.T) {
		Reset()
		origHome := os.Getenv("HOME")
		defer func() { _ = os.Setenv("HOME", origHome) }()

		tempDir := t.TempDir()
		_ = os.Setenv("HOME", tempDir)
		
		s1 := JsonSchema("user", 0)
		s2 := JsonSchema("user", 0)
		
		db := Open(true, s1, s2)
		assert.Nil(t, db)
	})
	
	t.Run("Persistent DB create dir error", func(t *testing.T) {
		Reset()
		origHome := os.Getenv("HOME")
		defer func() { _ = os.Setenv("HOME", origHome) }()

		// Create a file where a directory is expected, so MkdirAll fails
		tempDir := t.TempDir()
		fileHome := tempDir + "/fakehome"
		f, _ := os.Create(fileHome)
		_ = f.Close()
		_ = os.Setenv("HOME", fileHome)
		
		db := Open(true)
		assert.Nil(t, db)
	})
}

func TestDB_CRUD(t *testing.T) {
	db := setupTestDB(t)

	t.Run("Set and Get", func(t *testing.T) {
		tests := []struct {
			name    string
			key     string
			val     string
			wantVal string
			wantErr bool
		}{
			{"Basic set", "k1", "v1", "v1", false},
			{"Update existing", "k1", "v2", "v2", false},
			{"Get non-existent", "missing", "", "", true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if !tt.wantErr && tt.wantVal != "" {
					err := db.Set(tt.key, tt.val)
					assert.NoError(t, err)
				}

				res := db.Get(tt.key)
				if tt.wantErr {
					assert.True(t, res.IsError())
				} else {
					assert.True(t, res.IsOk())
					assert.Equal(t, tt.wantVal, res.MustGet())
				}
			})
		}
	})

	t.Run("SetNX", func(t *testing.T) {
		ok, err := db.SetNX("nx_key", "val1")
		assert.NoError(t, err)
		assert.True(t, ok)

		ok, err = db.SetNX("nx_key", "val2")
		assert.NoError(t, err)
		assert.False(t, ok, "Should not set existing key")

		res := db.Get("nx_key")
		assert.Equal(t, "val1", res.MustGet())
	})

	t.Run("Delete", func(t *testing.T) {
		_ = db.Set("del_key", "val")

		ok, err := db.Delete("del_key")
		assert.NoError(t, err)
		assert.True(t, ok)

		ok, err = db.Delete("del_key")
		assert.NoError(t, err)
		assert.False(t, ok, "Deleting non-existent key returns false")
	})

	t.Run("Keys", func(t *testing.T) {
		_ = db.Set("prefix:a", "1")
		_ = db.Set("prefix:b", "2")
		_ = db.Set("other:c", "3")

		res := db.Keys("prefix:*")
		assert.True(t, res.IsOk())
		keys := res.MustGet()
		assert.ElementsMatch(t, []string{"prefix:a", "prefix:b"}, keys)
	})

	t.Run("SetWithTtl", func(t *testing.T) {
		err := db.SetWithTtl("ttl_key", "val", 10*time.Millisecond)
		assert.NoError(t, err)

		res := db.Get("ttl_key")
		assert.True(t, res.IsOk())

		time.Sleep(20 * time.Millisecond)

		res = db.Get("ttl_key")
		assert.True(t, res.IsError(), "Key should be expired")
	})
}

func TestDB_Save(t *testing.T) {
	schema := JsonSchema("user", 0).PrefixAttr("id", "role")
	db := setupTestDB(t, schema)

	tests := []struct {
		name      string
		json      string
		wantErr   bool
		errStr    string
		verifyKey string
	}{
		{
			name:      "Valid JSON and prefixes",
			json:      `{"id": "123", "role": "admin", "name": "alice"}`,
			wantErr:   false,
			verifyKey: "user:123:admin",
		},
		{
			name:    "Invalid JSON",
			json:    `{invalid json`,
			wantErr: true,
			errStr:  "invalid json",
		},
		{
			name:    "Missing prefix attribute",
			json:    `{"id": "123", "name": "alice"}`,
			wantErr: true,
			errStr:  "json path role does not exist in json user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := db.Save(schema, tt.json)
			if tt.wantErr {
				assert.True(t, res.IsError())
				assert.Contains(t, res.Error().Error(), tt.errStr)
			} else {
				assert.False(t, res.IsError())
				assert.Equal(t, tt.verifyKey, res.MustGet())

				// verify it was actually saved by fetching the key
				got := db.Get(res.MustGet())
				assert.False(t, got.IsError())
				assert.Equal(t, tt.json, got.MustGet())
			}
		})
	}
}

func TestDB_ByIndex(t *testing.T) {
	schema := JsonSchema("user", 0).PrefixAttr("id").Index("age")
	db := setupTestDB(t, schema)

	// Seed data
	data := []string{
		`{"id": "1", "age": 20, "name": "A"}`,
		`{"id": "2", "age": 30, "name": "B"}`,
		`{"id": "3", "age": 25, "name": "C"}`,
	}
	for _, d := range data {
		res := db.Save(schema, d)
		assert.False(t, res.IsError())
	}

	tests := []struct {
		name       string
		schemaName string
		indexAttr  string
		filter     x.Filter
		desc       bool
		wantRes    []string
		wantErr    bool
	}{
		{
			name:       "Query ascending all",
			schemaName: "user",
			indexAttr:  "age",
			filter:     nil,
			desc:       false,
			wantRes:    []string{data[0], data[2], data[1]}, // 20, 25, 30
		},
		{
			name:       "Query descending all",
			schemaName: "user",
			indexAttr:  "age",
			filter:     nil,
			desc:       true,
			wantRes:    []string{data[1], data[2], data[0]}, // 30, 25, 20
		},
		{
			name:       "Query with filter",
			schemaName: "user",
			indexAttr:  "age",
			filter:     x.Gt("age", float64(22)), // age > 22
			desc:       false,
			wantRes:    []string{data[2], data[1]}, // 25, 30
		},
		{
			name:       "Query non-existent index",
			schemaName: "user",
			indexAttr:  "unknown",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := db.ByIndex(tt.schemaName, tt.indexAttr, tt.filter, tt.desc)
			if tt.wantErr {
				assert.True(t, res.IsError())
			} else {
				assert.True(t, res.IsOk())
				assert.Equal(t, tt.wantRes, res.MustGet())
			}
		})
	}
}

func TestDB_ByKey(t *testing.T) {
	schema := JsonSchema("order", 0).PrefixAttr("region", "id")
	db := setupTestDB(t, schema)

	data := []string{
		`{"region": "us", "id": "1", "status": "active"}`,
		`{"region": "us", "id": "2", "status": "pending"}`,
		`{"region": "eu", "id": "3", "status": "active"}`,
	}
	for _, d := range data {
		res := db.Save(schema, d)
		assert.False(t, res.IsError())
	}

	tests := []struct {
		name       string
		schemaName string
		pattern    string
		filter     x.Filter
		desc       bool
		wantRes    []string
	}{
		{
			name:       "QueryKey US region ascending",
			schemaName: "order",
			pattern:    "us:*",
			filter:     nil,
			desc:       false,
			wantRes:    []string{data[0], data[1]}, // id: 1, id: 2
		},
		{
			name:       "QueryKey US region descending",
			schemaName: "order",
			pattern:    "us:*",
			filter:     nil,
			desc:       true,
			wantRes:    []string{data[1], data[0]}, // id: 2, id: 1
		},
		{
			name:       "QueryKey with filter",
			schemaName: "order",
			pattern:    "*:*", // all regions
			filter:     x.Eq("status", "active"),
			desc:       false,
			wantRes:    []string{data[2], data[0]}, // order:eu:3, order:us:1 (lexicographical order)
		},
		{
			name:       "QueryKey no match",
			schemaName: "order",
			pattern:    "asia:*",
			wantRes:    nil, // Empty slice
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := db.ByKey(tt.schemaName, tt.pattern, tt.filter, tt.desc)
			assert.True(t, res.IsOk())

			got := res.MustGet()
			if len(tt.wantRes) == 0 {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tt.wantRes, got)
			}
		})
	}
}
