package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/x"
	"github.com/kcmvp/redisx/x/contract"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
)

type testUserDoc string

func (testUserDoc) Namespace() string  { return "user" }
func (testUserDoc) Mem() bool          { return false }
func (testUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u testUserDoc) RawJSON() string  { return string(u) }
func (testUserDoc) TTL() time.Duration { return time.Hour }

type testMemUserDoc string

func (testMemUserDoc) Namespace() string  { return "user" }
func (testMemUserDoc) Mem() bool          { return true }
func (testMemUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u testMemUserDoc) RawJSON() string  { return string(u) }
func (testMemUserDoc) TTL() time.Duration { return 0 }

type expiringUserDoc string

func (expiringUserDoc) Namespace() string  { return "expuser" }
func (expiringUserDoc) Mem() bool          { return false }
func (expiringUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u expiringUserDoc) RawJSON() string  { return string(u) }
func (expiringUserDoc) TTL() time.Duration { return 40 * time.Millisecond }

type DBSuite struct {
	suite.Suite
	db *DB
}

func (suite *DBSuite) SetupTest() {
	suite.db = openDB(testutil.DBPath(suite.T()))
	suite.NotNil(suite.db)
}

func (suite *DBSuite) TearDownTest() {
	if suite.db != nil {
		_ = suite.db.Close()
	}
}

func (suite *DBSuite) TestLifecycle() {
	suite.Run("Empty path is invalid", func() {
		db := openDB("")
		suite.Nil(db)
	})

	suite.Run("In-Memory DB", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		err := db.Set("key1", "val1")
		suite.NoError(err)

		res := db.Get("key1")
		suite.True(res.IsOk())
		suite.Equal("val1", res.MustGet())

		_ = db.Close()
	})

	suite.Run("File DB", func() {
		path := filepath.Join(suite.T().TempDir(), "hot", "kv.db")
		db := openDB(path)
		suite.NotNil(db)
		suite.NoError(db.Set("persist:key", "persist-val"))
		suite.NoError(db.Close())

		db = openDB(path)
		suite.NotNil(db)
		res := db.Get("persist:key")
		suite.True(res.IsOk())
		suite.Equal("persist-val", res.MustGet())
		suite.NoError(db.Close())
	})

	suite.Run("Creates missing parent directory", func() {
		base := suite.T().TempDir()
		path := filepath.Join(base, "nested", "deeper", "kv.db")

		db := openDB(path)
		suite.NotNil(db)
		suite.NoError(db.Close())

		info, err := os.Stat(filepath.Dir(path))
		suite.NoError(err)
		suite.True(info.IsDir())
	})

	suite.Run("Directory path is invalid", func() {
		dir := filepath.Join(suite.T().TempDir(), "dbdir")
		suite.NoError(os.MkdirAll(dir, 0o755))

		db := openDB(dir)
		suite.Nil(db)
	})

	suite.Run(`Special ":memory:" path is invalid`, func() {
		db := openDB(":memory:")
		suite.Nil(db)
	})
}

func (suite *DBSuite) TestRawHandlesExposeBothLayers() {
	suite.NotNil(suite.db.Raw())
	suite.NotNil(suite.db.RawMem())
	suite.NotSame(suite.db.Raw(), suite.db.RawMem())

	suite.NoError(suite.db.Raw().Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set("disk:key", "disk", nil)
		return err
	}))
	suite.NoError(suite.db.RawMem().Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set("_m_mem:key", "mem", nil)
		return err
	}))

	suite.Equal("disk", suite.db.Get("disk:key").MustGet())
	suite.Equal("mem", suite.db.Get("_m_mem:key").MustGet())
}

func (suite *DBSuite) TestRegisterIndexes() {
	suite.Run("initializes nil index layer map", func() {
		suite.db.indexLayers = nil
		err := suite.db.registerIndexes(x.Idx[testUserDoc]("age", "*", "age"))
		suite.NoError(err)
		suite.Equal(storageDisk, suite.db.indexLayers[x.Idx[testUserDoc]("age", "*", "age").Name()])
	})

	suite.Run("registers memory layer index", func() {
		err := suite.db.registerIndexes(x.Idx[testMemUserDoc]("age", "*", "age"))
		suite.NoError(err)
		suite.Equal(storageMem, suite.db.indexLayers[x.Idx[testMemUserDoc]("age", "*", "age").Name()])
	})

	suite.Run("rejects empty index name", func() {
		err := suite.db.registerIndexes(x.Index{})
		suite.EqualError(err, "index name is required")
	})

	suite.Run("rejects duplicate index declaration", func() {
		idx := x.Idx[testUserDoc]("dup", "*", "age")
		suite.NoError(suite.db.registerIndexes(idx))
		err := suite.db.registerIndexes(idx)
		suite.EqualError(err, "index already declared: user_dup")
	})
}

func (suite *DBSuite) TestCRUD() {
	suite.Run("Set and Get", func() {
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
			suite.Run(tt.name, func() {
				if !tt.wantErr && tt.wantVal != "" {
					err := suite.db.Set(tt.key, tt.val)
					suite.NoError(err)
				}

				res := suite.db.Get(tt.key)
				if tt.wantErr {
					suite.True(res.IsError())
				} else {
					suite.True(res.IsOk())
					suite.Equal(tt.wantVal, res.MustGet())
				}
			})
		}
	})

	suite.Run("SetNX", func() {
		ok, err := suite.db.SetNX("nx_key", "val1")
		suite.NoError(err)
		suite.True(ok)

		ok, err = suite.db.SetNX("nx_key", "val2")
		suite.NoError(err)
		suite.False(ok, "Should not set existing key")

		res := suite.db.Get("nx_key")
		suite.Equal("val1", res.MustGet())
	})

	suite.Run("Delete", func() {
		_ = suite.db.Set("del_key", "val")

		ok, err := suite.db.Delete("del_key")
		suite.NoError(err)
		suite.True(ok)

		ok, err = suite.db.Delete("del_key")
		suite.NoError(err)
		suite.False(ok, "Deleting non-existent key returns false")
	})

	suite.Run("Keys", func() {
		_ = suite.db.Set("prefix:a", "1")
		_ = suite.db.Set("prefix:b", "2")
		_ = suite.db.Set("other:c", "3")

		res := suite.db.Keys("prefix:*")
		suite.True(res.IsOk())
		keys := res.MustGet()
		suite.ElementsMatch([]string{"prefix:a", "prefix:b"}, keys)
	})

	suite.Run("SetWithTtl", func() {
		err := suite.db.SetWithTtl("ttl_key", "val", 10*time.Millisecond)
		suite.NoError(err)

		res := suite.db.Get("ttl_key")
		suite.True(res.IsOk())

		time.Sleep(20 * time.Millisecond)

		res = suite.db.Get("ttl_key")
		suite.True(res.IsError(), "Key should be expired")
	})
}

func (suite *DBSuite) TestHybridStorageLayers() {
	path := filepath.Join(suite.T().TempDir(), "hybrid", "kv.db")
	suite.NoError(os.MkdirAll(filepath.Dir(path), 0o755))
	db := openDB(path)
	suite.NotNil(db)
	suite.NoError(db.registerIndexes(
		x.Idx[testUserDoc]("age", "*", "age"),
		x.Idx[testMemUserDoc]("age", "*", "age"),
	))

	suite.NoError(db.Set("user:1", `{"id":"1","age":20,"status":"cold"}`))
	suite.NoError(db.Set("_m_user:2", `{"id":"2","age":30,"status":"hot"}`))
	suite.NoError(db.Set("_m_user:3", `{"id":"3","age":25,"status":"hot"}`))

	keys := db.Keys("*user:*")
	suite.True(keys.IsError())
	suite.Contains(keys.Error().Error(), "cannot start with wildcard")
	suite.ElementsMatch([]string{"user:1"}, db.Keys("user:*").MustGet())
	suite.ElementsMatch([]string{"_m_user:2", "_m_user:3"}, db.Keys("_m_user:*").MustGet())

	res := db.SearchKey(x.KeysPattern("*user:*"), nil, false)
	suite.True(res.IsError())
	suite.Contains(res.Error().Error(), "cannot start with wildcard")

	updated := db.Update("*user:*", nil, x.Set("status", "active"))
	suite.True(updated.IsError())
	suite.Contains(updated.Error().Error(), "cannot start with wildcard")
	suite.Equal("cold", gjson.Get(db.Get("user:1").MustGet(), "status").String())
	suite.Equal("hot", gjson.Get(db.Get("_m_user:2").MustGet(), "status").String())

	diskRes := db.SearchIndex(x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), nil, false)
	suite.True(diskRes.IsOk())
	suite.Len(diskRes.MustGet(), 1)

	memRes := db.SearchIndex(x.Idx[testMemUserDoc]("age", "*", "age").Name(), x.KeysPattern("_m_user:*"), nil, false)
	suite.True(memRes.IsOk())
	suite.Len(memRes.MustGet(), 2)

	mismatchDisk := db.SearchIndex(x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("_m_user:*"), nil, false)
	suite.True(mismatchDisk.IsError())
	suite.Contains(mismatchDisk.Error().Error(), "different storage layer")

	mismatchMem := db.SearchIndex(x.Idx[testMemUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), nil, false)
	suite.True(mismatchMem.IsError())
	suite.Contains(mismatchMem.Error().Error(), "different storage layer")

	suite.NoError(db.Close())

	db = openDB(path)
	suite.NotNil(db)
	suite.NoError(db.registerIndexes(
		x.Idx[testUserDoc]("age", "*", "age"),
		x.Idx[testMemUserDoc]("age", "*", "age"),
	))
	defer func() { _ = db.Close() }()

	suite.True(db.Get("user:1").IsOk())
	suite.True(db.Get("_m_user:2").IsError())
}

func TestDBSuite(t *testing.T) {
	suite.Run(t, new(DBSuite))
}

func TestDBX(t *testing.T) {
	db := openDB(testutil.DBPath(t))
	require.NotNil(t, db)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.registerIndexes(x.Idx[testUserDoc]("age", "*", "age")))

	raw := `{"id":"200","name":"Test","age":30}`
	d := testUserDoc(raw)

	dbx := As[testUserDoc](db)

	err := dbx.Set(d)
	require.NoError(t, err)

	key, err := x.StorageKey(d)
	require.NoError(t, err)

	got, err := dbx.Get("200")
	require.NoError(t, err)
	require.Equal(t, d, got)

	ok, err := dbx.SetNX(d)
	require.NoError(t, err)
	require.False(t, ok)

	keysRes := dbx.Keys("*")
	require.NoError(t, keysRes.Error())
	require.Contains(t, keysRes.MustGet(), key)

	badKeysRes := dbx.Keys("user:*")
	require.Error(t, badKeysRes.Error())
	require.Contains(t, badKeysRes.Error().Error(), "document-scoped")

	searchRes := dbx.SearchKey(x.KeysPattern("*"), x.Eq("age", float64(30)), false)
	require.NoError(t, searchRes.Error())
	require.Contains(t, searchRes.MustGet(), testUserDoc(raw))

	idxRes := dbx.SearchIndex("age", x.KeysPattern("*"), x.Eq("age", float64(30)), false)
	require.NoError(t, idxRes.Error())
	require.Contains(t, idxRes.MustGet(), testUserDoc(raw))

	badIdxRes := dbx.SearchIndex("age", x.KeysPattern("user:*"), x.Eq("age", float64(30)), false)
	require.Error(t, badIdxRes.Error())
	require.Contains(t, badIdxRes.Error().Error(), "document-scoped")

	badFullIdxNameRes := dbx.SearchIndex("user_age", x.KeysPattern("*"), x.Eq("age", float64(30)), false)
	require.Error(t, badFullIdxNameRes.Error())
	require.Contains(t, badFullIdxNameRes.Error().Error(), "fully-qualified index name")

	updRes := dbx.Update("*", x.Eq("age", float64(30)), x.Set("age", 31))
	require.NoError(t, updRes.Error())
	require.Contains(t, updRes.MustGet(), key)

	badUpdRes := dbx.Update("user:*", nil, x.Set("name", "updated"))
	require.Error(t, badUpdRes.Error())
	require.Contains(t, badUpdRes.Error().Error(), "document-scoped")

	valRes := db.Get(key)
	require.NoError(t, valRes.Error())
	require.Contains(t, valRes.MustGet(), `"age":31`)

	del, err := dbx.Delete(testUserDoc(`{"id":"200"}`))
	require.NoError(t, err)
	require.True(t, del)
}

func TestDBXTypedWritesRespectDocumentTTL(t *testing.T) {
	db := openDB(testutil.DBPath(t))
	require.NotNil(t, db)
	t.Cleanup(func() { _ = db.Close() })

	dbx := As[expiringUserDoc](db)

	first := expiringUserDoc(`{"id":"1","name":"alpha"}`)
	require.NoError(t, dbx.Set(first))

	firstKey, err := x.StorageKey(first)
	require.NoError(t, err)
	requireTTLPositive(t, db, firstKey)

	second := expiringUserDoc(`{"id":"2","name":"beta"}`)
	ok, err := dbx.SetNX(second)
	require.NoError(t, err)
	require.True(t, ok)

	secondKey, err := x.StorageKey(second)
	require.NoError(t, err)
	requireTTLPositive(t, db, secondKey)

	updRes := dbx.Update("*", x.Eq("id", "1"), x.Set("name", "updated"))
	require.NoError(t, updRes.Error())
	require.Contains(t, updRes.MustGet(), firstKey)
	requireTTLPositive(t, db, firstKey)

	time.Sleep(80 * time.Millisecond)

	_, err = dbx.Get("1")
	require.Error(t, err)
	_, err = dbx.Get("2")
	require.Error(t, err)
}

func requireTTLPositive(t *testing.T, db *DB, key string) {
	t.Helper()

	var ttl time.Duration
	err := db.store(layerForKey(key)).View(func(tx *buntdb.Tx) error {
		var ttlErr error
		ttl, ttlErr = tx.TTL(key)
		return ttlErr
	})
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
}

type UpdateSuite struct {
	suite.Suite
	db *DB
}

func (suite *UpdateSuite) SetupTest() {
	suite.db = openDB(testutil.DBPath(suite.T()))
	suite.NotNil(suite.db)

	data := map[string]string{
		"user:1": `{"id": "1", "age": 20, "name": "A"}`,
		"user:2": `{"id": "2", "age": 30, "name": "B"}`,
		"user:3": `{"id": "3", "age": 25, "name": "C"}`,
	}
	for key, value := range data {
		suite.NoError(suite.db.Set(key, value))
	}
}

func (suite *UpdateSuite) TearDownTest() {
	if suite.db != nil {
		_ = suite.db.Close()
	}
}

func (suite *UpdateSuite) TestUpdateCases() {
	tests := []struct {
		name     string
		filter   x.Filter
		updates  []x.Mutation
		wantKeys []string
	}{
		{
			name:     "Update existing property",
			filter:   x.Eq("id", "1"),
			updates:  []x.Mutation{x.Set("age", 21)},
			wantKeys: []string{"user:1"},
		},
		{
			name:     "Add new property",
			filter:   x.Eq("id", "2"),
			updates:  []x.Mutation{x.Set("active", true)},
			wantKeys: []string{"user:2"},
		},
		{
			name:     "Update multiple documents",
			filter:   x.Gt("age", float64(24)),
			updates:  []x.Mutation{x.Set("status", "verified")},
			wantKeys: []string{"user:2", "user:3"},
		},
		{
			name:     "Update without filter applies to all",
			filter:   nil,
			updates:  []x.Mutation{x.Set("version", 2)},
			wantKeys: []string{"user:1", "user:2", "user:3"},
		},
		{
			name:   "Update all data types",
			filter: x.Eq("id", "1"),
			updates: []x.Mutation{
				x.Set("int_val", int(-10)),
				x.Set("int32_val", int32(32)),
				x.Set("int64_val", int64(64)),
				x.Set("float32_val", float32(3.5)),
				x.Set("float64_val", float64(6.28)),
				x.Set("string_val", "hello"),
				x.Set("bool_val", true),
			},
			wantKeys: []string{"user:1"},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.TearDownTest()
			suite.SetupTest()

			res := suite.db.Update("user:*", tt.filter, tt.updates...)
			suite.True(res.IsOk())
			keys := res.MustGet()
			suite.ElementsMatch(tt.wantKeys, keys)

			for _, key := range tt.wantKeys {
				doc := suite.db.Get(key).MustGet()
				suite.True(gjson.Valid(doc))
			}
		})
	}
}

func TestUpdateSuite(t *testing.T) {
	suite.Run(t, new(UpdateSuite))
}

func TestDBUpdateEdgeCases(t *testing.T) {
	t.Run("rejects empty pattern", func(t *testing.T) {
		db := openDB(testutil.DBPath(t))
		require.NotNil(t, db)
		t.Cleanup(func() { _ = db.Close() })

		res := db.Update("", nil, x.Set("status", "active"))
		require.Error(t, res.Error())
		require.Contains(t, res.Error().Error(), "key pattern is required")
	})

	t.Run("rejects leading wildcard pattern", func(t *testing.T) {
		db := openDB(testutil.DBPath(t))
		require.NotNil(t, db)
		t.Cleanup(func() { _ = db.Close() })

		res := db.Update("*user:*", nil, x.Set("status", "active"))
		require.Error(t, res.Error())
		require.Contains(t, res.Error().Error(), "cannot start with wildcard")
	})

	t.Run("returns empty when no documents match", func(t *testing.T) {
		db := openDB(testutil.DBPath(t))
		require.NotNil(t, db)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, db.Set("user:1", `{"id":"1","status":"pending"}`))

		res := db.Update("user:*", x.Eq("status", "active"), x.Set("status", "reviewed"))
		require.NoError(t, res.Error())
		require.Empty(t, res.MustGet())

		val := db.Get("user:1").MustGet()
		require.Equal(t, "pending", gjson.Get(val, "status").String())
	})

	t.Run("returns matched key even when update is no-op", func(t *testing.T) {
		db := openDB(testutil.DBPath(t))
		require.NotNil(t, db)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, db.Set("user:1", `{"id":"1","status":"active"}`))

		before := db.Get("user:1").MustGet()
		res := db.Update("user:*", x.Eq("id", "1"), x.Set("status", "active"))
		require.NoError(t, res.Error())
		require.Equal(t, []string{"user:1"}, res.MustGet())

		after := db.Get("user:1").MustGet()
		require.Equal(t, before, after)
	})

	t.Run("preserves ttl on updated keys", func(t *testing.T) {
		db := openDB(testutil.DBPath(t))
		require.NotNil(t, db)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, db.SetWithTtl("user:1", `{"id":"1","status":"pending"}`, 200*time.Millisecond))

		requireTTLPositive(t, db, "user:1")

		res := db.Update("user:*", x.Eq("id", "1"), x.Set("status", "active"))
		require.NoError(t, res.Error())
		require.Equal(t, []string{"user:1"}, res.MustGet())
		requireTTLPositive(t, db, "user:1")

		val := db.Get("user:1").MustGet()
		require.Equal(t, "active", gjson.Get(val, "status").String())
	})

	t.Run("returns error and leaves data unchanged when one mutation path is invalid", func(t *testing.T) {
		db := openDB(testutil.DBPath(t))
		require.NotNil(t, db)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, db.Set("user:1", `{"id":"1","status":"pending"}`))

		before := db.Get("user:1").MustGet()

		res := db.Update("user:*", nil, x.Set("", "active"))
		require.Error(t, res.Error())
		require.Contains(t, res.Error().Error(), "failed to set ")

		after := db.Get("user:1").MustGet()
		require.Equal(t, before, after)
	})
}

type SearchSuite struct {
	suite.Suite
	db *DB
}

func (suite *SearchSuite) SetupTest() {
	suite.db = openDB(testutil.DBPath(suite.T()))
	suite.NotNil(suite.db)
	suite.NoError(suite.db.registerIndexes(x.Idx[testUserDoc]("age", "*", "age")))

	dataBytes, err := os.ReadFile("testdata/SearchSuite_InitData.json")
	suite.NoError(err)

	var employees []json.RawMessage
	err = json.Unmarshal(dataBytes, &employees)
	suite.NoError(err)

	for _, emp := range employees {
		raw := string(emp)
		department := gjson.Get(raw, "department").String()
		id := gjson.Get(raw, "id").String()
		key := "user" + contract.StorageKeySeparator + department + contract.StorageKeySeparator + id
		suite.NoError(suite.db.Set(key, raw))
	}
}

func (suite *SearchSuite) TearDownTest() {
	if suite.db != nil {
		_ = suite.db.Close()
	}
}

func (suite *SearchSuite) TestSearchIndex() {
	tests := []struct {
		name      string
		indexName string
		kr        x.KeyRange
		filter    x.Filter
		desc      bool
		wantErr   bool
		wantLen   int
	}{
		{
			name:      "Query ascending all",
			indexName: x.Idx[testUserDoc]("age", "*", "age").Name(),
			kr:        x.KeysPattern("user:*"),
			filter:    nil,
			desc:      false,
			wantErr:   false,
			wantLen:   5,
		},
		{
			name:      "Query descending all",
			indexName: x.Idx[testUserDoc]("age", "*", "age").Name(),
			kr:        x.KeysPattern("user:*"),
			filter:    nil,
			desc:      true,
			wantErr:   false,
			wantLen:   5,
		},
		{
			name:      "Query with filter",
			indexName: x.Idx[testUserDoc]("age", "*", "age").Name(),
			kr:        x.KeysPattern("user:*"),
			filter:    x.Gt("age", float64(28)),
			desc:      false,
			wantErr:   false,
			wantLen:   2,
		},
		{
			name:      "Query with key pattern filter",
			indexName: x.Idx[testUserDoc]("age", "*", "age").Name(),
			kr:        x.KeysPattern("user:Engineering:*"),
			filter:    nil,
			desc:      false,
			wantErr:   false,
			wantLen:   3,
		},
		{
			name:      "Query empty index name",
			indexName: "",
			kr:        x.KeysPattern("user:*"),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			res := suite.db.SearchIndex(tt.indexName, tt.kr, tt.filter, tt.desc)
			if tt.wantErr {
				suite.True(res.IsError())
			} else {
				suite.True(res.IsOk())
				got := res.MustGet()
				suite.Len(got, tt.wantLen)
			}
		})
	}
}

func (suite *SearchSuite) TestSearchKey() {
	tests := []struct {
		name      string
		kr        x.KeyRange
		filter    x.Filter
		desc      bool
		wantErr   bool
		wantEmpty bool
		wantLen   int
	}{
		{
			name:      "QueryKey Engineering department ascending",
			kr:        x.KeysPattern("user:Engineering:*"),
			filter:    nil,
			desc:      false,
			wantEmpty: false,
			wantLen:   3,
		},
		{
			name:      "QueryKey Engineering department descending",
			kr:        x.KeysPattern("user:Engineering:*"),
			filter:    nil,
			desc:      true,
			wantEmpty: false,
			wantLen:   3,
		},
		{
			name:      "QueryKey with filter",
			kr:        x.KeysPattern("user:*"),
			filter:    x.Eq("is_active", true),
			desc:      false,
			wantEmpty: false,
			wantLen:   3,
		},
		{
			name:      "QueryKey no match",
			kr:        x.KeysPattern("user:Marketing:*"),
			wantEmpty: true,
		},
		{
			name:    "QueryKey rejects cross-layer wildcard",
			kr:      x.KeysPattern("*user:*"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			res := suite.db.SearchKey(tt.kr, tt.filter, tt.desc)
			if tt.wantErr {
				suite.True(res.IsError())
				suite.Contains(res.Error().Error(), "cannot start with wildcard")
				return
			}
			suite.True(res.IsOk())

			got := res.MustGet()
			if tt.wantEmpty {
				suite.Empty(got)
			} else {
				suite.Len(got, tt.wantLen)
			}
		})
	}
}

func (suite *SearchSuite) setup1000Fixture(prefix string) {
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%s%04d", prefix, i)
		val := fmt.Sprintf(`{"key":"%s","n":%d}`, key, i)
		suite.NoError(suite.db.Set(key, val))
	}
}

func (suite *SearchSuite) TestSearchKey1000Fixture() {
	prefix := "u:"
	suite.setup1000Fixture(prefix)

	suite.Run("B_1_KeysBt_band_count", func() {
		kr := x.KeysBt(prefix+"0100", prefix+"0200")
		res := suite.db.SearchKey(kr, nil, false)
		suite.True(res.IsOk())
		got := res.MustGet()
		suite.NotEqual(1000, len(got), "must not visit all 1000 entries — super-range sweep required")
		suite.GreaterOrEqual(len(got), 98)
		suite.LessOrEqual(len(got), 102)
		suite.Equal(100, len(got), "literal [0100,0200) half-open should hit exactly 100 keys")
	})

	suite.Run("B_2_Limit_10_early_stop_not_post_hoc_trunc", func() {
		krBase := x.KeysBt(prefix+"0100", prefix+"0200")
		full := suite.db.SearchKey(krBase, nil, false)
		suite.True(full.IsOk())
		fullList := full.MustGet()
		suite.Len(fullList, 100)

		kr10 := krBase.Limit(10)
		res := suite.db.SearchKey(kr10, nil, false)
		suite.True(res.IsOk())
		got10 := res.MustGet()
		suite.Len(got10, 10, "Limit(10) must return exactly 10 entries")
		suite.Equal(fullList[:10], got10, "Limit(10) must return first 10 of full ASC output")

		visited := 0
		err := suite.db.disk.View(func(tx *buntdb.Tx) error {
			return applyKeyRange(tx, kr10, x.RangeAsc, func(key, value string) bool {
				visited++
				return true
			})
		})
		suite.NoError(err)
		suite.Equal(10, visited, "kr.Apply must invoke callback exactly 10 times — LIMIT must be true early-stop, not post-hoc slice trunc")
	})

	suite.Run("B_3_KeysGte_pattern_pivot_strict_lex_asc", func() {
		kr := x.KeysGte(prefix + "05*")
		res := suite.db.SearchKey(kr, nil, false)
		suite.True(res.IsOk())
		got := res.MustGet()
		suite.Len(got, 100, "KeysGte(u:05*) should return band 0500..0599 exactly 100")

		keys := make([]string, 0, len(got))
		for _, v := range got {
			keys = append(keys, gjson.Get(v, "key").String())
		}
		for i := 1; i < len(keys); i++ {
			suite.Less(keys[i-1], keys[i], "strict lex ASC zero inversion: %s >= %s", keys[i-1], keys[i])
		}
		suite.Equal(prefix+"0500", keys[0])
		suite.Equal(prefix+"0599", keys[len(keys)-1])
	})

	suite.Run("B_4_KeysBt_pattern_pattern_dual_param", func() {
		kr := x.KeysBt(prefix+"03*", prefix+"07*").Limit(50)
		res := suite.db.SearchKey(kr, nil, false)
		suite.True(res.IsOk())
		got := res.MustGet()
		suite.Len(got, 50, "pattern/pattern bt Limit(50) first 50 by ASC order")

		keys := make([]string, 0, len(got))
		for _, v := range got {
			keys = append(keys, gjson.Get(v, "key").String())
		}
		for i := 1; i < len(keys); i++ {
			suite.Less(keys[i-1], keys[i], "strict lex ASC zero inversion in pattern/pattern: %s >= %s", keys[i-1], keys[i])
		}
		suite.Equal(prefix+"0300", keys[0])
		suite.Equal(prefix+"0349", keys[49])

		krNoLimit := x.KeysBt(prefix+"03*", prefix+"07*")
		fullRes := suite.db.SearchKey(krNoLimit, nil, false)
		suite.True(fullRes.IsOk())
		fullAll := fullRes.MustGet()
		// Heuristic for pattern/pattern KeysBt(ge, lt): Allowable(ge) to Allowable(lt)
		// sweeps bands 03,04,05,06,07 — total 500 entries (not 400), because
		// any key u:07XX satisfies match.Match(k, "u:07*") = true under the ltOK
		// OR-branch: (k < lt_literal) || (pattern && match(k, lt_pattern)).
		suite.Len(fullAll, 500)

		// Additional semantic check: first band should be 03, last band 07
		firstKey := gjson.Get(fullAll[0], "key").String()
		lastKey := gjson.Get(fullAll[len(fullAll)-1], "key").String()
		suite.Equal(prefix+"0300", firstKey)
		suite.Equal(prefix+"0799", lastKey)
	})
}

func TestSearchSuite(t *testing.T) {
	suite.Run(t, new(SearchSuite))
}
