package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
)

type DBSuite struct {
	suite.Suite
	db *DB
}

func (suite *DBSuite) SetupTest() {
	resetStorage()
	suite.db = openDB(":memory:")
	suite.NotNil(suite.db)
}

func (suite *DBSuite) TearDownTest() {
	if suite.db != nil {
		_ = suite.db.Close()
	}
	resetStorage()
}

func (suite *DBSuite) TestLifecycle() {
	suite.Run("In-Memory DB", func() {
		resetStorage()
		db := openDB(":memory:")
		suite.NotNil(db)
		err := db.Set("key1", "val1")
		suite.NoError(err)

		res := db.Get("key1")
		suite.True(res.IsOk())
		suite.Equal("val1", res.MustGet())

		_ = db.Close()
	})

	suite.Run("File DB", func() {
		resetStorage()
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

func TestDBSuite(t *testing.T) {
	suite.Run(t, new(DBSuite))
}

type UpdateSuite struct {
	suite.Suite
	db *DB
}

func (suite *UpdateSuite) SetupTest() {
	resetStorage()
	suite.db = openDB(":memory:")
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
	resetStorage()
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

type SearchSuite struct {
	suite.Suite
	db *DB
}

func (suite *SearchSuite) SetupTest() {
	resetStorage()
	suite.db = openDB(":memory:")
	suite.NotNil(suite.db)
	suite.NoError(suite.db.registerIndexes(Idx("*", "age")))

	dataBytes, err := os.ReadFile("testdata/SearchSuite_InitData.json")
	suite.NoError(err)

	var employees []json.RawMessage
	err = json.Unmarshal(dataBytes, &employees)
	suite.NoError(err)

	for _, emp := range employees {
		raw := string(emp)
		department := gjson.Get(raw, "department").String()
		id := gjson.Get(raw, "id").String()
		key := department + keySeparator + id
		suite.NoError(suite.db.Set(key, raw))
	}
}

func (suite *SearchSuite) TearDownTest() {
	if suite.db != nil {
		_ = suite.db.Close()
	}
	resetStorage()
}

func (suite *SearchSuite) TestSearchIndex() {
	tests := []struct {
		name      string
		indexName string
		filter    x.Filter
		desc      bool
		wantErr   bool
		wantLen   int
	}{
		{
			name:      "Query ascending all",
			indexName: "idx_age",
			filter:    nil,
			desc:      false,
			wantErr:   false,
			wantLen:   5,
		},
		{
			name:      "Query descending all",
			indexName: "idx_age",
			filter:    nil,
			desc:      true,
			wantErr:   false,
			wantLen:   5,
		},
		{
			name:      "Query with filter",
			indexName: "idx_age",
			filter:    x.Gt("age", float64(28)),
			desc:      false,
			wantErr:   false,
			wantLen:   2,
		},
		{
			name:      "Query empty index name",
			indexName: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			res := suite.db.SearchIndex(tt.indexName, tt.filter, tt.desc)
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
		pattern   string
		filter    x.Filter
		desc      bool
		wantEmpty bool
		wantLen   int
	}{
		{
			name:      "QueryKey Engineering department ascending",
			pattern:   "Engineering:*",
			filter:    nil,
			desc:      false,
			wantEmpty: false,
			wantLen:   3,
		},
		{
			name:      "QueryKey Engineering department descending",
			pattern:   "Engineering:*",
			filter:    nil,
			desc:      true,
			wantEmpty: false,
			wantLen:   3,
		},
		{
			name:      "QueryKey with filter",
			pattern:   "*:*",
			filter:    x.Eq("is_active", true),
			desc:      false,
			wantEmpty: false,
			wantLen:   3,
		},
		{
			name:      "QueryKey no match",
			pattern:   "Marketing:*",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			res := suite.db.SearchKey(tt.pattern, tt.filter, tt.desc)
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

func TestSearchSuite(t *testing.T) {
	suite.Run(t, new(SearchSuite))
}
