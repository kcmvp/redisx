package storage

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/kcmvp/indx/x"
	"github.com/kcmvp/indx/x/testutil"
	"github.com/stretchr/testify/suite"
)

type DBSuite struct {
	suite.Suite
	db DB
}

func (suite *DBSuite) SetupTest() {
	Reset()
	// Use an empty schema for basic tests
	suite.db = Open(false)
	suite.NotNil(suite.db)
}

func (suite *DBSuite) TearDownTest() {
	if suite.db != nil {
		_ = suite.db.Close()
	}
	Reset()
}

func (suite *DBSuite) TestLifecycle() {
	suite.Run("In-Memory DB", func() {
		Reset()
		db := Open(false)
		suite.NotNil(db)
		err := db.Set("key1", "val1")
		suite.NoError(err)

		res := db.Get("key1")
		suite.True(res.IsOk())
		suite.Equal("val1", res.MustGet())

		_ = db.Close()
	})

	suite.Run("Duplicate Schemas rejected", func() {
		Reset()
		s1 := JsonSchema("user", 0)
		s2 := JsonSchema("user", 0)
		db := Open(false, s1, s2)
		suite.Nil(db)
	})

	suite.Run("Persistent DB singleton", func() {
		Reset()
		origHome := os.Getenv("HOME")
		defer func() { _ = os.Setenv("HOME", origHome) }()

		tempDir := suite.T().TempDir()
		_ = os.Setenv("HOME", tempDir)

		db1 := Open(true)
		suite.NotNil(db1)

		db2 := Open(true)
		suite.Equal(db1, db2, "Should return singleton instance")

		_ = db1.Close()
	})

	suite.Run("Persistent DB singleton duplicate schemas", func() {
		Reset()
		origHome := os.Getenv("HOME")
		defer func() { _ = os.Setenv("HOME", origHome) }()

		tempDir := suite.T().TempDir()
		_ = os.Setenv("HOME", tempDir)

		s1 := JsonSchema("user", 0)
		s2 := JsonSchema("user", 0)

		db := Open(true, s1, s2)
		suite.Nil(db)
	})

	suite.Run("Persistent DB create dir error", func() {
		Reset()
		origHome := os.Getenv("HOME")
		defer func() { _ = os.Setenv("HOME", origHome) }()

		// Create a file where a directory is expected, so MkdirAll fails
		tempDir := suite.T().TempDir()
		fileHome := tempDir + "/fakehome"
		f, _ := os.Create(fileHome)
		_ = f.Close()
		_ = os.Setenv("HOME", fileHome)

		db := Open(true)
		suite.Nil(db)
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

func (suite *DBSuite) TestSave() {
	// Close default test db to use a specific schema for Save test
	_ = suite.db.Close()
	Reset()

	schema := JsonSchema("user", 0).PrefixAttr("id", "role")
	db := Open(false, schema)
	defer func() {
		_ = db.Close()
		Reset()
	}()

	tests := []struct {
		name      string
		json      string // For error cases or where we still want raw string injection
		wantErr   bool
		errStr    string
		verifyKey string
	}{
		{
			name:      "Valid JSON and prefixes",
			json:      "", // We will load this from the feature file
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
		suite.Run(tt.name, func() {
			inputJSON := tt.json
			if inputJSON == "" {
				inputJSON = testutil.LoadFeature(suite.T())
			}

			res := db.Save(schema, inputJSON)
			if tt.wantErr {
				suite.True(res.IsError())
				suite.Contains(res.Error().Error(), tt.errStr)
			} else {
				suite.False(res.IsError())
				suite.Equal(tt.verifyKey, res.MustGet())

				// verify it was actually saved by fetching the key
				got := db.Get(res.MustGet())
				suite.False(got.IsError())
				suite.JSONEq(inputJSON, got.MustGet())
			}
		})
	}
}

func TestDBSuite(t *testing.T) {
	suite.Run(t, new(DBSuite))
}

type UpdateSuite struct {
	suite.Suite
	db     DB
	schema Schema
}

func (suite *UpdateSuite) SetupTest() {
	Reset()
	suite.schema = JsonSchema("user", 0).PrefixAttr("id")
	suite.db = Open(false, suite.schema)
	suite.NotNil(suite.db)

	data := []string{
		`{"id": "1", "age": 20, "name": "A"}`,
		`{"id": "2", "age": 30, "name": "B"}`,
		`{"id": "3", "age": 25, "name": "C"}`,
	}
	for _, d := range data {
		res := suite.db.Save(suite.schema, d)
		suite.False(res.IsError())
	}
}

func (suite *UpdateSuite) TearDownTest() {
	if suite.db != nil {
		_ = suite.db.Close()
	}
	Reset()
}

func (suite *UpdateSuite) TestUpdateCases() {
	tests := []struct {
		name     string
		filter   x.Filter
		updates  []JsonPair
		wantKeys []string
	}{
		{
			name:     "Update existing property",
			filter:   x.Eq("id", "1"),
			updates:  []JsonPair{Pair("age", 21)},
			wantKeys: []string{"user:1"},
		},
		{
			name:     "Add new property",
			filter:   x.Eq("id", "2"),
			updates:  []JsonPair{Pair("active", true)},
			wantKeys: []string{"user:2"},
		},
		{
			name:     "Update multiple documents",
			filter:   x.Gt("age", float64(24)),
			updates:  []JsonPair{Pair("status", "verified")},
			wantKeys: []string{"user:2", "user:3"},
		},
		{
			name:     "Update without filter applies to all",
			filter:   nil,
			updates:  []JsonPair{Pair("version", 2)},
			wantKeys: []string{"user:1", "user:2", "user:3"},
		},
		{
			name:   "Update all data types",
			filter: x.Eq("id", "1"),
			updates: []JsonPair{
				Pair("int_val", int(-10)),
				Pair("int32_val", int32(32)),
				Pair("int64_val", int64(64)),
				Pair("float32_val", float32(3.5)),
				Pair("float64_val", float64(6.28)),
				Pair("string_val", "hello"),
				Pair("bool_val", true),
			},
			wantKeys: []string{"user:1"},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			// Because UpdateSuite uses shared DB state initialized in SetupTest,
			// we must tear down and re-setup the DB for EACH subtest to avoid dirty state.
			suite.TearDownTest()
			suite.SetupTest()

			res := suite.db.Update("user", tt.filter, tt.updates...)
			suite.True(res.IsOk())
			keys := res.MustGet()
			suite.ElementsMatch(tt.wantKeys, keys)

			// Load the expected state from the JSON file
			featureJSON := testutil.LoadFeature(suite.T())

			// Parse the feature file which is a map of key -> json document
			var expectedDocs map[string]json.RawMessage
			err := json.Unmarshal([]byte(featureJSON), &expectedDocs)
			suite.NoError(err)

			for k, expectedJSON := range expectedDocs {
				doc := suite.db.Get(k).MustGet()
				suite.JSONEq(string(expectedJSON), doc)
			}
		})
	}
}

func TestUpdateSuite(t *testing.T) {
	suite.Run(t, new(UpdateSuite))
}

type SearchSuite struct {
	suite.Suite
	db     DB
	schema Schema
}

func (suite *SearchSuite) SetupSuite() {
	Reset()
	suite.schema = JsonSchema("employee", 0).PrefixAttr("department", "id").Index("age")
	suite.db = Open(false, suite.schema)
	suite.NotNil(suite.db)

	// Load complex initial data from JSON feature file
	dataBytes, err := os.ReadFile("testdata/SearchSuite_InitData.json")
	suite.NoError(err)

	var employees []json.RawMessage
	err = json.Unmarshal(dataBytes, &employees)
	suite.NoError(err)

	for _, emp := range employees {
		res := suite.db.Save(suite.schema, string(emp))
		suite.False(res.IsError(), "Failed to save employee: %v", res.Error())
	}
}

func (suite *SearchSuite) TearDownSuite() {
	if suite.db != nil {
		_ = suite.db.Close()
	}
	Reset()
}

func (suite *SearchSuite) TestSearchIndex() {
	tests := []struct {
		name       string
		schemaName string
		indexAttr  string
		filter     x.Filter
		desc       bool
		wantErr    bool
	}{
		{
			name:       "Query ascending all",
			schemaName: "employee",
			indexAttr:  "age",
			filter:     nil,
			desc:       false,
			wantErr:    false,
		},
		{
			name:       "Query descending all",
			schemaName: "employee",
			indexAttr:  "age",
			filter:     nil,
			desc:       true,
			wantErr:    false,
		},
		{
			name:       "Query with filter",
			schemaName: "employee",
			indexAttr:  "age",
			filter:     x.Gt("age", float64(28)), // age > 28
			desc:       false,
			wantErr:    false,
		},
		{
			name:       "Query non existent index",
			schemaName: "employee",
			indexAttr:  "unknown",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			res := suite.db.SearchIndex(tt.schemaName, tt.indexAttr, tt.filter, tt.desc)
			if tt.wantErr {
				suite.True(res.IsError())
			} else {
				suite.True(res.IsOk())

				// Use feature driven validation
				featureJSON := testutil.LoadFeature(suite.T())
				var expected []json.RawMessage
				err := json.Unmarshal([]byte(featureJSON), &expected)
				suite.NoError(err)

				got := res.MustGet()
				suite.Equal(len(expected), len(got))
				for i, exp := range expected {
					suite.JSONEq(string(exp), got[i])
				}
			}
		})
	}
}

func (suite *SearchSuite) TestSearchKey() {
	tests := []struct {
		name       string
		schemaName string
		pattern    string
		filter     x.Filter
		desc       bool
		wantEmpty  bool
	}{
		{
			name:       "QueryKey Engineering department ascending",
			schemaName: "employee",
			pattern:    "Engineering:*",
			filter:     nil,
			desc:       false,
			wantEmpty:  false,
		},
		{
			name:       "QueryKey Engineering department descending",
			schemaName: "employee",
			pattern:    "Engineering:*",
			filter:     nil,
			desc:       true,
			wantEmpty:  false,
		},
		{
			name:       "QueryKey with filter",
			schemaName: "employee",
			pattern:    "*:*", // all departments
			filter:     x.Eq("is_active", true),
			desc:       false,
			wantEmpty:  false,
		},
		{
			name:       "QueryKey no match",
			schemaName: "employee",
			pattern:    "Marketing:*",
			wantEmpty:  true,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			res := suite.db.SearchKey(tt.schemaName, tt.pattern, tt.filter, tt.desc)
			suite.True(res.IsOk())

			got := res.MustGet()
			if tt.wantEmpty {
				suite.Empty(got)
			} else {
				// Use feature driven validation
				featureJSON := testutil.LoadFeature(suite.T())
				var expected []json.RawMessage
				err := json.Unmarshal([]byte(featureJSON), &expected)
				suite.NoError(err)

				suite.Equal(len(expected), len(got))
				for i, exp := range expected {
					suite.JSONEq(string(exp), got[i])
				}
			}
		})
	}
}

func TestSearchSuite(t *testing.T) {
	suite.Run(t, new(SearchSuite))
}
