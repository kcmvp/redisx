package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/x"
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
	suite.NotNil(suite.db.raw())
	suite.NotNil(suite.db.rawMem())
	suite.NotSame(suite.db.raw(), suite.db.rawMem())

	suite.NoError(suite.db.raw().Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set("disk:key", "disk", nil)
		return err
	}))
	suite.NoError(suite.db.rawMem().Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(naming.BuildStorageKey(naming.BuildStorageNs("mem", true), "key"), "mem", nil)
		return err
	}))

	suite.Equal("disk", suite.db.Get("disk:key").MustGet())
	suite.Equal("mem", suite.db.Get(naming.BuildStorageKey(naming.BuildStorageNs("mem", true), "key")).MustGet())
}

func (suite *DBSuite) TestRegisterIndexes() {
	suite.Run("registers disk layer index on pre-initialized idxRegSpec", func() {
		suite.NotNil(suite.db.idxRegSpec)
		err := suite.db.registerIndexes(x.Idx[testUserDoc]("age", "*", "age"))
		suite.NoError(err)
		diskIdx := x.Idx[testUserDoc]("age", "*", "age")
		idx, ok := suite.db.idxRegSpec[diskIdx.Name()]
		suite.True(ok)
		suite.False(naming.HasMemPrefix(idx.OwnerNs))
	})

	suite.Run("registers memory layer index", func() {
		err := suite.db.registerIndexes(x.Idx[testMemUserDoc]("age", "*", "age"))
		suite.NoError(err)
		memIdx := x.Idx[testMemUserDoc]("age", "*", "age")
		idx, ok := suite.db.idxRegSpec[memIdx.Name()]
		suite.True(ok)
		suite.True(naming.HasMemPrefix(idx.OwnerNs))
	})

	suite.Run("rejects empty index name", func() {
		err := suite.db.registerIndexes(x.Index{})
		suite.EqualError(err, "index name is required")
	})

	suite.Run("duplicate index declaration is idempotent via writeIndexSpec", func() {
		idx := x.Idx[testUserDoc]("dup", "*", "age")
		suite.NoError(suite.db.registerIndexes(idx))
		err := suite.db.registerIndexes(idx)
		suite.NoError(err)
	})
}

func (suite *DBSuite) TestLoadIndexes() {
	suite.Run("loads disk index meta and builds BTree", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		spec := idxSpec{
			OwnerNs:    "user",
			Logical:    "age",
			KeyPattern: "user:*",
			Paths:      []string{"age"},
		}
		versionHex, _ := canonicalIdxMD5(spec)
		spec.Version = versionHex
		raw, err := json.Marshal(spec)
		suite.NoError(err)
		metaKey := naming.IdxMetaKey(spec.fullName(), versionHex)
		suite.NoError(db.raw().Update(func(tx *buntdb.Tx) error {
			_, _, e := tx.Set(metaKey, string(raw), nil)
			return e
		}))

		suite.NoError(db.loadIndexes())
		loaded, ok := db.idxRegSpec[spec.fullName()]
		suite.True(ok, "index should be registered")
		suite.Equal(spec.fullName(), loaded.fullName())
		suite.Equal(spec.Paths, loaded.Paths)
		suite.Equal(versionHex, loaded.Version)
		suite.False(naming.HasMemPrefix(loaded.OwnerNs))

		suite.NoError(db.Set("user:1", `{"id":"1","age":20}`))
		suite.NoError(db.Set("user:2", `{"id":"2","age":30}`))
		res := db.SearchIndex(spec.fullName(), x.KeysPattern("user:*"), nil, false)
		suite.True(res.IsOk())
		suite.Len(res.MustGet(), 2)
	})

	suite.Run("loads mem index meta and builds BTree", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		memStorageNs := naming.BuildStorageNs("user", true)
		spec := idxSpec{
			OwnerNs:    memStorageNs,
			Logical:    "rank",
			KeyPattern: naming.StorageNsKeyPattern(memStorageNs),
			Paths:      []string{"rank"},
		}
		versionHex, _ := canonicalIdxMD5(spec)
		spec.Version = versionHex
		raw, err := json.Marshal(spec)
		suite.NoError(err)
		metaKey := naming.IdxMetaKey(spec.fullName(), versionHex)
		suite.NoError(db.rawMem().Update(func(tx *buntdb.Tx) error {
			_, _, e := tx.Set(metaKey, string(raw), nil)
			return e
		}))

		suite.NoError(db.loadIndexes())
		loaded, ok := db.idxRegSpec[spec.fullName()]
		suite.True(ok, "mem index should be registered")
		suite.True(naming.HasMemPrefix(loaded.OwnerNs))
		suite.Equal(versionHex, loaded.Version)

		suite.NoError(db.rawMem().Update(func(tx *buntdb.Tx) error {
			_, _, e := tx.Set(naming.BuildStorageKey(memStorageNs, "x1"), `{"id":"x1","rank":5}`, nil)
			return e
		}))
		res := db.SearchIndex(spec.fullName(), x.KeysPattern(naming.StorageNsKeyPattern(memStorageNs)), nil, false)
		suite.True(res.IsOk())
		suite.Len(res.MustGet(), 1)
	})

	suite.Run("corrupt meta record returns error", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		metaKey := naming.IdxMetaKey("user_broken", "placeholder")
		suite.NoError(db.raw().Update(func(tx *buntdb.Tx) error {
			_, _, e := tx.Set(metaKey, "{not json", nil)
			return e
		}))

		err := db.loadIndexes()
		suite.Error(err)
		suite.Contains(err.Error(), "index "+metaKey+": unmarshal idxSpec")
	})

	suite.Run("index with empty FullName returns error", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		spec := idxSpec{
			OwnerNs: "user", Logical: "x",
			KeyPattern: "user:*", Paths: []string{"x"},
		}
		raw, err := json.Marshal(spec)
		suite.NoError(err)
		badRaw := strings.ReplaceAll(string(raw), `"owner_ns":"user"`, `"owner_ns":""`)
		metaKey := naming.IdxMetaKey("user_bad", "placeholder")
		suite.NoError(db.raw().Update(func(tx *buntdb.Tx) error {
			_, _, e := tx.Set(metaKey, badRaw, nil)
			return e
		}))

		err = db.loadIndexes()
		suite.Error(err)
		suite.Contains(err.Error(), "index "+metaKey+": missing owner_ns/logical")
	})
}

func (suite *DBSuite) TestLoadDocSpecs() {
	suite.Run("loads disk doc meta and registers namespace", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		spec := docSpec{
			Namespace: "profile",
			Mem:       false,
			KeyAttrs:  []string{"uid"},
			TTL:       2 * time.Hour,
		}
		versionHex, _ := canonicalDocMD5(spec)
		spec.Version = versionHex
		raw, err := json.Marshal(spec)
		suite.NoError(err)
		metaKey := naming.DocMetaKey(spec.storageNs(), versionHex)
		suite.NoError(db.raw().Update(func(tx *buntdb.Tx) error {
			_, _, e := tx.Set(metaKey, string(raw), nil)
			return e
		}))

		suite.NoError(db.loadDocSpecs())
		loaded, ok := db.docRegSpec[spec.storageNs()]
		suite.True(ok, "doc should be registered")
		suite.Equal("profile", loaded.Namespace)
		suite.Equal([]string{"uid"}, loaded.KeyAttrs)
		suite.Equal(2*time.Hour, loaded.TTL)
		suite.Equal(versionHex, loaded.Version)
	})

	suite.Run("loads mem doc meta", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		spec := docSpec{
			Namespace: "session",
			Mem:       true,
			KeyAttrs:  []string{"sid"},
			TTL:       0,
		}
		versionHex, _ := canonicalDocMD5(spec)
		spec.Version = versionHex
		raw, err := json.Marshal(spec)
		suite.NoError(err)
		metaKey := naming.DocMetaKey(spec.storageNs(), versionHex)
		suite.NoError(db.rawMem().Update(func(tx *buntdb.Tx) error {
			_, _, e := tx.Set(metaKey, string(raw), nil)
			return e
		}))

		suite.NoError(db.loadDocSpecs())
		loaded, ok := db.docRegSpec[spec.storageNs()]
		suite.True(ok)
		suite.True(loaded.Mem)
		suite.Equal("session", loaded.Namespace)
		suite.Equal(versionHex, loaded.Version)
	})

	suite.Run("semantic change (TTL) via writeDocSpec after load triggers version upgrade", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		older := docSpec{
			Namespace: "account", Mem: false, KeyAttrs: []string{"id"},
			TTL: time.Hour,
		}
		suite.NoError(db.writeDocSpec(older))
		v1hex := db.docRegSpec[older.storageNs()].Version
		v1created := db.docRegSpec[older.storageNs()].CreatedAt
		suite.NotEmpty(v1hex)
		suite.False(v1created.IsZero())

		suite.NoError(db.loadDocSpecs())

		incoming := docSpec{
			Namespace: "account", Mem: false, KeyAttrs: []string{"id"},
			TTL: 48 * time.Hour,
		}
		err := db.writeDocSpec(incoming)
		suite.NoError(err, "writeDocSpec upgrades semantic changes (MD5 diff)")
		upgraded, ok := db.docRegSpec[older.storageNs()]
		suite.True(ok)
		suite.Equal(48*time.Hour, upgraded.TTL, "TTL must reflect new spec on version upgrade")
		suite.NotEqual(v1hex, upgraded.Version, "version must change after MD5 differs")
		suite.Equal(v1created, upgraded.CreatedAt, "CreatedAt must be preserved across upgrades")
		suite.False(upgraded.UpdatedAt.IsZero(), "UpdatedAt must be set on first upgrade")
	})

	suite.Run("re-applying identical schema is no-op (MD5 matches)", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		spec := docSpec{
			Namespace: "static", Mem: false, KeyAttrs: []string{"k"},
			TTL: time.Minute,
		}
		suite.NoError(db.writeDocSpec(spec))
		suite.NoError(db.loadDocSpecs())
		firstUpdated := db.docRegSpec[spec.storageNs()].UpdatedAt
		err := db.writeDocSpec(spec)
		suite.NoError(err, "writeDocSpec with identical MD5 returns nil no-op")
		loaded, ok := db.docRegSpec[spec.storageNs()]
		suite.True(ok)
		suite.Equal(firstUpdated, loaded.UpdatedAt, "no-op write must leave UpdatedAt untouched")
	})

	suite.Run("KeyAttrs semantic change triggers upgrade (overwrites)", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		spec := docSpec{
			Namespace: "conflict", Mem: false, KeyAttrs: []string{"a"},
			TTL: time.Hour,
		}
		suite.NoError(db.writeDocSpec(spec))
		v1created := db.docRegSpec[spec.storageNs()].CreatedAt
		suite.NoError(db.loadDocSpecs())

		bad := docSpec{
			Namespace: "conflict", Mem: false, KeyAttrs: []string{"b"},
			TTL: time.Hour,
		}
		err := db.writeDocSpec(bad)
		suite.NoError(err, "writeDocSpec upgrades differing KeyAttrs (MD5 diff)")
		overwritten, ok := db.docRegSpec[spec.storageNs()]
		suite.True(ok)
		suite.Equal([]string{"b"}, overwritten.KeyAttrs, "KeyAttrs must reflect latest spec on upgrade")
		suite.Equal(v1created, overwritten.CreatedAt, "CreatedAt preserved across KeyAttrs upgrade")
		suite.False(overwritten.UpdatedAt.IsZero(), "UpdatedAt set on KeyAttrs upgrade")
	})

	suite.Run("corrupt meta record returns error", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		metaKey := naming.DocMetaKey("brokenns", "placeholder")
		suite.NoError(db.raw().Update(func(tx *buntdb.Tx) error {
			_, _, e := tx.Set(metaKey, "not a json", nil)
			return e
		}))
		err := db.loadDocSpecs()
		suite.Error(err)
		suite.Contains(err.Error(), "doc "+metaKey+": unmarshal docSpec")
	})

	suite.Run("empty Namespace in meta returns error", func() {
		db := openDB(testutil.DBPath(suite.T()))
		suite.NotNil(db)
		defer func() { _ = db.Close() }()

		spec := docSpec{KeyAttrs: []string{"id"}, TTL: time.Hour}
		raw, err := json.Marshal(spec)
		suite.NoError(err)
		metaKey := naming.DocMetaKey("bad", "placeholder")
		suite.NoError(db.raw().Update(func(tx *buntdb.Tx) error {
			_, _, e := tx.Set(metaKey, string(raw), nil)
			return e
		}))
		err = db.loadDocSpecs()
		suite.Error(err)
		suite.Contains(err.Error(), "doc "+metaKey+": empty namespace")
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

	suite.Run("SetNXWithTtl", func() {
		ok, err := suite.db.SetNXWithTtl("nx_ttl_key", "val1", 10*time.Millisecond)
		suite.NoError(err)
		suite.True(ok)
		requireTTLPositive(suite.T(), suite.db, "nx_ttl_key")

		ok, err = suite.db.SetNXWithTtl("nx_ttl_key", "val2", time.Hour)
		suite.NoError(err)
		suite.False(ok, "Should not set existing key")

		res := suite.db.Get("nx_ttl_key")
		suite.Equal("val1", res.MustGet())

		time.Sleep(20 * time.Millisecond)
		res = suite.db.Get("nx_ttl_key")
		suite.True(res.IsError(), "Key should be expired")
	})
}

func (suite *DBSuite) TestHybridStorageLayers() {
	path := filepath.Join(suite.T().TempDir(), "hybrid", "kv.db")
	suite.NoError(os.MkdirAll(filepath.Dir(path), 0o755))
	db := openDB(path)
	suite.NotNil(db)
	suite.NoError(db.writeDocSpec(docSpec{Namespace: "user", Mem: false, KeyAttrs: []string{"id"}, TTL: 0}))
	suite.NoError(db.writeDocSpec(docSpec{Namespace: "user", Mem: true, KeyAttrs: []string{"id"}, TTL: 0}))
	suite.NoError(db.registerIndexes(
		x.Idx[testUserDoc]("age", "*", "age"),
		x.Idx[testMemUserDoc]("age", "*", "age"),
	))

	memStorageNs := naming.BuildStorageNs("user", true)
	userMemPrefix := naming.StorageNsScope(memStorageNs)

	suite.NoError(db.Set("user:1", `{"id":"1","age":20,"status":"cold"}`))
	suite.NoError(db.Set(userMemPrefix+"2", `{"id":"2","age":30,"status":"hot"}`))
	suite.NoError(db.Set(userMemPrefix+"3", `{"id":"3","age":25,"status":"hot"}`))

	keys := db.Keys("*user:*")
	suite.True(keys.IsError())
	suite.Contains(keys.Error().Error(), "cannot start with wildcard")
	suite.ElementsMatch([]string{"user:1"}, db.Keys("user:*").MustGet())
	suite.ElementsMatch([]string{userMemPrefix + "2", userMemPrefix + "3"}, db.Keys(userMemPrefix+"*").MustGet())

	res := db.SearchKey(x.KeysPattern("*user:*"), nil, false)
	suite.True(res.IsError())
	suite.Contains(res.Error().Error(), "cannot start with wildcard")

	updated := db.Update(x.KeysPattern("*user:*"), nil, x.Set("status", "active"))
	suite.True(updated.IsError())
	suite.Contains(updated.Error().Error(), "cannot start with wildcard")
	suite.Equal("cold", gjson.Get(db.Get("user:1").MustGet(), "status").String())
	suite.Equal("hot", gjson.Get(db.Get(userMemPrefix+"2").MustGet(), "status").String())

	diskRes := db.SearchIndex(x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), nil, false)
	suite.True(diskRes.IsOk())
	suite.Len(diskRes.MustGet(), 1)

	memRes := db.SearchIndex(x.Idx[testMemUserDoc]("age", "*", "age").Name(), x.KeysPattern(userMemPrefix+"*"), nil, false)
	suite.True(memRes.IsOk())
	suite.Len(memRes.MustGet(), 2)

	mismatchDisk := db.SearchIndex(x.Idx[testUserDoc]("age", "*", "age").Name(), x.KeysPattern(userMemPrefix+"*"), nil, false)
	suite.True(mismatchDisk.IsError())
	suite.Contains(mismatchDisk.Error().Error(), "different storage layer")

	mismatchMem := db.SearchIndex(x.Idx[testMemUserDoc]("age", "*", "age").Name(), x.KeysPattern("user:*"), nil, false)
	suite.True(mismatchMem.IsError())
	suite.Contains(mismatchMem.Error().Error(), "different storage layer")
}

func (suite *DBSuite) TestCheatsheetSymmetry() {
	path := testutil.DBPath(suite.T())
	db := openDB(path)
	suite.NotNil(db)

	// Doc cheatsheet contract (x/document.go RESP Wire Cheatsheet §0)
	// — CheatDoc    disk: Namespace="cheatdoc"    Mem=false KeyAttrs=["tenant","id"] TTL=24h Index=age
	// — CheatSess   mem:  Namespace="cheatsess"   Mem=true  KeyAttrs=["sid"]          TTL=30m Index=last
	suite.NoError(db.writeDocSpec(docSpec{
		Namespace: "cheatdoc", Mem: false, KeyAttrs: []string{"tenant", "id"}, TTL: 24 * time.Hour,
	}))
	suite.NoError(db.writeDocSpec(docSpec{
		Namespace: "cheatsess", Mem: true, KeyAttrs: []string{"sid"}, TTL: 30 * time.Minute,
	}))
	userDiskSN := naming.BuildStorageNs("cheatdoc", false)
	sessionMemSN := naming.BuildStorageNs("cheatsess", true)
	suite.NoError(db.writeIndexSpec(idxSpec{
		OwnerNs:    userDiskSN,
		Logical:    "age",
		KeyPattern: naming.StorageNsKeyPattern(userDiskSN),
		Paths:      []string{"age"},
	}))
	suite.NoError(db.writeIndexSpec(idxSpec{
		OwnerNs:    sessionMemSN,
		Logical:    "last",
		KeyPattern: naming.StorageNsKeyPattern(sessionMemSN),
		Paths:      []string{"last_ts"},
	}))

	userDocFullKey := naming.BuildStorageKey(userDiskSN, naming.JoinPKAttrValues([]string{"acme", "7"}))
	suite.Equal("cheatdoc:acme_7", userDocFullKey, "cheatsheet CheatDoc full key = cheatdoc:{tenant}_{id} for {acme,7}")
	sessionDocFullKey := naming.BuildStorageKey(sessionMemSN, naming.JoinPKAttrValues([]string{"abc"}))
	suite.Equal("_m_:cheatsess:abc", sessionDocFullKey, "cheatsheet CheatSess full key = _m_:cheatsess:{sid} for {abc}")
	suite.Equal("_doc_:cheatdoc:v_placeholder", naming.DocMetaKey(userDiskSN, "placeholder"), "cheatsheet doc meta: _doc_:{storageNs}")
	suite.Equal("_doc_:_m_:cheatsess:v_placeholder", naming.DocMetaKey(sessionMemSN, "placeholder"), "cheatsheet session doc meta")
	userAgeIdx := naming.BuildIdxFullName(userDiskSN, "age")
	sessionLastIdx := naming.BuildIdxFullName(sessionMemSN, "last")
	suite.Equal("cheatdoc:age", userAgeIdx, "cheatsheet cheatdoc index full name = {storageNs}:age")
	suite.Equal("_m_:cheatsess:last", sessionLastIdx, "cheatsheet cheatsess index full name = {storageNs}:last")
	suite.Equal("_idx_:cheatdoc:age:v_placeholder", naming.IdxMetaKey(userAgeIdx, "placeholder"), "cheatsheet cheatdoc index meta")
	suite.Equal("_idx_:_m_:cheatsess:last:v_placeholder", naming.IdxMetaKey(sessionLastIdx, "placeholder"), "cheatsheet cheatsess index meta")

	suite.NoError(db.Set(userDocFullKey, `{"tenant":"acme","id":"7","age":30,"status":"cold"}`))
	suite.NoError(db.Set(sessionDocFullKey, `{"sid":"abc","user_id":1,"last_ts":1000}`))
	suite.True(db.Get(userDocFullKey).IsOk(), "CheatDoc disk value written at cheatsheet exact key")
	suite.True(db.Get(sessionDocFullKey).IsOk(), "CheatSess mem value written at cheatsheet exact key")

	suite.NoError(db.Close())

	db = openDB(path)
	suite.NotNil(db)
	suite.NoError(db.loadIndexes())
	suite.NoError(db.loadDocSpecs())
	defer func() { _ = db.Close() }()

	// Registry persistence must follow storage layer symmetry: mem-only specs
	// (Docs + Indexes) live on mem-layer meta store so they are dropped on
	// restart; disk-layer specs persist across restart.
	userMemSN := naming.BuildStorageNs("cheatdoc", true)
	diskIdx := naming.BuildIdxFullName(userDiskSN, "age")
	memIdx := naming.BuildIdxFullName(userMemSN, "age")
	_, hasDiskDoc := db.docRegSpec[userDiskSN]
	suite.True(hasDiskDoc, "disk doc %s must remain registered after restart", userDiskSN)
	_, hasMemDoc := db.docRegSpec[userMemSN]
	suite.False(hasMemDoc, "mem doc %s must be dropped on restart (meta lives on mem-layer store)", userMemSN)
	_, hasDiskIdx := db.idxRegSpec[diskIdx]
	suite.True(hasDiskIdx, "disk index %s must remain registered after restart", diskIdx)
	_, hasMemIdx := db.idxRegSpec[memIdx]
	suite.False(hasMemIdx, "mem index %s must be dropped on restart (meta lives on mem-layer store)", memIdx)

	// Cheatsheet §3 symmetric restart — CheatDoc disk vs CheatSess mem.
	suite.True(db.Get(userDocFullKey).IsOk(), "cheatsheet: CheatDoc disk value cheatdoc:acme_7 must survive restart")
	suite.True(db.Get(sessionDocFullKey).IsError(), "cheatsheet: CheatSess mem value _m_:cheatsess:abc must NOT survive restart")
	_, hasSessionDocReg := db.docRegSpec[sessionMemSN]
	suite.False(hasSessionDocReg, "cheatsheet: CheatSess mem docSpec _doc_:_m_:cheatsess must NOT auto-re-register on restart")
	_, hasSessionLastIdx := db.idxRegSpec[sessionLastIdx]
	suite.False(hasSessionLastIdx, "cheatsheet: CheatSess mem idxSpec _idx_:_m_:cheatsess:last must NOT auto-re-register on restart")
}

func TestDBSuite(t *testing.T) {
	suite.Run(t, new(DBSuite))
}

func (suite *DBSuite) TestBatchAtomicCrossLayerAndNxAllOrNothing() {
	userStorageNs := naming.BuildStorageNs("user", false)
	userMemStorageNs := naming.BuildStorageNs("user", true)

	suite.NoError(suite.db.writeDocSpec(docSpec{Namespace: "user", Mem: false, KeyAttrs: []string{"id"}, TTL: 0}))

	suite.Run("rejects cross-layer writes", func() {
		batch := []batchedWrite{
			{Key: naming.BuildStorageKey(userStorageNs, "1"), Value: `{"id":"1"}`, TTL: 0},
			{Key: naming.BuildStorageKey(userMemStorageNs, "2"), Value: `{"id":"2"}`, TTL: 0},
		}
		_, err := suite.db.setBatchAtomic(batch, false)
		suite.Error(err)
		suite.Contains(err.Error(), "cannot span storage layers")
	})

	suite.Run("rejects cross-layer deletes", func() {
		keys := []string{
			naming.BuildStorageKey(userStorageNs, "1"),
			naming.BuildStorageKey(userMemStorageNs, "2"),
		}
		_, err := suite.db.deleteBatchAtomic(keys)
		suite.Error(err)
		suite.Contains(err.Error(), "cannot span storage layers")
	})

	suite.Run("same layer writes are atomic", func() {
		batch := []batchedWrite{
			{Key: naming.BuildStorageKey(userStorageNs, "a"), Value: `{"id":"a"}`, TTL: 0},
			{Key: naming.BuildStorageKey(userStorageNs, "b"), Value: `{"id":"b"}`, TTL: 0},
		}
		n, err := suite.db.setBatchAtomic(batch, false)
		suite.NoError(err)
		suite.Equal(2, n)
		suite.True(suite.db.Get(naming.BuildStorageKey(userStorageNs, "a")).IsOk())
		suite.True(suite.db.Get(naming.BuildStorageKey(userStorageNs, "b")).IsOk())
	})

	suite.Run("NX all-or-null precondition rollback", func() {
		suite.NoError(suite.db.Set(naming.BuildStorageKey(userStorageNs, "nx_hit"), `{"id":"nx_hit"}`))
		batch := []batchedWrite{
			{Key: naming.BuildStorageKey(userStorageNs, "nx_hit"), Value: `{"id":"nx_hit","v":2}`, TTL: 0},
			{Key: naming.BuildStorageKey(userStorageNs, "nx_new"), Value: `{"id":"nx_new"}`, TTL: 0},
		}
		n, err := suite.db.setBatchAtomic(batch, true)
		suite.Equal(0, n)
		suite.ErrorIs(err, errNxPreconditionFailed)
		suite.True(suite.db.Get(naming.BuildStorageKey(userStorageNs, "nx_new")).IsError(), "new key in NX collision batch must NOT be written")
		suite.Equal(`{"id":"nx_hit"}`, suite.db.Get(naming.BuildStorageKey(userStorageNs, "nx_hit")).MustGet())
	})

	suite.Run("empty batch no error", func() {
		n, err := suite.db.setBatchAtomic(nil, false)
		suite.NoError(err)
		suite.Equal(0, n)
	})
}

func (suite *DBSuite) TestUpdatePKMutationStrict() {
	suite.NoError(suite.db.writeDocSpec(docSpec{Namespace: "pkdoc", Mem: false, KeyAttrs: []string{"id"}}))
	sn := naming.BuildStorageNs("pkdoc", false)
	suite.NoError(suite.db.Set(naming.BuildStorageKey(sn, "10"), `{"id":"10","age":1}`))

	updated := suite.db.Update(x.KeysPattern(naming.BuildStorageKey(sn, "*")), nil, x.Set("id", "20"))
	suite.True(updated.IsError())
	suite.Contains(updated.Error().Error(), "pk mutations are not allowed")
	suite.Equal(`{"id":"10","age":1}`, suite.db.Get(naming.BuildStorageKey(sn, "10")).MustGet())
}

func (suite *DBSuite) TestCompositeIndexOrderAscend() {
	docSN := naming.BuildStorageNs("compositens", false)
	suite.NoError(suite.db.writeDocSpec(docSpec{Namespace: "compositens", Mem: false, KeyAttrs: []string{"id"}}))

	idx := idxSpec{
		OwnerNs:    docSN,
		Logical:    "tenantage",
		KeyPattern: docSN + ":*",
		Paths:      []string{"tenant", "age"},
	}
	suite.NoError(suite.db.writeIndexSpec(idx))

	docs := []struct {
		id     string
		tenant string
		age    int
	}{
		{"6", "initech", 25},
		{"2", "acme", 30},
		{"4", "globex", 18},
		{"3", "acme", 30},
		{"1", "acme", 20},
		{"5", "globex", 45},
	}
	for _, d := range docs {
		key := naming.BuildStorageKey(docSN, d.id)
		val := fmt.Sprintf(`{"id":%q,"tenant":%q,"age":%d}`, d.id, d.tenant, d.age)
		suite.NoError(suite.db.Set(key, val))
	}

	asc := suite.db.SearchIndex(idx.fullName(), x.KeysPattern(docSN+":*"), nil, false)
	if !suite.True(asc.IsOk(), "SearchIndex ASC err: %v", asc.Error()) {
		return
	}
	gotDocs := asc.MustGet()
	suite.Len(gotDocs, 6, "expected 6 docs in ASC order, got %v", len(gotDocs))

	wantTenantOrder := []string{"acme", "acme", "acme", "globex", "globex", "initech"}
	wantAgeOrder := []int{20, 30, 30, 18, 45, 25}
	for i, raw := range gotDocs {
		gotTenant := gjson.Get(raw, "tenant").String()
		gotAge := int(gjson.Get(raw, "age").Float())
		suite.Equal(wantTenantOrder[i], gotTenant, "ASC idx=%d tenant mismatch; raw=%s", i, raw)
		suite.Equal(wantAgeOrder[i], gotAge, "ASC idx=%d age mismatch; raw=%s", i, raw)
	}

	desc := suite.db.SearchIndex(idx.fullName(), x.KeysPattern(docSN+":*"), nil, true)
	suite.True(desc.IsOk(), "SearchIndex DESC err: %v", desc.Error())
	gotDesc := desc.MustGet()
	suite.Len(gotDesc, 6)
	suite.Equal("initech", gjson.Get(gotDesc[0], "tenant").String(), "DESC first should be initech (tenant last)")
}

func (suite *DBSuite) TestDocSpecPersistsCreatedAt() {
	spec := docSpec{Namespace: "createdatns", Mem: false, KeyAttrs: []string{"id"}, TTL: 0}
	suite.NoError(suite.db.writeDocSpec(spec))

	var raw string
	metaGlob := naming.DocMetaGlobFor(spec.storageNs())
	suite.NoError(suite.db.raw().View(func(tx *buntdb.Tx) error {
		found := false
		scanErr := tx.AscendKeys(metaGlob, func(k, v string) bool {
			raw = v
			found = true
			return false
		})
		if scanErr != nil {
			return scanErr
		}
		if !found {
			return fmt.Errorf("no doc meta key found under %s", metaGlob)
		}
		return nil
	}))
	ca := gjson.Get(raw, "created_at")
	suite.True(ca.Exists(), "doc meta raw json must contain created_at; got: %s", raw)
	suite.NotEmpty(ca.String())

	spec2 := idxSpec{OwnerNs: spec.storageNs(), Logical: "age", KeyPattern: spec.storageNs() + ":*", Paths: []string{"age"}}
	suite.NoError(suite.db.writeIndexSpec(spec2))
	var iraw string
	idxMetaGlob := naming.IdxMetaGlobFor(spec2.fullName())
	suite.NoError(suite.db.raw().View(func(tx *buntdb.Tx) error {
		found := false
		scanErr := tx.AscendKeys(idxMetaGlob, func(k, v string) bool {
			iraw = v
			found = true
			return false
		})
		if scanErr != nil {
			return scanErr
		}
		if !found {
			return fmt.Errorf("no idx meta key found under %s", idxMetaGlob)
		}
		return nil
	}))
	ica := gjson.Get(iraw, "created_at")
	suite.True(ica.Exists(), "idx meta raw json must contain created_at; got: %s", iraw)
	suite.NotEmpty(ica.String())
}

func (suite *DBSuite) TestWriteDocSpecLifecycleRetainsCreatedAt() {
	specV1 := docSpec{Namespace: "cyclens", Mem: false, KeyAttrs: []string{"id"}, TTL: time.Hour}
	suite.NoError(suite.db.writeDocSpec(specV1))

	storageNs := specV1.storageNs()
	regV1, ok := suite.db.docRegSpec[storageNs]
	suite.True(ok, "doc spec must be registered after writeDocSpec V1")
	suite.False(regV1.CreatedAt.IsZero(), "V1 CreatedAt must be non-zero (first write)")
	suite.True(regV1.UpdatedAt.IsZero(), "V1 UpdatedAt must be zero (omitempty means never updated)")
	suite.NotEmpty(regV1.Version, "V1 Version must be 12-hex MD5")
	v1hex := regV1.Version
	v1created := regV1.CreatedAt

	specV2 := docSpec{Namespace: "cyclens", Mem: false, KeyAttrs: []string{"id"}, TTL: 48 * time.Hour}
	suite.NoError(suite.db.writeDocSpec(specV2))
	regV2, ok := suite.db.docRegSpec[storageNs]
	suite.True(ok)
	suite.Equal(v1created, regV2.CreatedAt, "V2 CreatedAt must retain the original V1 timestamp unchanged")
	suite.False(regV2.UpdatedAt.IsZero(), "V2 UpdatedAt must be set on semantic change (MD5 differs)")
	suite.NotEqual(v1hex, regV2.Version, "V2 Version must differ after TTL semantic change")
	v2updated := regV2.UpdatedAt
	v2hex := regV2.Version

	specV2Again := docSpec{Namespace: "cyclens", Mem: false, KeyAttrs: []string{"id"}, TTL: 48 * time.Hour}
	suite.NoError(suite.db.writeDocSpec(specV2Again))
	regV2b, ok := suite.db.docRegSpec[storageNs]
	suite.True(ok)
	suite.Equal(v2updated, regV2b.UpdatedAt, "noop repeat write must not refresh UpdatedAt")
	suite.Equal(v2hex, regV2b.Version, "noop repeat write must keep Version unchanged")
	suite.Equal(v1created, regV2b.CreatedAt, "CreatedAt still immutable after noop")

	var metaKeys []string
	suite.NoError(suite.db.raw().View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(naming.DocMetaGlobFor(storageNs), func(k, v string) bool {
			metaKeys = append(metaKeys, k)
			return true
		})
	}))
	suite.Len(metaKeys, 1, "disk invariant: exactly 1 _doc_:*:v_* key per logical ns after 2 versions + noop; got: %v", metaKeys)
}

func (suite *DBSuite) TestWriteIndexSpecVersionChangeDropsOldAndMountsNewComparator() {
	suite.NoError(suite.db.writeDocSpec(docSpec{Namespace: "cycleidx", Mem: false, KeyAttrs: []string{"id"}, TTL: 0}))
	ownerNs := naming.BuildStorageNs("cycleidx", false)

	idxV1 := idxSpec{OwnerNs: ownerNs, Logical: "age", KeyPattern: ownerNs + ":*", Paths: []string{"age"}}
	suite.NoError(suite.db.writeIndexSpec(idxV1))
	regV1, ok := suite.db.idxRegSpec[idxV1.fullName()]
	suite.True(ok)
	suite.NotEmpty(regV1.Version)
	v1hex := regV1.Version
	suite.False(regV1.CreatedAt.IsZero())
	suite.True(regV1.UpdatedAt.IsZero())

	docs := []string{
		`{"id":"a","age":10,"score":900}`,
		`{"id":"b","age":30,"score":100}`,
		`{"id":"c","age":20,"score":500}`,
	}
	for _, d := range docs {
		suite.NoError(suite.db.Set(ownerNs+":"+gjson.Get(d, "id").String(), d))
	}

	resV1 := suite.db.SearchIndex(idxV1.fullName(), x.KeysPattern(ownerNs+":*"), nil, false)
	suite.True(resV1.IsOk())
	gotV1 := resV1.MustGet()
	suite.Len(gotV1, 3)
	suite.Equal([]int64{10, 20, 30}, []int64{
		gjson.Get(gotV1[0], "age").Int(),
		gjson.Get(gotV1[1], "age").Int(),
		gjson.Get(gotV1[2], "age").Int(),
	}, "V1 index must order rows by age ASC")

	idxV2 := idxSpec{OwnerNs: ownerNs, Logical: "age", KeyPattern: ownerNs + ":*", Paths: []string{"score"}}
	suite.NoError(suite.db.writeIndexSpec(idxV2))
	regV2, ok := suite.db.idxRegSpec[idxV2.fullName()]
	suite.True(ok)
	suite.NotEqual(v1hex, regV2.Version, "paths change must produce different MD5 version")
	suite.Equal(regV1.CreatedAt, regV2.CreatedAt, "CreatedAt immutable across index versions")
	suite.False(regV2.UpdatedAt.IsZero(), "UpdatedAt must refresh on version change")

	resV2 := suite.db.SearchIndex(idxV2.fullName(), x.KeysPattern(ownerNs+":*"), nil, false)
	suite.True(resV2.IsOk())
	gotV2 := resV2.MustGet()
	suite.Len(gotV2, 3)
	suite.Equal([]int64{100, 500, 900}, []int64{
		gjson.Get(gotV2[0], "score").Int(),
		gjson.Get(gotV2[1], "score").Int(),
		gjson.Get(gotV2[2], "score").Int(),
	}, "V2 index must order rows by score ASC after DropIndex+CreateIndex remount")

	var metaKeys []string
	suite.NoError(suite.db.raw().View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(naming.IdxMetaGlobFor(idxV1.fullName()), func(k, v string) bool {
			metaKeys = append(metaKeys, k)
			return true
		})
	}))
	suite.Len(metaKeys, 1, "disk invariant: exactly 1 _idx_:*:v_* key per logical index after 2 versions; got: %v", metaKeys)
}

func (suite *DBSuite) TestSingleMetaVersionPerLogicalOnDiskInvariant() {
	storageNs := naming.BuildStorageNs("invariant", false)
	writeDoc := func(ttl time.Duration, keyAttrs []string) {
		suite.T().Helper()
		spec := docSpec{Namespace: "invariant", Mem: false, KeyAttrs: keyAttrs, TTL: ttl}
		suite.NoError(suite.db.writeDocSpec(spec))
	}
	countDocMeta := func() int {
		suite.T().Helper()
		var n int
		suite.NoError(suite.db.raw().View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(naming.DocMetaGlobFor(storageNs), func(k, v string) bool {
				n++
				return true
			})
		}))
		return n
	}

	writeDoc(time.Hour, []string{"id"})
	suite.Equal(1, countDocMeta(), "doc V1: 1 meta key on disk")
	writeDoc(2*time.Hour, []string{"id"})
	suite.Equal(1, countDocMeta(), "doc V2: old meta key deleted, 1 meta key on disk")
	writeDoc(2*time.Hour, []string{"tenant", "id"})
	suite.Equal(1, countDocMeta(), "doc V3: keyattrs change, 1 meta key on disk")
	writeDoc(2*time.Hour, []string{"tenant", "id"})
	suite.Equal(1, countDocMeta(), "doc V3 noop: still 1 meta key")

	writeDocZero := docSpec{Namespace: "invariant", Mem: true, KeyAttrs: []string{"sid"}, TTL: 0}
	suite.NoError(suite.db.writeDocSpec(writeDocZero))
	memStorageNs := naming.BuildStorageNs("invariant", true)
	var memMetaCount int
	suite.NoError(suite.db.rawMem().View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(naming.DocMetaGlobFor(memStorageNs), func(k, v string) bool {
			memMetaCount++
			return true
		})
	}))
	suite.Equal(1, memMetaCount, "mem layer doc meta also single-versioned; symmetric storage check")

	idxFull := naming.BuildIdxFullName(storageNs, "sort")
	writeIdx := func(paths []string) {
		suite.T().Helper()
		spec := idxSpec{OwnerNs: storageNs, Logical: "sort", KeyPattern: storageNs + ":*", Paths: paths}
		suite.NoError(suite.db.writeIndexSpec(spec))
	}
	countIdxMeta := func() int {
		suite.T().Helper()
		var n int
		suite.NoError(suite.db.raw().View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(naming.IdxMetaGlobFor(idxFull), func(k, v string) bool {
				n++
				return true
			})
		}))
		return n
	}

	writeIdx([]string{"a"})
	suite.Equal(1, countIdxMeta(), "idx V1: 1 meta key")
	writeIdx([]string{"a", "b"})
	suite.Equal(1, countIdxMeta(), "idx V2: add path, old meta deleted → 1 meta key")
	writeIdx([]string{"c"})
	suite.Equal(1, countIdxMeta(), "idx V3: change path entirely → 1 meta key")
	writeIdx([]string{"c"})
	suite.Equal(1, countIdxMeta(), "idx V3 noop: still 1 meta key")
}

func requireTTLPositive(t *testing.T, db *DB, key string) {
	t.Helper()

	var ttl time.Duration
	layer, constrained, err := resolveLayer(key)
	require.NoError(t, err)
	require.True(t, constrained, "requireTTLPositive: key %q is not a concrete key", key)
	err = db.store(layer).View(func(tx *buntdb.Tx) error {
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
		kr       x.KeyRange
		filter   x.Filter
		updates  []x.Mutation
		wantKeys []string
	}{
		{
			name:     "Update existing property",
			kr:       x.KeysPattern("user:*"),
			filter:   x.Eq("id", "1"),
			updates:  []x.Mutation{x.Set("age", 21)},
			wantKeys: []string{"user:1"},
		},
		{
			name:     "Add new property",
			kr:       x.KeysPattern("user:*"),
			filter:   x.Eq("id", "2"),
			updates:  []x.Mutation{x.Set("active", true)},
			wantKeys: []string{"user:2"},
		},
		{
			name:     "Update multiple documents",
			kr:       x.KeysPattern("user:*"),
			filter:   x.Gt("age", float64(24)),
			updates:  []x.Mutation{x.Set("status", "verified")},
			wantKeys: []string{"user:2", "user:3"},
		},
		{
			name:     "Update without filter applies to all",
			kr:       x.KeysPattern("user:*"),
			filter:   nil,
			updates:  []x.Mutation{x.Set("version", 2)},
			wantKeys: []string{"user:1", "user:2", "user:3"},
		},
		{
			name:   "Update all data types",
			kr:     x.KeysPattern("user:*"),
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
		{
			name:     "KeysBt half-open range excludes upper bound",
			kr:       x.KeysBt("user:1", "user:3"),
			filter:   nil,
			updates:  []x.Mutation{x.Set("ranged", "bt")},
			wantKeys: []string{"user:1", "user:2"},
		},
		{
			name:     "KeysLte range includes equal",
			kr:       x.KeysLte("user:2"),
			filter:   nil,
			updates:  []x.Mutation{x.Set("ranged", "lte")},
			wantKeys: []string{"user:1", "user:2"},
		},
		{
			name:     "Limit(1) updates only first match",
			kr:       x.KeysPattern("user:*").Limit(1),
			filter:   nil,
			updates:  []x.Mutation{x.Set("capped", true)},
			wantKeys: []string{"user:1"},
		},
		{
			name:     "KeysGt + Limit(1)",
			kr:       x.KeysGt("user:1").Limit(1),
			filter:   nil,
			updates:  []x.Mutation{x.Set("capped", true)},
			wantKeys: []string{"user:2"},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			suite.TearDownTest()
			suite.SetupTest()

			res := suite.db.Update(tt.kr, tt.filter, tt.updates...)
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
	t.Run("rejects nil key range", func(t *testing.T) {
		db := openDB(testutil.DBPath(t))
		require.NotNil(t, db)
		t.Cleanup(func() { _ = db.Close() })

		res := db.Update(nil, nil, x.Set("status", "active"))
		require.Error(t, res.Error())
		require.Contains(t, res.Error().Error(), "key range is required")
	})

	t.Run("rejects leading wildcard pattern", func(t *testing.T) {
		db := openDB(testutil.DBPath(t))
		require.NotNil(t, db)
		t.Cleanup(func() { _ = db.Close() })

		res := db.Update(x.KeysPattern("*user:*"), nil, x.Set("status", "active"))
		require.Error(t, res.Error())
		require.Contains(t, res.Error().Error(), "cannot start with wildcard")
	})

	t.Run("returns empty when no documents match", func(t *testing.T) {
		db := openDB(testutil.DBPath(t))
		require.NotNil(t, db)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, db.Set("user:1", `{"id":"1","status":"pending"}`))

		res := db.Update(x.KeysPattern("user:*"), x.Eq("status", "active"), x.Set("status", "reviewed"))
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
		res := db.Update(x.KeysPattern("user:*"), x.Eq("id", "1"), x.Set("status", "active"))
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

		res := db.Update(x.KeysPattern("user:*"), x.Eq("id", "1"), x.Set("status", "active"))
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

		res := db.Update(x.KeysPattern("user:*"), nil, x.Set("", "active"))
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
		key := naming.BuildStorageKey("user", naming.JoinPKAttrValues([]string{department, id}))
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
			kr:        x.KeysPattern("user:Engineering_*"),
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
		{
			name:      "Query unknown non-empty index → index not found error",
			indexName: "nonexistent_idx_zzz",
			kr:        x.KeysPattern("user:*"),
			wantErr:   true,
		},
		{
			name:      "Query unanchored wildcard KeyRange → rejected with cannot start with wildcard",
			indexName: x.Idx[testUserDoc]("age", "*", "age").Name(),
			kr:        x.KeysPattern("*user:*"),
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
			kr:        x.KeysPattern("user:Engineering_*"),
			filter:    nil,
			desc:      false,
			wantEmpty: false,
			wantLen:   3,
		},
		{
			name:      "QueryKey Engineering department descending",
			kr:        x.KeysPattern("user:Engineering_*"),
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
			kr:        x.KeysPattern("user:Marketing_*"),
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

const (
	searchKRFixtureNamespace = "probeserver"
	updateKRFixtureNamespace = "updserver000"
)

type SearchFixtureDoc string

func (SearchFixtureDoc) Namespace() string  { return searchKRFixtureNamespace }
func (SearchFixtureDoc) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (SearchFixtureDoc) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d SearchFixtureDoc) RawJSON() string  { return string(d) }
func (SearchFixtureDoc) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

type UpdateFixtureDoc string

func (UpdateFixtureDoc) Namespace() string  { return updateKRFixtureNamespace }
func (UpdateFixtureDoc) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (UpdateFixtureDoc) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d UpdateFixtureDoc) RawJSON() string  { return string(d) }
func (UpdateFixtureDoc) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

func krID(id string) string {
	return testutil.XIDKey(searchKRFixtureNamespace, testutil.KeyRangeFixtureMem(), id)
}

func updID(id string) string {
	return testutil.XIDKey(updateKRFixtureNamespace, testutil.KeyRangeFixtureMem(), id)
}

type SearchKeyRangeSuite struct {
	suite.Suite
	db            *DB
	idxScoreName  string
	idxBucketName string
	idxSparseName string
}

func (s *SearchKeyRangeSuite) SetupTest() {
	s.db = openDB(testutil.DBPath(s.T()))
	s.Require().NotNil(s.db)

	indexes := testutil.KeyRangeRawIndexes(searchKRFixtureNamespace, testutil.KeyRangeFixtureMem())
	s.idxScoreName = indexes[0].Name()
	s.idxBucketName = indexes[1].Name()
	s.idxSparseName = indexes[2].Name()
	s.Require().NoError(s.db.registerIndexes(indexes...))

	for _, kv := range testutil.LoadXFor(s.T(), searchKRFixtureNamespace, testutil.KeyRangeFixtureMem()) {
		s.Require().NoError(s.db.Set(kv.K, kv.V))
	}
}

func (s *SearchKeyRangeSuite) TearDownTest() {
	if s.db != nil {
		_ = s.db.Close()
	}
}

func (s *SearchKeyRangeSuite) TestSeedCountIs100() {
	kr := x.KeysPattern(krID("*"))
	got := s.db.SearchKey(kr, nil, false)
	s.True(got.IsOk(), "SearchKey err: %v", got.Error())
	s.Len(got.MustGet(), testutil.CountX())
}

func (s *SearchKeyRangeSuite) TestNoCrossContamination() {
	krDisk := x.KeysPattern("user:*")
	resDisk := s.db.SearchKey(krDisk, nil, false)
	s.True(resDisk.IsOk())
	s.Empty(resDisk.MustGet())

	krWrong := x.KeysPattern("probe:*")
	resWrong := s.db.SearchKey(krWrong, nil, false)
	s.True(resWrong.IsOk())
	s.Empty(resWrong.MustGet())
}

func (s *SearchKeyRangeSuite) TestAllKeyRangeCtorShapes_TABLE_DRIVEN() {
	run := func(kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := s.db.SearchKey(kr, nil, desc)
		if !res.IsOk() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(res.MustGet()), true, ""
	}
	testutil.AssertSearchKeyMatrix(s.T(), run, testutil.KeyRangeCtorCases(), krID, "SK/")
}

func (s *SearchKeyRangeSuite) TestGtGteBoundaryGapEqualsOne() {
	resGte := s.db.SearchKey(x.KeysGte(krID("p027")), nil, false)
	s.Require().True(resGte.IsOk())
	idsGte := testutil.XIDsFromValues(resGte.MustGet())

	resGt := s.db.SearchKey(x.KeysGt(krID("p027")), nil, false)
	s.Require().True(resGt.IsOk())
	idsGt := testutil.XIDsFromValues(resGt.MustGet())

	testutil.AssertGtGteGap1(s.T(), idsGte, idsGt, "p027")
}

func (s *SearchKeyRangeSuite) TestLtLteBoundaryGapEqualsOne() {
	resLte := s.db.SearchKey(x.KeysLte(krID("p072")), nil, false)
	s.Require().True(resLte.IsOk())
	idsLte := testutil.XIDsFromValues(resLte.MustGet())

	resLt := s.db.SearchKey(x.KeysLt(krID("p072")), nil, false)
	s.Require().True(resLt.IsOk())
	idsLt := testutil.XIDsFromValues(resLt.MustGet())

	testutil.AssertLtLteGap1(s.T(), idsLte, idsLt, "p072")
}

func (s *SearchKeyRangeSuite) TestSIScoreSeedCountIs100() {
	kr := x.KeysPattern(krID("*"))
	got := s.db.SearchIndex(s.idxScoreName, kr, nil, false)
	s.True(got.IsOk(), "SearchIndex score ASC err: %v", got.Error())
	s.Len(got.MustGet(), testutil.CountX())
}

func (s *SearchKeyRangeSuite) TestSIScoreOrderingMatchesSKIdOrder() {
	krAll := x.KeysPattern(krID("*"))

	siAsc := s.db.SearchIndex(s.idxScoreName, krAll, nil, false)
	s.Require().True(siAsc.IsOk())
	siIdsAsc := testutil.XIDsFromValues(siAsc.MustGet())

	skAsc := s.db.SearchKey(krAll, nil, false)
	s.Require().True(skAsc.IsOk())
	skIdsAsc := testutil.XIDsFromValues(skAsc.MustGet())

	siDesc := s.db.SearchIndex(s.idxScoreName, krAll, nil, true)
	s.Require().True(siDesc.IsOk())
	siIdsDesc := testutil.XIDsFromValues(siDesc.MustGet())

	skDesc := s.db.SearchKey(krAll, nil, true)
	s.Require().True(skDesc.IsOk())
	skIdsDesc := testutil.XIDsFromValues(skDesc.MustGet())

	testutil.AssertScoreEqSKId(s.T(), siIdsAsc, skIdsAsc, siIdsDesc, skIdsDesc)
}

func (s *SearchKeyRangeSuite) TestSIAllKeyRangeCtorShapes_TABLE_DRIVEN() {
	run := func(idxName string, kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := s.db.SearchIndex(idxName, kr, nil, desc)
		if !res.IsOk() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(res.MustGet()), true, ""
	}
	testutil.AssertSearchIndexMatrix(s.T(), run, s.idxScoreName, testutil.KeyRangeCtorCases(), krID, "idx=score/")
}

func (s *SearchKeyRangeSuite) TestSIBucketTiebreakersLexicographicById() {
	krAll := x.KeysPattern(krID("*"))
	resA := s.db.SearchIndex(s.idxBucketName, krAll, x.Eq("bucket", "A"), false)
	s.Require().True(resA.IsOk())
	idsA := testutil.XIDsFromValues(resA.MustGet())

	resC := s.db.SearchIndex(s.idxBucketName, krAll, x.Eq("bucket", "C"), false)
	s.Require().True(resC.IsOk())
	idsC := testutil.XIDsFromValues(resC.MustGet())

	resAll := s.db.SearchIndex(s.idxBucketName, krAll, nil, false)
	s.Require().True(resAll.IsOk())
	allIDs := testutil.XIDsFromValues(resAll.MustGet())

	testutil.AssertBucketDistribution(s.T(), idsA, idsC, allIDs)
}

func (s *SearchKeyRangeSuite) TestSISparseAmtLimit10() {
	krLimit := x.KeysPattern(krID("*")).Limit(10)
	si := s.db.SearchIndex(s.idxSparseName, krLimit, nil, false)
	s.Require().True(si.IsOk())
	testutil.AssertSparseLimit10(s.T(), testutil.XIDsFromValues(si.MustGet()))
}

func (s *SearchKeyRangeSuite) TestSICrossLayerMismatchRejects() {
	krDisk := x.KeysPattern("user:*")
	res := s.db.SearchIndex(s.idxScoreName, krDisk, nil, false)
	s.Require().True(res.IsError(), "got Ok len=%d", len(res.OrEmpty()))
	s.Contains(res.Error().Error(), "different storage layer")
}

func TestSearchKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(SearchKeyRangeSuite))
}

type UpdateKeyRangeSuite struct {
	suite.Suite
	db *DB
}

func (s *UpdateKeyRangeSuite) SetupTest() {
	s.db = openDB(testutil.DBPath(s.T()))
	s.Require().NotNil(s.db)
	for _, kv := range testutil.LoadXFor(s.T(), updateKRFixtureNamespace, testutil.KeyRangeFixtureMem()) {
		s.Require().NoError(s.db.Set(kv.K, kv.V))
	}
}

func (s *UpdateKeyRangeSuite) TearDownTest() {
	if s.db != nil {
		_ = s.db.Close()
	}
}

func (s *UpdateKeyRangeSuite) TestSeedCountMatchesSearchKey() {
	allKr := x.KeysPattern(updID("*"))
	skRes := s.db.SearchKey(allKr, nil, false)
	s.Require().True(skRes.IsOk())
	s.Len(skRes.MustGet(), testutil.CountX(), "UpdateKR seed count=%d should equal SearchKey fixture", len(skRes.MustGet()))
}

func (s *UpdateKeyRangeSuite) TestNoCrossContamination() {
	resWrong := s.db.Update(x.KeysPattern(naming.StorageNsKeyPattern(naming.BuildStorageNs(searchKRFixtureNamespace, testutil.KeyRangeFixtureMem()))), nil, x.Set("tag_contam", true))
	s.True(resWrong.IsOk())
	s.Empty(resWrong.MustGet(), "cross-contam probe-server prefix should hit zero keys")

	resUser := s.db.Update(x.KeysPattern("user:*"), nil, x.Set("tag_contam", true))
	s.True(resUser.IsOk())
	s.Empty(resUser.MustGet(), "cross-contam user:* should hit zero keys")

	skAll := s.db.SearchKey(x.KeysPattern(updID("*")), nil, false)
	s.Require().True(skAll.IsOk())
	for _, v := range skAll.MustGet() {
		got := updRawGet(v, "tag_contam")
		s.NotEqual("true", got, "tag_contam leaked to fixture data; key=ctor_shape=%q raw=%s", updRawGet(v, "ctor_shape"), v)
	}
}

func (s *UpdateKeyRangeSuite) TestBulkSetAllTagThenVerifyViaSearchKey() {
	allKr := x.KeysPattern(updID("*"))
	res := s.db.Update(allKr, nil, x.Set("update_tagged", "bulk_all"))
	s.Require().True(res.IsOk(), "Update bulk_all err: %v", res.Error())
	keys := res.MustGet()
	s.Len(keys, testutil.CountX(), "Update all expected count=%d got=%d", testutil.CountX(), len(keys))
	sort.Strings(keys)

	skAfter := s.db.SearchKey(allKr, nil, false)
	s.Require().True(skAfter.IsOk())
	after := skAfter.MustGet()
	s.Len(after, testutil.CountX())
	for _, v := range after {
		s.Equal("bulk_all", updRawGet(v, "update_tagged"),
			"Update bulk_all: every value should carry update_tagged=bulk_all; raw=%s", v)
	}
}

func (s *UpdateKeyRangeSuite) TestUpdateAllKeyRangeCtorShapes_TABLE_DRIVEN() {
	epoch := 0
	nextTag := func() string {
		epoch++
		return fmt.Sprintf("e%d", epoch)
	}
	runAsc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := s.db.Update(kr, nil, x.Set("ctor_shape", tag))
		if !res.IsOk() {
			return nil, false, res.Error().Error()
		}
		return updIDFromStorage(res.MustGet()), true, ""
	}
	runDesc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := s.db.Update(kr, nil, x.Set("ctor_shape", tag))
		if !res.IsOk() {
			return nil, false, res.Error().Error()
		}
		ids := updIDFromStorage(res.MustGet())
		for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
			ids[i], ids[j] = ids[j], ids[i]
		}
		return ids, true, ""
	}
	assertCtorShapeWritten := func(caseName, label string, wantCount int, verifyRange x.KeyRange, wantTag string) {
		s.T().Helper()
		skRes := s.db.SearchKey(verifyRange, nil, false)
		s.Require().True(skRes.IsOk(), "%s/%s: SearchKey after Update err: %v", caseName, label, skRes.Error())
		values := skRes.MustGet()
		var count int
		for _, v := range values {
			if updRawGet(v, "ctor_shape") == wantTag {
				count++
			}
		}
		s.Equal(wantCount, count,
			"%s/%s: ctor_shape=%q written count mismatch want=%d got=%d (SearchKey range len=%d)",
			caseName, label, wantTag, wantCount, count, len(values))
	}
	for _, tc := range testutil.KeyRangeCtorCases() {
		tc := tc
		kr := tc.Build(updID)
		fullCase := "UpdateKR/" + tc.Name

		tag := nextTag()
		ids, ok, errMsg := runAsc(kr, tag)
		assertKRResult(s.T(), fullCase, "ASC_no_limit", tc.WantAsc, ids, ok, errMsg, false)
		if ok && len(ids) > 0 {
			assertCtorShapeWritten(fullCase, "ASC_no_limit", len(ids), kr, tag)
		}

		tag = nextTag()
		ids, ok, errMsg = runDesc(kr, tag)
		assertKRResult(s.T(), fullCase, "DESC_no_limit", tc.WantAsc, ids, ok, errMsg, true)
		if ok && len(tc.WantAsc) > 0 {
			assertCtorShapeWritten(fullCase, "DESC_no_limit", len(tc.WantAsc), kr, tag)
		}

		if len(tc.WantAsc) >= 5 {
			limit5Asc := tc.WantAsc[:5]
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(5), tag)
			assertKRResult(s.T(), fullCase, "ASC_Limit_5_is_first_5", limit5Asc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_5", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runDesc(kr.Limit(5), tag)
			assertKRResult(s.T(), fullCase, "DESC_Limit_5_is_last_5_rev", limit5Asc, ids, ok, errMsg, true)
			if ok && len(limit5Asc) > 0 {
				assertCtorShapeWritten(fullCase, "DESC_Limit_5", 5, kr, tag)
			}
		}
		if len(tc.WantAsc) >= 3 {
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)), tag)
			assertKRResult(s.T(), fullCase, "ASC_Limit_EQ_count_returns_all", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_EQ_count", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)+500), tag)
			assertKRResult(s.T(), fullCase, "ASC_Limit_OVER_count_safe", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_OVER_count", len(ids), kr, tag)
			}
		}
	}
}

func assertKRResult(t *testing.T, caseName, label string, wantAsc, ids []string, ok bool, errMsg string, desc bool) {
	t.Helper()
	if !ok {
		t.Errorf("%s/%s: expected Ok, got Error: %s", caseName, label, errMsg)
		return
	}
	if len(wantAsc) != len(ids) {
		t.Errorf("%s/%s: length mismatch want=%d got=%d ids=%v", caseName, label, len(wantAsc), len(ids), ids)
		return
	}
	var want []string
	if desc {
		want = make([]string, len(wantAsc))
		copy(want, wantAsc)
		for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
			want[i], want[j] = want[j], want[i]
		}
	} else {
		want = wantAsc
	}
	if len(want) == 0 && len(ids) == 0 {
		return
	}
	if len(ids) > 1 {
		if desc {
			for i := 1; i < len(ids); i++ {
				if ids[i-1] <= ids[i] {
					t.Errorf("%s/%s DESC not strictly decreasing: ids[%d]=%q ids[%d]=%q",
						caseName, label, i-1, ids[i-1], i, ids[i])
					break
				}
			}
		} else {
			for i := 1; i < len(ids); i++ {
				if ids[i-1] >= ids[i] {
					t.Errorf("%s/%s ASC not strictly increasing: ids[%d]=%q ids[%d]=%q",
						caseName, label, i-1, ids[i-1], i, ids[i])
					break
				}
			}
		}
	}
	if len(want) != len(ids) {
		return
	}
	for i := range want {
		if want[i] != ids[i] {
			t.Errorf("%s/%s content mismatch (desc=%v): want[%d]=%q got[%d]=%q", caseName, label, desc, i, want[i], i, ids[i])
			return
		}
	}
}

func (s *UpdateKeyRangeSuite) TestGtGteBoundaryGapEqualsOne() {
	krGte := x.KeysGte(updID("p027"))
	resGte := s.db.Update(krGte, nil, x.Set("boundary", "gte"))
	s.Require().True(resGte.IsOk())
	idsGte := updIDFromStorage(resGte.MustGet())

	skGte := s.db.SearchKey(krGte, nil, false)
	s.Require().True(skGte.IsOk())
	gotGte := skGte.MustGet()
	s.Len(gotGte, len(idsGte), "Gte SK sweep after Update expected len=%d got=%d", len(idsGte), len(gotGte))
	for _, v := range gotGte {
		got := updRawGet(v, "boundary")
		s.Equal("gte", got, "Update Gte value mismatch on boundary field: raw=%s", v)
	}

	krGt := x.KeysGt(updID("p027"))
	resGt := s.db.Update(krGt, nil, x.Set("boundary", "gt"))
	s.Require().True(resGt.IsOk())
	idsGt := updIDFromStorage(resGt.MustGet())

	skGt := s.db.SearchKey(krGt, nil, false)
	s.Require().True(skGt.IsOk())
	gotGt := skGt.MustGet()
	s.Len(gotGt, len(idsGt), "Gt SK sweep after Update expected len=%d got=%d", len(idsGt), len(gotGt))
	for _, v := range gotGt {
		got := updRawGet(v, "boundary")
		s.Equal("gt", got, "Update Gt value mismatch on boundary field: raw=%s", v)
	}

	testutil.AssertGtGteGap1(s.T(), idsGte, idsGt, "p027")
}

func (s *UpdateKeyRangeSuite) TestLtLteBoundaryGapEqualsOne() {
	krLte := x.KeysLte(updID("p072"))
	resLte := s.db.Update(krLte, nil, x.Set("boundary", "lte"))
	s.Require().True(resLte.IsOk())
	idsLte := updIDFromStorage(resLte.MustGet())

	skLte := s.db.SearchKey(krLte, nil, false)
	s.Require().True(skLte.IsOk())
	gotLte := skLte.MustGet()
	s.Len(gotLte, len(idsLte), "Lte SK sweep after Update expected len=%d got=%d", len(idsLte), len(gotLte))
	for _, v := range gotLte {
		got := updRawGet(v, "boundary")
		s.Equal("lte", got, "Update Lte value mismatch on boundary field: raw=%s", v)
	}

	krLt := x.KeysLt(updID("p072"))
	resLt := s.db.Update(krLt, nil, x.Set("boundary", "lt"))
	s.Require().True(resLt.IsOk())
	idsLt := updIDFromStorage(resLt.MustGet())

	skLt := s.db.SearchKey(krLt, nil, false)
	s.Require().True(skLt.IsOk())
	gotLt := skLt.MustGet()
	s.Len(gotLt, len(idsLt), "Lt SK sweep after Update expected len=%d got=%d", len(idsLt), len(gotLt))
	for _, v := range gotLt {
		got := updRawGet(v, "boundary")
		s.Equal("lt", got, "Update Lt value mismatch on boundary field: raw=%s", v)
	}

	testutil.AssertLtLteGap1(s.T(), idsLte, idsLt, "p072")
}

func (s *UpdateKeyRangeSuite) TestLimit7PrefixEqualFullSet() {
	allKr := x.KeysPattern(updID("*"))

	fullRes := s.db.Update(allKr, nil, x.Set("lim", "full"))
	s.Require().True(fullRes.IsOk(), "full err=%v", fullRes.Error())
	full := fullRes.MustGet()
	s.Len(full, testutil.CountX())
	sort.Strings(full)
	skFull := s.db.SearchKey(allKr, nil, false)
	s.Require().True(skFull.IsOk())
	gotFull := skFull.MustGet()
	s.Len(gotFull, testutil.CountX())
	for _, v := range gotFull {
		got := updRawGet(v, "lim")
		s.Equal("full", got, "Update lim=full value mismatch: raw=%s", v)
	}

	limitRes := s.db.Update(x.KeysPattern(updID("*")).Limit(7), nil, x.Set("lim", "7"))
	s.Require().True(limitRes.IsOk(), "limit err=%v", limitRes.Error())
	lim := limitRes.MustGet()
	s.Len(lim, 7, "Limit(7) must truncate at callback=7, got len=%d", len(lim))
	sort.Strings(lim)
	s.Equal(full[:7], lim, "Limit(7) updated keys should equal ASC first-7 of full updated set — proves Limit is callback early-stop not post-hoc slice")
	skLim := s.db.SearchKey(allKr, nil, false)
	s.Require().True(skLim.IsOk())
	gotLim := skLim.MustGet()
	var cntLim7 int
	for _, v := range gotLim {
		got := updRawGet(v, "lim")
		if got == "7" {
			cntLim7++
			continue
		}
		s.Equal("full", got, "Limit=7 sweep: non-first-7 docs must keep lim=full, got %q; raw=%s", got, v)
	}
	s.Equal(7, cntLim7, "lim=7 want 7 docs with exact value lim==7 got=%d", cntLim7)
}

func (s *UpdateKeyRangeSuite) TestFilterUpdatesOnlyMatched() {
	filter := x.Eq("bucket", "A")
	res := s.db.Update(x.KeysPattern(updID("*")), filter, x.Set("filtered_tag", "A-only"))
	s.Require().True(res.IsOk(), "filtered err=%v", res.Error())
	ids := updIDFromStorage(res.MustGet())
	s.Len(ids, 34, "Update+filter Eq(bucket,A) should match 34 bucket=A rows (probe fixture distribution)")

	skAll := s.db.SearchKey(x.KeysPattern(updID("*")), nil, false)
	s.Require().True(skAll.IsOk())
	var count int
	for _, v := range skAll.MustGet() {
		if updRawGet(v, "filtered_tag") == "A-only" {
			count++
		}
	}
	s.Equal(len(ids), count, "only updated count rows have filtered_tag; rows=%d", count)
}

func (s *UpdateKeyRangeSuite) TestNilKRRejects() {
	res := s.db.Update(nil, nil, x.Set("nil_tag", true))
	s.Require().True(res.IsError(), "nil kr must reject")
	s.Contains(res.Error().Error(), "key range is required")
}

func updIDPrefix() string {
	return testutil.XKeyPrefix(updateKRFixtureNamespace, testutil.KeyRangeFixtureMem())
}

func updRawGet(raw, path string) string { return gjson.Get(raw, path).String() }

func updIDFromStorage(storageKeys []string) []string {
	prefix := updIDPrefix()
	out := make([]string, 0, len(storageKeys))
	for _, k := range storageKeys {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k[len(prefix):])
			continue
		}
		out = append(out, "")
	}
	return out
}

func TestUpdateKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(UpdateKeyRangeSuite))
}

// ─── EffectiveAuthKeys ───

func TestEffectiveAuthKeys(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T) *DB
		wantApp   string
		wantCtrl  string
	}{
		{
			name: "nil DB returns empty",
			setup: func(t *testing.T) *DB {
				return nil
			},
			wantApp:  "",
			wantCtrl: "",
		},
		{
			name: "empty DB returns empty",
			setup: func(t *testing.T) *DB {
				db := openDB(testutil.DBPath(t))
				require.NotNil(t, db)
				t.Cleanup(func() { _ = db.Close() })
				return db
			},
			wantApp:  "",
			wantCtrl: "",
		},
		{
			name: "after bootstrap returns keys",
			setup: func(t *testing.T) *DB {
				db := openDB(testutil.DBPath(t))
				require.NotNil(t, db)
				t.Cleanup(func() { _ = db.Close() })
				_, _, _, err := db.bootstrapAuth("test-app", "test-ctrl")
				require.NoError(t, err)
				return db
			},
			wantApp:  "test-app",
			wantCtrl: "test-ctrl",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.setup(t)
			app, ctrl := db.EffectiveAuthKeys()
			require.Equal(t, tt.wantApp, app)
			require.Equal(t, tt.wantCtrl, ctrl)
		})
	}
}

// ─── registerSchemas ───

func TestRegisterSchemas(t *testing.T) {
	db := openDB(testutil.DBPath(t))
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()

	tests := []struct {
		name    string
		schemas []x.Schema
		wantErr bool
	}{
		{
			name:    "single schema",
			schemas: []x.Schema{testUserDoc("{}")},
			wantErr: false,
		},
		{
			name:    "re-register same schema is idempotent",
			schemas: []x.Schema{testUserDoc("{}")},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.registerSchemas(tt.schemas...)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}

	// Verify schema was actually registered
	db.docRegMu.Lock()
	_, ok := db.docRegSpec[naming.BuildStorageNs("user", false)]
	db.docRegMu.Unlock()
	require.True(t, ok, "user schema should be registered")
}

// ─── generateBootstrapAuth ───

func TestGenerateBootstrapAuth(t *testing.T) {
	app, ctrl, err := generateBootstrapAuth()
	require.NoError(t, err)
	require.Len(t, app, 32, "app should be 32-char hex (128-bit)")
	require.Len(t, ctrl, 32, "ctrl should be 32-char hex (128-bit)")
	require.NotEqual(t, app, ctrl, "app and ctrl should be distinct")

	// Multiple calls should produce different values
	app2, ctrl2, err2 := generateBootstrapAuth()
	require.NoError(t, err2)
	require.NotEqual(t, app, app2)
	require.NotEqual(t, ctrl, ctrl2)
}

// ─── openDB edge cases ───

func TestOpenDB_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantNil bool
	}{
		{name: "empty path", path: "", wantNil: true},
		{name: ":memory: reserved", path: ":memory:", wantNil: true},
		{name: "directory path", path: t.TempDir(), wantNil: true},
		{name: "valid file path", path: testutil.DBPath(t), wantNil: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openDB(tt.path)
			if tt.wantNil {
				require.Nil(t, db)
			} else {
				require.NotNil(t, db)
				_ = db.Close()
			}
		})
	}
}

// ─── idxSpec.UnmarshalJSON ───

func TestIdxSpecUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		wantPath []string
		wantErr  bool
	}{
		{
			name:     "new paths field",
			json:     `{"owner_ns":"user","logical":"age","paths":["$.age","$.name"]}`,
			wantPath: []string{"$.age", "$.name"},
		},
		{
			name:     "legacy single path field",
			json:     `{"owner_ns":"user","logical":"age","path":"$.age"}`,
			wantPath: []string{"$.age"},
		},
		{
			name:     "both fields paths wins",
			json:     `{"owner_ns":"user","logical":"age","paths":["$.a"],"path":"$.b"}`,
			wantPath: []string{"$.a"},
		},
		{
			name:    "invalid json",
			json:    `{invalid`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var idx idxSpec
			err := json.Unmarshal([]byte(tt.json), &idx)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantPath, idx.Paths)
		})
	}
}

// ─── applyPatternRange ───

func TestApplyPatternRange(t *testing.T) {
	db := openDB(testutil.DBPath(t))
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()

	// Seed data
	keys := []string{"user:1", "user:2", "user:3", "order:1", "order:2"}
	for _, k := range keys {
		require.NoError(t, db.Set(k, `{"k":"`+k+`"}`))
	}

	tests := []struct {
		name    string
		pattern string
		dir     x.RangeDirection
		wantLen int
	}{
		{name: "star matches all ASC", pattern: "*", dir: x.RangeAsc, wantLen: 5},
		{name: "star matches all DESC", pattern: "*", dir: x.RangeDesc, wantLen: 5},
		{name: "prefix wildcard ASC", pattern: "user:*", dir: x.RangeAsc, wantLen: 3},
		{name: "prefix wildcard DESC", pattern: "user:*", dir: x.RangeDesc, wantLen: 3},
		{name: "leading wildcard ASC", pattern: "*:1", dir: x.RangeAsc, wantLen: 2},
		{name: "leading wildcard DESC", pattern: "*:1", dir: x.RangeDesc, wantLen: 2},
		{name: "empty pattern", pattern: "", dir: x.RangeAsc, wantLen: 0},
		{name: "no match pattern", pattern: "zzz:*", dir: x.RangeAsc, wantLen: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var collected []string
			err := db.disk.View(func(tx *buntdb.Tx) error {
				return applyPatternRange(tx, tt.pattern, 0, tt.dir, func(key, value string) bool {
					collected = append(collected, key)
					return true
				})
			})
			require.NoError(t, err)
			require.Len(t, collected, tt.wantLen)
		})
	}
}
