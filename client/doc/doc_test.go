package doc

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/kcmvp/redisx/client"
	"github.com/kcmvp/redisx/server"
	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/suite"
)

type DocTestSuite struct {
	suite.Suite
}

func TestDocSuite(t *testing.T) {
	suite.Run(t, new(DocTestSuite))
}

const docTestServerAddr = "127.0.0.1:36382"

func (s *DocTestSuite) SetupSuite() {
	s.T().Setenv("HOME", s.T().TempDir())
	dbPath := filepath.Join(s.T().TempDir(), "redisx.db")
	db := server.Start(
		docTestServerAddr,
		dbPath,
		x.Idx[UserDoc]("age", "*", "age"),
	)
	s.Require().NotNil(db)

	err := client.ConnectEmbed(docTestServerAddr)
	s.Require().NoError(err)

	for range 30 {
		res := client.Keys("probe*")
		err = res.Error()
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.T().Fatal("failed to connect to test server")
}

func (s *DocTestSuite) TearDownSuite() {
	client.Disconnect()
}

type UserDoc string

func (UserDoc) Namespace() string  { return "user" }
func (UserDoc) Mem() bool          { return false }
func (UserDoc) KeyAttrs() []string { return []string{"id"} }
func (u UserDoc) RawJSON() string  { return string(u) }
func (UserDoc) TTL() time.Duration { return time.Hour }

type ExpiringUserDoc string

func (ExpiringUserDoc) Namespace() string  { return "expuserclient" }
func (ExpiringUserDoc) Mem() bool          { return false }
func (ExpiringUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u ExpiringUserDoc) RawJSON() string  { return string(u) }
func (ExpiringUserDoc) TTL() time.Duration { return 40 * time.Millisecond }

func (s *DocTestSuite) TestGenericDocMethods() {
	jsonStr := `{"id":"200","name":"Test","age":30}`
	doc := UserDoc(jsonStr)

	err := Set(doc)
	s.Require().NoError(err)

	val, err := Get[UserDoc]("200")
	s.Require().NoError(err)
	s.Equal(UserDoc(jsonStr), val)

	ok, err := SetNX(doc)
	s.Require().NoError(err)
	s.False(ok)

	keysRes := Keys[UserDoc]("*")
	s.Require().NoError(keysRes.Error())
	s.Contains(keysRes.MustGet(), "user:200")

	searchRes := SearchKey[UserDoc]("*", x.Eq("age", 30), false)
	s.Require().NoError(searchRes.Error())
	s.Contains(searchRes.MustGet(), UserDoc(jsonStr))

	idxRes := SearchIndex[UserDoc]("age", "*", x.Eq("age", float64(30)), false)
	s.Require().NoError(idxRes.Error())
	s.Contains(idxRes.MustGet(), UserDoc(jsonStr))

	updRes := Update[UserDoc]("*", x.Eq("age", 30), x.Set("age", 31))
	s.Require().NoError(updRes.Error())

	del, err := Delete(UserDoc(`{"id":"200"}`))
	s.Require().NoError(err)
	s.True(del)
}

func (s *DocTestSuite) TestStorageKeyFromDocument() {
	doc := UserDoc(`{"id":"201","name":"Alice"}`)

	key, err := x.StorageKey(doc)
	s.Require().NoError(err)
	s.Equal("user:201", key)
}

func (s *DocTestSuite) TestTypedWritesRespectDocumentTTL() {
	first := ExpiringUserDoc(`{"id":"1","name":"alpha"}`)
	err := Set(first)
	s.Require().NoError(err)

	second := ExpiringUserDoc(`{"id":"2","name":"beta"}`)
	ok, err := SetNX(second)
	s.Require().NoError(err)
	s.True(ok)

	updRes := Update[ExpiringUserDoc]("*", x.Eq("id", "1"), x.Set("name", "updated"))
	s.Require().NoError(updRes.Error())

	time.Sleep(80 * time.Millisecond)

	_, err = Get[ExpiringUserDoc]("1")
	s.Require().Error(err)
	_, err = Get[ExpiringUserDoc]("2")
	s.Require().Error(err)
}

func (s *DocTestSuite) TestSearchIndexRejectsPrefixedStoragePattern() {
	res := SearchIndex[UserDoc]("age", "user:*", nil, false)
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "document-scoped")
}

func (s *DocTestSuite) TestSearchKeyRejectsPrefixedStoragePattern() {
	res := SearchKey[UserDoc]("user:*", nil, false)
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "document-scoped")
}

func (s *DocTestSuite) TestKeysRejectsPrefixedStoragePattern() {
	res := Keys[UserDoc]("user:*")
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "document-scoped")
}

func (s *DocTestSuite) TestUpdateRejectsPrefixedStoragePattern() {
	res := Update[UserDoc]("user:*", nil, x.Set("name", "updated"))
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "document-scoped")
}

func (s *DocTestSuite) TestSearchIndexRejectsFullIdxName() {
	res := SearchIndex[UserDoc]("user_age", "*", nil, false)
	s.Require().True(res.IsError())
	s.Contains(res.Error().Error(), "fully-qualified index name")
}
