package doc

import (
	"testing"
	"time"

	"github.com/kcmvp/redisx/client"
	"github.com/kcmvp/redisx/server"
	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
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
	db := server.Start(
		docTestServerAddr,
		":memory:",
	)
	s.Require().NotNil(db)

	err := client.ConnectEmbed(docTestServerAddr)
	s.Require().NoError(err)

	for i := 0; i < 30; i++ {
		res := client.Keys("*")
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

// UserDoc example implementation
type UserDoc string

func (u UserDoc) Prefix() string     { return "user:" }
func (u UserDoc) Key() string        { return gjson.Get(string(u), "id").String() }
func (u UserDoc) Value() string      { return string(u) }
func (u UserDoc) TTL() time.Duration { return time.Hour }
func (u UserDoc) StorageKey() string { return x.DefaultStorageKey(u) }

func (s *DocTestSuite) TestGenericDocMethods() {
	jsonStr := `{"id": "200", "name": "Test", "age": 30}`
	doc := UserDoc(jsonStr)

	// Set
	err := Set(doc)
	s.Require().NoError(err)

	// Get
	fetchDoc := UserDoc(`{"id": "200"}`)
	val, err := Get(fetchDoc)
	s.Require().NoError(err)
	s.Equal(jsonStr, val)

	// SetNX
	ok, err := SetNX(doc)
	s.Require().NoError(err)
	s.False(ok) // Already exists

	// Keys
	keysRes := Keys(doc, "*")
	s.Require().NoError(keysRes.Error())
	s.Contains(keysRes.MustGet(), "user:200")

	// SearchKey
	searchRes := SearchKey(doc, "*", x.Eq("age", 30), false)
	s.Require().NoError(searchRes.Error())
	s.Contains(searchRes.MustGet(), jsonStr)

	// Update
	updRes := Update(doc, "*", x.Eq("age", 30), x.Set("age", 31))
	s.Require().NoError(updRes.Error())

	// Delete
	del, err := Delete(fetchDoc)
	s.Require().NoError(err)
	s.True(del)
}
