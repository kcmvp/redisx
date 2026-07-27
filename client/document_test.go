package client

import (
	"testing"
	"time"

	"github.com/kcmvp/redisx/server"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
)

type DocumentTestSuite struct {
	suite.Suite
}

func TestDocumentSuite(t *testing.T) {
	suite.Run(t, new(DocumentTestSuite))
}

const docTestServerAddr = "127.0.0.1:36381"

func (s *DocumentTestSuite) SetupSuite() {
	s.T().Setenv("HOME", s.T().TempDir())
	db := server.Start(
		docTestServerAddr,
		":memory:",
	)
	s.Require().NotNil(db)

	err := ConnectEmbed(docTestServerAddr)
	s.Require().NoError(err)

	// Wait for connection
	for i := 0; i < 30; i++ {
		if getSharedClient() != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.T().Fatal("failed to connect to test server")
}

func (s *DocumentTestSuite) TearDownSuite() {
	Disconnect()
}

// UserDoc example from the plan
type UserDoc string

func (u UserDoc) Prefix() string {
	return "user:"
}

func (u UserDoc) Key() string {
	return gjson.Get(string(u), "id").String()
}

func (u UserDoc) Value() string {
	return string(u)
}

func (u UserDoc) Ttl() time.Duration {
	return time.Hour
}

func (s *DocumentTestSuite) TestSaveAndFetchDoc() {
	jsonStr := `{"id": "10086", "name": "KCMVP", "profile": {"age": 28, "active": true}}`
	doc := UserDoc(jsonStr)

	// Save
	err := SaveDoc(doc)
	s.Require().NoError(err)

	// Fetch
	fetchDoc := UserDoc(`{"id": "10086"}`)
	val, err := FetchDoc(fetchDoc)
	s.Require().NoError(err)
	s.Equal(jsonStr, val)

	// gjson extraction test
	s.Equal("KCMVP", gjson.Get(val, "name").String())
	s.Equal(int64(28), gjson.Get(val, "profile.age").Int())

	// Delete
	deleted, err := DeleteDoc(fetchDoc)
	s.Require().NoError(err)
	s.True(deleted)
}
