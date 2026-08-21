package client

import (
	"testing"
	"time"

	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/suite"
)

const (
	searchKRClientNamespace = "probe-client"
)

type SearchFixtureDoc string

func (SearchFixtureDoc) Namespace() string  { return searchKRClientNamespace }
func (SearchFixtureDoc) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (SearchFixtureDoc) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d SearchFixtureDoc) RawJSON() string  { return string(d) }
func (SearchFixtureDoc) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

func krClientID(id string) string {
	return testutil.XIDKey(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), id)
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeSeedCountIs100() {
	s.ensureConnectedClientAndAuth()
	kr := x.KeysPattern(krClientID("*"))
	got := SearchKey(kr, nil, false)
	s.False(got.IsError(), "SearchKey err: %v", got.Error())
	s.Len(got.MustGet(), testutil.CountX())
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeNoCrossContamination() {
	s.ensureConnectedClientAndAuth()

	krWrongPrefix := x.KeysPattern("probe-client:*")
	resWrong := SearchKey(krWrongPrefix, nil, false)
	s.False(resWrong.IsError())
	s.Empty(resWrong.MustGet())

	krServer := x.KeysPattern(testutil.XKeyPrefix("probe-server", true) + "*")
	resServer := SearchKey(krServer, nil, false)
	s.False(resServer.IsError())
	s.Empty(resServer.MustGet())
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeFullMatrix_TABLE_DRIVEN() {
	s.ensureConnectedClientAndAuth()
	run := func(kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := SearchKey(kr, nil, desc)
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(res.MustGet()), true, ""
	}
	testutil.AssertSearchKeyMatrix(s.T(), run, testutil.KeyRangeCtorCases(), krClientID, "SK/")
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeGtGteGapEqualsOne() {
	s.ensureConnectedClientAndAuth()
	gte := SearchKey(x.KeysGte(krClientID("p027")), nil, false)
	s.Require().False(gte.IsError())
	gt := SearchKey(x.KeysGt(krClientID("p027")), nil, false)
	s.Require().False(gt.IsError())
	testutil.AssertGtGteGap1(s.T(),
		testutil.XIDsFromValues(gte.MustGet()),
		testutil.XIDsFromValues(gt.MustGet()),
		"p027")
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeLtLteGapEqualsOne() {
	s.ensureConnectedClientAndAuth()
	lte := SearchKey(x.KeysLte(krClientID("p072")), nil, false)
	s.Require().False(lte.IsError())
	lt := SearchKey(x.KeysLt(krClientID("p072")), nil, false)
	s.Require().False(lt.IsError())
	testutil.AssertLtLteGap1(s.T(),
		testutil.XIDsFromValues(lte.MustGet()),
		testutil.XIDsFromValues(lt.MustGet()),
		"p072")
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeScoreSeedCountIs100() {
	s.ensureConnectedClientAndAuth()
	kr := x.KeysPattern(krClientID("*"))
	idxName := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")
	got := SearchIndex(idxName, kr, nil, false)
	s.False(got.IsError(), "SearchIndex score ASC err: %v", got.Error())
	s.Len(got.MustGet(), testutil.CountX())
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeScoreOrderingMatchesSearchKeyIdOrder() {
	s.ensureConnectedClientAndAuth()
	krAll := x.KeysPattern(krClientID("*"))
	idxScore := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")

	siAsc := SearchIndex(idxScore, krAll, nil, false)
	s.Require().False(siAsc.IsError())
	skAsc := SearchKey(krAll, nil, false)
	s.Require().False(skAsc.IsError())
	siDesc := SearchIndex(idxScore, krAll, nil, true)
	s.Require().False(siDesc.IsError())
	skDesc := SearchKey(krAll, nil, true)
	s.Require().False(skDesc.IsError())

	testutil.AssertScoreEqSKId(s.T(),
		testutil.XIDsFromValues(siAsc.MustGet()),
		testutil.XIDsFromValues(skAsc.MustGet()),
		testutil.XIDsFromValues(siDesc.MustGet()),
		testutil.XIDsFromValues(skDesc.MustGet()))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeFullMatrix_TABLE_DRIVEN() {
	s.ensureConnectedClientAndAuth()
	idxName := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")
	run := func(n string, kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := SearchIndex(n, kr, nil, desc)
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(res.MustGet()), true, ""
	}
	testutil.AssertSearchIndexMatrix(s.T(), run, idxName, testutil.KeyRangeCtorCases(), krClientID, "idx=score/")
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeBucketTiebreakersLexicographicById() {
	s.ensureConnectedClientAndAuth()
	krAll := x.KeysPattern(krClientID("*"))
	idxBucket := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "bucket")
	eqA := SearchIndex(idxBucket, krAll, x.Eq("bucket", "A"), false)
	s.Require().False(eqA.IsError())
	eqC := SearchIndex(idxBucket, krAll, x.Eq("bucket", "C"), false)
	s.Require().False(eqC.IsError())
	all := SearchIndex(idxBucket, krAll, nil, false)
	s.Require().False(all.IsError())
	testutil.AssertBucketDistribution(s.T(),
		testutil.XIDsFromValues(eqA.MustGet()),
		testutil.XIDsFromValues(eqC.MustGet()),
		testutil.XIDsFromValues(all.MustGet()))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeSparseAmtLimit10() {
	s.ensureConnectedClientAndAuth()
	krLimit := x.KeysPattern(krClientID("*")).Limit(10)
	idxSparse := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "sparse_amt")
	si := SearchIndex(idxSparse, krLimit, nil, false)
	s.Require().False(si.IsError())
	testutil.AssertSparseLimit10(s.T(), testutil.XIDsFromValues(si.MustGet()))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeCrossLayerMismatchRejects() {
	s.ensureConnectedClientAndAuth()
	krDisk := x.KeysPattern("user:*")
	idxScore := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")
	res := SearchIndex(idxScore, krDisk, nil, false)
	s.True(res.IsError(), "got Ok len=%d", len(res.OrEmpty()))
	s.Contains(res.Error().Error(), "different storage layer")
}

type SearchKeyRangeSuite struct {
	suite.Suite
}

func (s *SearchKeyRangeSuite) SetupSuite() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.SetupSuite()
}

func (s *SearchKeyRangeSuite) SetupTest() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.SetupTest()
}

func (s *SearchKeyRangeSuite) ensureConnectedClientAndAuth() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.ensureConnectedClientAndAuth()
}

func TestSearchKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(SearchKeyRangeSuite))
}

type UpdateKeyRangeSuite struct {
	suite.Suite
}

func (s *UpdateKeyRangeSuite) TestPlaceholderOnly() {
	s.T().Skip("UpdateKeyRangeSuite placeholder — to be implemented with Update API KeyRange parity")
}

func TestUpdateKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(UpdateKeyRangeSuite))
}
