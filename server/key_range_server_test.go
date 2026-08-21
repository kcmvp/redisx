package server

import (
	"testing"
	"time"

	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/suite"
)

const (
	searchKRFixtureNamespace = "probe-server"
)

type SearchFixtureDoc string

func (SearchFixtureDoc) Namespace() string  { return searchKRFixtureNamespace }
func (SearchFixtureDoc) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (SearchFixtureDoc) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d SearchFixtureDoc) RawJSON() string  { return string(d) }
func (SearchFixtureDoc) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

func krID(id string) string {
	return testutil.XIDKey(searchKRFixtureNamespace, testutil.KeyRangeFixtureMem(), id)
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
}

func (s *UpdateKeyRangeSuite) TestPlaceholderOnly() {
	s.T().Skip("UpdateKeyRangeSuite placeholder — to be implemented with Update API KeyRange parity")
}

func TestUpdateKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(UpdateKeyRangeSuite))
}
