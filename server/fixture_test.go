package server

import (
	"testing"

	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/suite"
)

const (
	fixtureNamespace = "probe-server"
	fixtureMem       = true
)

func fixtureID(id string) string {
	return testutil.XIDKey(fixtureNamespace, fixtureMem, id)
}

type FixtureSuite struct {
	suite.Suite
	db *DB
}

func (suite *FixtureSuite) SetupTest() {
	suite.db = openDB(testutil.DBPath(suite.T()))
	suite.Require().NotNil(suite.db)

	kvs := testutil.LoadXFor(suite.T(), fixtureNamespace, fixtureMem)
	for _, kv := range kvs {
		suite.Require().NoError(suite.db.Set(kv.K, kv.V))
	}
}

func (suite *FixtureSuite) TearDownTest() {
	if suite.db != nil {
		_ = suite.db.Close()
	}
}

func (suite *FixtureSuite) TestXSeedCountIs100() {
	kr := x.KeysPattern(testutil.XKeyPrefix(fixtureNamespace, fixtureMem) + "*")
	got := suite.db.SearchKey(kr, nil, false)
	suite.True(got.IsOk(), "SearchKey err: %v", got.Error())
	suite.Len(got.MustGet(), testutil.CountX(), "fixture X should seed exactly %d keys; got %d (mem layer routing broken?)", testutil.CountX(), len(got.MustGet()))
}

func (suite *FixtureSuite) TestXNoCrossContaminationWithDiskSuite() {
	krDisk := x.KeysPattern("user:*")
	resDisk := suite.db.SearchKey(krDisk, nil, false)
	suite.True(resDisk.IsOk(), "SearchKey err: %v", resDisk.Error())
	suite.Empty(resDisk.MustGet(), "fixture suite is memory-only, should have 0 user:* disk keys (DB isolation broken?)")

	krWrongPrefix := x.KeysPattern("probe:*")
	resWrong := suite.db.SearchKey(krWrongPrefix, nil, false)
	suite.True(resWrong.IsOk(), "SearchKey err: %v", resWrong.Error())
	suite.Empty(resWrong.MustGet(), "keys without _m_ prefix should not exist — XFixture.Mem() must add the prefix")
}

type allCtorCase struct {
	name    string
	build   func() x.KeyRange
	wantAsc []string
}

func ctorCases() []allCtorCase {
	allAsc := testutil.XRangeIDs(0, 99)
	cases := []allCtorCase{}

	cases = append(cases, allCtorCase{
		name:    "KeysPattern_star_all_100",
		build:   func() x.KeyRange { return x.KeysPattern(testutil.XKeyPrefix(fixtureNamespace, fixtureMem) + "*") },
		wantAsc: allAsc,
	})

	cases = append(cases, allCtorCase{
		name:    "KeysPattern_leading_glob_p05_star_band05_only_10",
		build:   func() x.KeyRange { return x.KeysPattern(fixtureID("p05*")) },
		wantAsc: testutil.XRangeIDs(50, 59),
	})

	cases = append(cases, allCtorCase{
		name:  "KeysPattern_single_char_question_p0_Q5_10_every_tenth",
		build: func() x.KeyRange { return x.KeysPattern(fixtureID("p0?5")) },
		wantAsc: []string{
			"p005", "p015", "p025", "p035", "p045",
			"p055", "p065", "p075", "p085", "p095",
		},
	})

	cases = append(cases, allCtorCase{
		name:    "KeysGte_literal_p050_INCLUSIVE_50",
		build:   func() x.KeyRange { return x.KeysGte(fixtureID("p050")) },
		wantAsc: testutil.XRangeIDs(50, 99),
	})

	cases = append(cases, allCtorCase{
		name:    "KeysGte_pattern_p05_star_10_ONLY_match_true_kept",
		build:   func() x.KeyRange { return x.KeysGte(fixtureID("p05*")) },
		wantAsc: testutil.XRangeIDs(50, 59),
	})

	cases = append(cases, allCtorCase{
		name:    "KeysGt_literal_p050_EXCLUSIVE_49",
		build:   func() x.KeyRange { return x.KeysGt(fixtureID("p050")) },
		wantAsc: testutil.XRangeIDs(51, 99),
	})

	cases = append(cases, allCtorCase{
		name:    "KeysGt_pattern_p05_star_SAME_10_as_Gte_pattern_for_band05",
		build:   func() x.KeyRange { return x.KeysGt(fixtureID("p05*")) },
		wantAsc: testutil.XRangeIDs(50, 59),
	})

	cases = append(cases, allCtorCase{
		name:    "KeysLte_literal_p049_INCLUSIVE_50",
		build:   func() x.KeyRange { return x.KeysLte(fixtureID("p049")) },
		wantAsc: testutil.XRangeIDs(0, 49),
	})

	cases = append(cases, allCtorCase{
		name:    "KeysLte_pattern_p04_star_EMPTY_never_both_true",
		build:   func() x.KeyRange { return x.KeysLte(fixtureID("p04*")) },
		wantAsc: []string{},
	})

	cases = append(cases, allCtorCase{
		name:    "KeysLt_literal_p050_EXCLUSIVE_50",
		build:   func() x.KeyRange { return x.KeysLt(fixtureID("p050")) },
		wantAsc: testutil.XRangeIDs(0, 49),
	})

	cases = append(cases, allCtorCase{
		name:    "KeysLt_pattern_p05_star_EMPTY_never_both_true",
		build:   func() x.KeyRange { return x.KeysLt(fixtureID("p05*")) },
		wantAsc: []string{},
	})

	cases = append(cases, allCtorCase{
		name:    "KeysBt_literal_literal_p020_p070_halfopen_50",
		build:   func() x.KeyRange { return x.KeysBt(fixtureID("p020"), fixtureID("p070")) },
		wantAsc: testutil.XRangeIDs(20, 69),
	})

	cases = append(cases, allCtorCase{
		name:    "KeysBt_pattern_ge_p03_star_literal_lt_p070_40",
		build:   func() x.KeyRange { return x.KeysBt(fixtureID("p03*"), fixtureID("p070")) },
		wantAsc: testutil.XRangeIDs(30, 69),
	})

	cases = append(cases, allCtorCase{
		name:    "KeysBt_literal_ge_p020_pattern_lt_p06_star_50",
		build:   func() x.KeyRange { return x.KeysBt(fixtureID("p020"), fixtureID("p06*")) },
		wantAsc: testutil.XRangeIDs(20, 69),
	})

	cases = append(cases, allCtorCase{
		name:    "KeysBt_pattern_p03_star_pattern_p06_star_40",
		build:   func() x.KeyRange { return x.KeysBt(fixtureID("p03*"), fixtureID("p06*")) },
		wantAsc: testutil.XRangeIDs(30, 69),
	})

	return cases
}

func (suite *FixtureSuite) TestXAllKeyRangeCtorShapes_TABLE_DRIVEN() {
	cases := ctorCases()

	for _, tc := range cases {
		kr := tc.build()

		suite.Run(tc.name+"/ASC_no_limit", func() {
			res := suite.db.SearchKey(kr, nil, false)
			suite.True(res.IsOk(), "SearchKey ASC err: %v", res.Error())
			ids := testutil.XIDsFromValues(res.MustGet())
			suite.Equal(len(tc.wantAsc), len(ids), "length mismatch: want %d got %d", len(tc.wantAsc), len(ids))
			if len(ids) > 0 {
				suite.True(testutil.XStrictMonotonic(ids, false), "ASC not strictly monotonic")
			}
			suite.Equal(tc.wantAsc, ids, "ASC full list mismatch")
		})

		suite.Run(tc.name+"/DESC_no_limit", func() {
			res := suite.db.SearchKey(kr, nil, true)
			suite.True(res.IsOk(), "SearchKey DESC err: %v", res.Error())
			ids := testutil.XIDsFromValues(res.MustGet())
			suite.Equal(len(tc.wantAsc), len(ids), "DESC length mismatch: want %d got %d", len(tc.wantAsc), len(ids))
			if len(ids) > 0 {
				suite.True(testutil.XStrictMonotonic(ids, true), "DESC not strictly monotonic")
			}
			suite.Equal(testutil.XReverseIDs(tc.wantAsc), ids, "DESC full list must equal reverse(ASC)")
		})

		if len(tc.wantAsc) >= 5 {
			suite.Run(tc.name+"/ASC_Limit_5_is_first_5", func() {
				krLimit := kr.Limit(5)
				res := suite.db.SearchKey(krLimit, nil, false)
				suite.True(res.IsOk(), "SearchKey ASC Limit(5) err: %v", res.Error())
				ids := testutil.XIDsFromValues(res.MustGet())
				suite.Len(ids, 5, "Limit(5) should return exactly 5 entries; got %d", len(ids))
				suite.Equal(testutil.XFirstN(tc.wantAsc, 5), ids, "ASC Limit(5) must equal first 5 of ASC unlimited")
			})

			suite.Run(tc.name+"/DESC_Limit_5_is_last_5_of_ASC_reversed", func() {
				krLimit := kr.Limit(5)
				res := suite.db.SearchKey(krLimit, nil, true)
				suite.True(res.IsOk(), "SearchKey DESC Limit(5) err: %v", res.Error())
				ids := testutil.XIDsFromValues(res.MustGet())
				suite.Len(ids, 5, "DESC Limit(5) should return exactly 5 entries; got %d", len(ids))
				wantDesc5 := testutil.XReverseIDs(testutil.XLastN(tc.wantAsc, 5))
				suite.Equal(wantDesc5, ids, "DESC Limit(5) must be reverse(last 5 ASC); got %v want %v", ids, wantDesc5)
			})
		}

		if len(tc.wantAsc) >= 3 {
			suite.Run(tc.name+"/ASC_Limit_EQ_full_count_returns_all", func() {
				krLimit := kr.Limit(len(tc.wantAsc))
				res := suite.db.SearchKey(krLimit, nil, false)
				suite.True(res.IsOk())
				ids := testutil.XIDsFromValues(res.MustGet())
				suite.Equal(tc.wantAsc, ids, "Limit(%d) should be identical to unlimited ASC", len(tc.wantAsc))
			})

			suite.Run(tc.name+"/ASC_Limit_OVER_count_safe_returns_all", func() {
				krLimit := kr.Limit(len(tc.wantAsc) + 500)
				res := suite.db.SearchKey(krLimit, nil, false)
				suite.True(res.IsOk())
				ids := testutil.XIDsFromValues(res.MustGet())
				suite.Equal(tc.wantAsc, ids, "Limit larger than result set must be safe (no extra, no short)")
			})
		}
	}
}

func (suite *FixtureSuite) TestXGtAndGte_LITERAL_DIFFERENCE_is_exactly_1_at_boundary() {
	resGte := suite.db.SearchKey(x.KeysGte(fixtureID("p050")), nil, false)
	suite.True(resGte.IsOk())
	idsGte := testutil.XIDsFromValues(resGte.MustGet())

	resGt := suite.db.SearchKey(x.KeysGt(fixtureID("p050")), nil, false)
	suite.True(resGt.IsOk())
	idsGt := testutil.XIDsFromValues(resGt.MustGet())

	suite.Equal(len(idsGte), len(idsGt)+1, "Gte should be strictly 1 item longer than Gt (the boundary itself); %d vs %d", len(idsGte), len(idsGt))
	suite.Contains(idsGte, "p050", "Gte must INCLUDE boundary p050")
	suite.NotContains(idsGt, "p050", "Gt must EXCLUDE boundary p050")
	suite.Equal(idsGte[1:], idsGt, "Gt should equal Gte minus the first boundary element")
}

func (suite *FixtureSuite) TestXLtAndLte_LITERAL_DIFFERENCE_is_exactly_1_at_boundary() {
	resLte := suite.db.SearchKey(x.KeysLte(fixtureID("p049")), nil, false)
	suite.True(resLte.IsOk())
	idsLte := testutil.XIDsFromValues(resLte.MustGet())

	resLt := suite.db.SearchKey(x.KeysLt(fixtureID("p050")), nil, false)
	suite.True(resLt.IsOk())
	idsLt := testutil.XIDsFromValues(resLt.MustGet())

	suite.Equal(idsLte, idsLt, "KeysLte(p049) == KeysLt(p050) (both mean 0..49 inclusive = 50 items)")
	suite.Len(idsLte, 50)

	resLte050 := suite.db.SearchKey(x.KeysLte(fixtureID("p050")), nil, false)
	suite.True(resLte050.IsOk())
	idsLte050 := testutil.XIDsFromValues(resLte050.MustGet())
	suite.Contains(idsLte050, "p050", "KeysLte(p050) must include the boundary")
	suite.Equal(len(idsLt), len(idsLte050)-1, "Lte(p050) should be exactly 1 longer than Lt(p050)")
}

func TestFixtureSuite(t *testing.T) {
	suite.Run(t, new(FixtureSuite))
}
