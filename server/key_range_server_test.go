package server

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
)

const (
	searchKRFixtureNamespace = "probe-server"
	updateKRFixtureNamespace = "000updserver"
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
	resWrong := s.db.Update(x.KeysPattern("probe-server:*"), nil, x.Set("tag_contam", true))
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
