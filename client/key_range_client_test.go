package client

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
	searchKRClientNamespace = "probe-client"
	updateKRClientNamespace = "000updclient"
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

type UpdateFixtureDoc string

func (UpdateFixtureDoc) Namespace() string  { return updateKRClientNamespace }
func (UpdateFixtureDoc) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (UpdateFixtureDoc) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d UpdateFixtureDoc) RawJSON() string  { return string(d) }
func (UpdateFixtureDoc) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

func updClientID(id string) string {
	return testutil.XIDKey(updateKRClientNamespace, testutil.KeyRangeFixtureMem(), id)
}

func updClientIDPrefix() string {
	return testutil.XKeyPrefix(updateKRClientNamespace, testutil.KeyRangeFixtureMem())
}

func updClientIDFromStorage(storageKeys []string) []string {
	prefix := updClientIDPrefix()
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

func updClientRawGet(raw, path string) string { return gjson.Get(raw, path).String() }

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

func (s *SearchKeyRangeSuite) TestSearchIndexRange_InvalidInputs_EmptyIndexAndNilKeyrange() {
	s.ensureConnectedClientAndAuth()
	s.Run("empty index name → early error", func() {
		res := SearchIndex("", x.KeysPattern(krClientID("*")), nil, false)
		s.Require().True(res.IsError(), "expected Err for empty indexName; got Ok len=%d", len(res.OrEmpty()))
		s.Contains(res.Error().Error(), "index name is required",
			"err=%v", res.Error())
	})
	s.Run("nil keyrange → early error", func() {
		idxScore := testutil.KeyRangeIndexName(searchKRClientNamespace, testutil.KeyRangeFixtureMem(), "score")
		res := SearchIndex(idxScore, nil, nil, false)
		s.Require().True(res.IsError(), "expected Err for nil kr; got Ok len=%d", len(res.OrEmpty()))
		s.Contains(res.Error().Error(), "key range is required",
			"err=%v", res.Error())
	})
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

func (s *UpdateKeyRangeSuite) SetupSuite() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.SetupSuite()
}

func (s *UpdateKeyRangeSuite) SetupTest() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.SetupTest()
	s.ensureConnectedClientAndAuth()
	for _, kv := range testutil.LoadXFor(s.T(), updateKRClientNamespace, testutil.KeyRangeFixtureMem()) {
		err := Set(kv.K, kv.V)
		s.Require().NoError(err, "UpdateKR SetupTest Set key=%s err=%v", kv.K, err)
	}
}

func (s *UpdateKeyRangeSuite) ensureConnectedClientAndAuth() {
	var clientSuite ClientTestSuite
	clientSuite.SetT(s.T())
	clientSuite.ensureConnectedClientAndAuth()
}

func (s *UpdateKeyRangeSuite) TestSeedCountMatchesSearchKey() {
	allKr := x.KeysPattern(updClientID("*"))
	skRes := SearchKey(allKr, nil, false)
	s.Require().False(skRes.IsError(), "SK err: %v", skRes.Error())
	s.Len(skRes.MustGet(), testutil.CountX(), "UpdateKR seed count should equal fixture count")
}

func (s *UpdateKeyRangeSuite) TestNoCrossContamination() {
	resWrong := Update(x.KeysPattern("probe-client:*"), nil, x.Set("tag_contam", true))
	s.False(resWrong.IsError())
	s.Empty(resWrong.MustGet(), "cross-contam probe-client prefix should hit zero keys")

	resServer := Update(x.KeysPattern(testutil.XKeyPrefix("probe-server", true)+"*"), nil, x.Set("tag_contam", true))
	s.False(resServer.IsError())
	s.Empty(resServer.MustGet(), "cross-contam probe-server prefix should hit zero keys")

	skAll := SearchKey(x.KeysPattern(updClientID("*")), nil, false)
	s.Require().False(skAll.IsError())
	for _, v := range skAll.MustGet() {
		got := updClientRawGet(v, "tag_contam")
		s.NotEqual("true", got, "tag_contam leaked to fixture data; ctor_shape=%q raw=%s", updClientRawGet(v, "ctor_shape"), v)
	}
}

func (s *UpdateKeyRangeSuite) TestBulkSetAllTagThenVerifyViaSearchKey() {
	allKr := x.KeysPattern(updClientID("*"))
	res := Update(allKr, nil, x.Set("update_tagged", "bulk_all"))
	s.Require().False(res.IsError(), "Update bulk_all err: %v", res.Error())
	keys := res.MustGet()
	s.Len(keys, testutil.CountX())
	sort.Strings(keys)

	skAfter := SearchKey(allKr, nil, false)
	s.Require().False(skAfter.IsError())
	after := skAfter.MustGet()
	s.Len(after, testutil.CountX())
	for _, v := range after {
		s.Equal("bulk_all", updClientRawGet(v, "update_tagged"),
			"every value should carry update_tagged=bulk_all; raw=%s", v)
	}
}

func (s *UpdateKeyRangeSuite) TestUpdateAllKeyRangeCtorShapes_TABLE_DRIVEN() {
	epoch := 0
	nextTag := func() string {
		epoch++
		return fmt.Sprintf("e%d", epoch)
	}
	runAsc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := Update(kr, nil, x.Set("ctor_shape", tag))
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return updClientIDFromStorage(res.MustGet()), true, ""
	}
	runDesc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := Update(kr, nil, x.Set("ctor_shape", tag))
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		ids := updClientIDFromStorage(res.MustGet())
		for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
			ids[i], ids[j] = ids[j], ids[i]
		}
		return ids, true, ""
	}
	assertCtorShapeWritten := func(caseName, label string, wantCount int, verifyRange x.KeyRange, wantTag string) {
		s.T().Helper()
		skRes := SearchKey(verifyRange, nil, false)
		s.Require().False(skRes.IsError(), "%s/%s: SearchKey after Update err: %v", caseName, label, skRes.Error())
		values := skRes.MustGet()
		var count int
		for _, v := range values {
			if updClientRawGet(v, "ctor_shape") == wantTag {
				count++
			}
		}
		s.Equal(wantCount, count,
			"%s/%s: ctor_shape=%q written count mismatch want=%d got=%d (SearchKey range len=%d)",
			caseName, label, wantTag, wantCount, count, len(values))
	}
	for _, tc := range testutil.KeyRangeCtorCases() {
		tc := tc
		kr := tc.Build(updClientID)
		fullCase := "UpdateKR/" + tc.Name

		tag := nextTag()
		ids, ok, errMsg := runAsc(kr, tag)
		assertClientKRResult(s.T(), fullCase, "ASC_no_limit", tc.WantAsc, ids, ok, errMsg, false)
		if ok && len(ids) > 0 {
			assertCtorShapeWritten(fullCase, "ASC_no_limit", len(ids), kr, tag)
		}

		tag = nextTag()
		ids, ok, errMsg = runDesc(kr, tag)
		assertClientKRResult(s.T(), fullCase, "DESC_no_limit", tc.WantAsc, ids, ok, errMsg, true)
		if ok && len(tc.WantAsc) > 0 {
			assertCtorShapeWritten(fullCase, "DESC_no_limit", len(tc.WantAsc), kr, tag)
		}

		if len(tc.WantAsc) >= 5 {
			limit5Asc := tc.WantAsc[:5]
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(5), tag)
			assertClientKRResult(s.T(), fullCase, "ASC_Limit_5_is_first_5", limit5Asc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_5", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runDesc(kr.Limit(5), tag)
			assertClientKRResult(s.T(), fullCase, "DESC_Limit_5_is_last_5_rev", limit5Asc, ids, ok, errMsg, true)
			if ok && len(limit5Asc) > 0 {
				assertCtorShapeWritten(fullCase, "DESC_Limit_5", 5, kr, tag)
			}
		}
		if len(tc.WantAsc) >= 3 {
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)), tag)
			assertClientKRResult(s.T(), fullCase, "ASC_Limit_EQ_count_returns_all", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_EQ_count", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)+500), tag)
			assertClientKRResult(s.T(), fullCase, "ASC_Limit_OVER_count_safe", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_OVER_count", len(ids), kr, tag)
			}
		}
	}
}

func assertClientKRResult(t *testing.T, caseName, label string, wantAsc, ids []string, ok bool, errMsg string, desc bool) {
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
	for i := range want {
		if want[i] != ids[i] {
			t.Errorf("%s/%s content mismatch (desc=%v): want[%d]=%q got[%d]=%q", caseName, label, desc, i, want[i], i, ids[i])
			return
		}
	}
}

func (s *UpdateKeyRangeSuite) TestGtGteBoundaryGapEqualsOne() {
	krGte := x.KeysGte(updClientID("p027"))
	resGte := Update(krGte, nil, x.Set("boundary", "gte"))
	s.Require().False(resGte.IsError())
	idsGte := updClientIDFromStorage(resGte.MustGet())

	skGte := SearchKey(krGte, nil, false)
	s.Require().False(skGte.IsError())
	gotGte := skGte.MustGet()
	s.Len(gotGte, len(idsGte), "Gte SK sweep after Update expected len=%d got=%d", len(idsGte), len(gotGte))
	for _, v := range gotGte {
		got := updClientRawGet(v, "boundary")
		s.Equal("gte", got, "Update Gte value mismatch on boundary field: raw=%s", v)
	}

	krGt := x.KeysGt(updClientID("p027"))
	resGt := Update(krGt, nil, x.Set("boundary", "gt"))
	s.Require().False(resGt.IsError())
	idsGt := updClientIDFromStorage(resGt.MustGet())

	skGt := SearchKey(krGt, nil, false)
	s.Require().False(skGt.IsError())
	gotGt := skGt.MustGet()
	s.Len(gotGt, len(idsGt), "Gt SK sweep after Update expected len=%d got=%d", len(idsGt), len(gotGt))
	for _, v := range gotGt {
		got := updClientRawGet(v, "boundary")
		s.Equal("gt", got, "Update Gt value mismatch on boundary field: raw=%s", v)
	}

	testutil.AssertGtGteGap1(s.T(), idsGte, idsGt, "p027")
}

func (s *UpdateKeyRangeSuite) TestLtLteBoundaryGapEqualsOne() {
	krLte := x.KeysLte(updClientID("p072"))
	resLte := Update(krLte, nil, x.Set("boundary", "lte"))
	s.Require().False(resLte.IsError())
	idsLte := updClientIDFromStorage(resLte.MustGet())

	skLte := SearchKey(krLte, nil, false)
	s.Require().False(skLte.IsError())
	gotLte := skLte.MustGet()
	s.Len(gotLte, len(idsLte), "Lte SK sweep after Update expected len=%d got=%d", len(idsLte), len(gotLte))
	for _, v := range gotLte {
		got := updClientRawGet(v, "boundary")
		s.Equal("lte", got, "Update Lte value mismatch on boundary field: raw=%s", v)
	}

	krLt := x.KeysLt(updClientID("p072"))
	resLt := Update(krLt, nil, x.Set("boundary", "lt"))
	s.Require().False(resLt.IsError())
	idsLt := updClientIDFromStorage(resLt.MustGet())

	skLt := SearchKey(krLt, nil, false)
	s.Require().False(skLt.IsError())
	gotLt := skLt.MustGet()
	s.Len(gotLt, len(idsLt), "Lt SK sweep after Update expected len=%d got=%d", len(idsLt), len(gotLt))
	for _, v := range gotLt {
		got := updClientRawGet(v, "boundary")
		s.Equal("lt", got, "Update Lt value mismatch on boundary field: raw=%s", v)
	}

	testutil.AssertLtLteGap1(s.T(), idsLte, idsLt, "p072")
}

func (s *UpdateKeyRangeSuite) TestLimit7PrefixEqualFullSet() {
	allKr := x.KeysPattern(updClientID("*"))

	fullRes := Update(allKr, nil, x.Set("lim", "full"))
	s.Require().False(fullRes.IsError(), "full err=%v", fullRes.Error())
	full := fullRes.MustGet()
	s.Len(full, testutil.CountX())
	sort.Strings(full)
	skFull := SearchKey(allKr, nil, false)
	s.Require().False(skFull.IsError())
	gotFull := skFull.MustGet()
	s.Len(gotFull, testutil.CountX())
	for _, v := range gotFull {
		got := updClientRawGet(v, "lim")
		s.Equal("full", got, "Update lim=full value mismatch: raw=%s", v)
	}

	limitRes := Update(x.KeysPattern(updClientID("*")).Limit(7), nil, x.Set("lim", "7"))
	s.Require().False(limitRes.IsError(), "limit err=%v", limitRes.Error())
	lim := limitRes.MustGet()
	s.Len(lim, 7, "Limit(7) must truncate at callback=7, got len=%d", len(lim))
	sort.Strings(lim)
	s.Equal(full[:7], lim, "Limit(7) updated keys must equal ASC first-7 of full set — proves early-stop at callback")
	skLim := SearchKey(allKr, nil, false)
	s.Require().False(skLim.IsError())
	gotLim := skLim.MustGet()
	var cntLim7 int
	for _, v := range gotLim {
		got := updClientRawGet(v, "lim")
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
	res := Update(x.KeysPattern(updClientID("*")), filter, x.Set("filtered_tag", "A-only"))
	s.Require().False(res.IsError(), "filtered err=%v", res.Error())
	ids := updClientIDFromStorage(res.MustGet())
	s.Len(ids, 34, "Update+filter Eq(bucket,A) should match 34 bucket=A rows")

	skAll := SearchKey(x.KeysPattern(updClientID("*")), nil, false)
	s.Require().False(skAll.IsError())
	var count int
	for _, v := range skAll.MustGet() {
		if updClientRawGet(v, "filtered_tag") == "A-only" {
			count++
		}
	}
	s.Equal(len(ids), count, "only updated rows carry filtered_tag; count=%d", count)
}

func (s *UpdateKeyRangeSuite) TestNilKRRejects() {
	res := Update(nil, nil, x.Set("nil_tag", true))
	s.Require().True(res.IsError(), "nil kr must reject")
	s.Contains(res.Error().Error(), "key range is required")
}

func (s *UpdateKeyRangeSuite) TestEmptyValuesRejects() {
	res := Update(x.KeysPattern(updClientID("*")), nil)
	s.Require().True(res.IsError(), "no mutation values must reject")
	s.Contains(res.Error().Error(), "no update values provided")
}

func TestUpdateKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(UpdateKeyRangeSuite))
}
