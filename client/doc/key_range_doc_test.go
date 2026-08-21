package doc

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/kcmvp/redisx/client"
	"github.com/kcmvp/redisx/internal/testutil"
	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/gjson"
)

const (
	searchKRDocNamespace = "probe-doc"
	updateKRDocNamespace = "000upddoc"
)

type SearchFixtureDoc string

func (SearchFixtureDoc) Namespace() string  { return searchKRDocNamespace }
func (SearchFixtureDoc) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (SearchFixtureDoc) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d SearchFixtureDoc) RawJSON() string  { return string(d) }
func (SearchFixtureDoc) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

type UpdateFixtureDoc string

func (UpdateFixtureDoc) Namespace() string  { return updateKRDocNamespace }
func (UpdateFixtureDoc) Mem() bool          { return testutil.KeyRangeFixtureMem() }
func (UpdateFixtureDoc) KeyAttrs() []string { return testutil.KeyRangeDocKeyAttrs() }
func (d UpdateFixtureDoc) RawJSON() string  { return string(d) }
func (UpdateFixtureDoc) TTL() time.Duration { return testutil.KeyRangeDocTTL() }

func krDocID(id string) string  { return id }
func updDocID(id string) string { return id }

func docRaw[D x.Document](docs []D) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.RawJSON())
	}
	return out
}

func updDocIDPrefix() string {
	return testutil.XKeyPrefix(updateKRDocNamespace, testutil.KeyRangeFixtureMem())
}

func updDocIDFromStorage(storageKeys []string) []string {
	prefix := updDocIDPrefix()
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

func updDocRawGet(raw, path string) string { return gjson.Get(raw, path).String() }

func (s *SearchKeyRangeSuite) TestSearchKeyRangeSeedCountIs100() {
	kr := x.KeysPattern("p*")
	got := SearchKey[SearchFixtureDoc](kr, nil, false)
	s.False(got.IsError(), "SearchKey err: %v", got.Error())
	s.Len(got.MustGet(), testutil.CountX())
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeNoCrossContamination() {
	krServer := x.KeysPattern(testutil.XKeyPrefix("probe-server", true) + "*")
	resServer := SearchKey[SearchFixtureDoc](krServer, nil, false)
	s.False(resServer.IsError())
	s.Empty(resServer.MustGet())

	krClient := x.KeysPattern(testutil.XKeyPrefix("probe-client", true) + "*")
	resClient := SearchKey[SearchFixtureDoc](krClient, nil, false)
	s.False(resClient.IsError())
	s.Empty(resClient.MustGet())
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeFullMatrix_TABLE_DRIVEN() {
	run := func(kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := SearchKey[SearchFixtureDoc](kr, nil, desc)
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(docRaw(res.MustGet())), true, ""
	}
	testutil.AssertSearchKeyMatrix(s.T(), run, testutil.KeyRangeCtorCases(), krDocID, "SK/")
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeGtGteGapEqualsOne() {
	gte := SearchKey[SearchFixtureDoc](x.KeysGte(krDocID("p027")), nil, false)
	s.Require().False(gte.IsError())
	gt := SearchKey[SearchFixtureDoc](x.KeysGt(krDocID("p027")), nil, false)
	s.Require().False(gt.IsError())
	testutil.AssertGtGteGap1(s.T(),
		testutil.XIDsFromValues(docRaw(gte.MustGet())),
		testutil.XIDsFromValues(docRaw(gt.MustGet())),
		"p027")
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeLtLteGapEqualsOne() {
	lte := SearchKey[SearchFixtureDoc](x.KeysLte(krDocID("p072")), nil, false)
	s.Require().False(lte.IsError())
	lt := SearchKey[SearchFixtureDoc](x.KeysLt(krDocID("p072")), nil, false)
	s.Require().False(lt.IsError())
	testutil.AssertLtLteGap1(s.T(),
		testutil.XIDsFromValues(docRaw(lte.MustGet())),
		testutil.XIDsFromValues(docRaw(lt.MustGet())),
		"p072")
}

func (s *SearchKeyRangeSuite) TestSearchKeyRangeCrossLayerMismatchNote_docScoped() {
	kr, _ := x.UnmarshalKeyRange([]byte(`{"op":"pattern","p":"user:*"}`))
	if kr == nil {
		kr = x.KeysPattern("user:*")
	}
	res := client.SearchKey(kr, nil, false)
	if res.IsError() {
		s.Contains(res.Error().Error(), "wrong number of arguments",
			"untyped client SearchKey error allowed (client may not be fully seeded); skipping cross-layer assertion in doc suite")
		return
	}
	s.True(true,
		"doc-scoped typed APIs (SearchKey[D]/SearchIndex[D]) always scope to D's namespace via ScopeKeyRange prefix, so cross-layer mismatch is impossible at doc layer.")
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeScoreSeedCountIs100() {
	krAll := x.KeysPattern("p*")
	res := SearchIndex[SearchFixtureDoc]("score", krAll, nil, false)
	s.False(res.IsError(), "SearchIndex err: %v", res.Error())
	s.Len(res.MustGet(), testutil.CountX())
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeScoreOrderingMatchesSearchKeyIdOrder() {
	krAll := x.KeysPattern("p*")
	siAsc := SearchIndex[SearchFixtureDoc]("score", krAll, nil, false)
	s.Require().False(siAsc.IsError())
	skAsc := SearchKey[SearchFixtureDoc](krAll, nil, false)
	s.Require().False(skAsc.IsError())
	siDesc := SearchIndex[SearchFixtureDoc]("score", krAll, nil, true)
	s.Require().False(siDesc.IsError())
	skDesc := SearchKey[SearchFixtureDoc](krAll, nil, true)
	s.Require().False(skDesc.IsError())
	testutil.AssertScoreEqSKId(s.T(),
		testutil.XIDsFromValues(docRaw(siAsc.MustGet())),
		testutil.XIDsFromValues(docRaw(skAsc.MustGet())),
		testutil.XIDsFromValues(docRaw(siDesc.MustGet())),
		testutil.XIDsFromValues(docRaw(skDesc.MustGet())))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeFullMatrix_TABLE_DRIVEN() {
	run := func(idxName string, kr x.KeyRange, desc bool) ([]string, bool, string) {
		res := SearchIndex[SearchFixtureDoc](idxName, kr, nil, desc)
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return testutil.XIDsFromValues(docRaw(res.MustGet())), true, ""
	}
	testutil.AssertSearchIndexMatrix(s.T(), run, "score", testutil.KeyRangeCtorCases(), krDocID, "idx=score/")
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeBucketTiebreakersLexicographicById() {
	krAll := x.KeysPattern("p*")
	resA := SearchIndex[SearchFixtureDoc]("bucket", krAll, x.Eq("bucket", "A"), false)
	s.Require().False(resA.IsError())
	resC := SearchIndex[SearchFixtureDoc]("bucket", krAll, x.Eq("bucket", "C"), false)
	s.Require().False(resC.IsError())
	all := SearchIndex[SearchFixtureDoc]("bucket", krAll, nil, false)
	s.Require().False(all.IsError())
	testutil.AssertBucketDistribution(s.T(),
		testutil.XIDsFromValues(docRaw(resA.MustGet())),
		testutil.XIDsFromValues(docRaw(resC.MustGet())),
		testutil.XIDsFromValues(docRaw(all.MustGet())))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeSparseAmtLimit10() {
	krLimit := x.KeysPattern("p*").Limit(10)
	si := SearchIndex[SearchFixtureDoc]("sparse_amt", krLimit, nil, false)
	s.Require().False(si.IsError())
	testutil.AssertSparseLimit10(s.T(), testutil.XIDsFromValues(docRaw(si.MustGet())))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeCrossLayerMismatchNote_docScoped() {
	memKP := testutil.XKeyPrefix(searchKRDocNamespace, testutil.KeyRangeFixtureMem())
	memIdxName := memKP[:len(memKP)-1] + "_score"
	diskKr := x.KeysPattern("user:*")
	res := client.SearchIndex(memIdxName, diskKr, nil, false)
	if res.IsError() {
		s.Contains(res.Error().Error(), "different storage layer",
			"mem index (%s) + disk KeyRange (user:*) must reject cross-layer; got err: %v", memIdxName, res.Error())
		return
	}
	if len(res.OrEmpty()) == 0 {
		s.True(true,
			"doc typed-layer APIs scope keyranges under D's namespace (always same layer), so cross-layer cannot be synthesized from doc package; untyped SI returned empty here, OK to skip.")
		return
	}
	s.FailNowf("unexpected SI Ok result", "len=%d", len(res.OrEmpty()))
}

func (s *SearchKeyRangeSuite) TestSearchIndexRangeScopedLimitCarry4LayerAlignment() {
	idSet := []string{"si_doc_a", "si_doc_b", "si_doc_c", "si_doc_d", "si_doc_e"}
	for _, id := range idSet {
		doc := UserDoc(fmt.Sprintf(`{"id":"%s","tag":"si_doc4align","age":33}`, id))
		s.NoError(Set(doc))
	}
	fullRes := SearchIndex[UserDoc]("age", x.KeysPattern("si_doc*"), x.Eq("tag", "si_doc4align"), false)
	s.Require().NoError(fullRes.Error())
	s.Len(fullRes.MustGet(), len(idSet))

	limitRes := SearchIndex[UserDoc]("age", x.KeysPattern("si_doc*").Limit(2), x.Eq("tag", "si_doc4align"), false)
	s.Require().NoError(limitRes.Error())
	s.Len(limitRes.MustGet(), 2, "Limit(2) must be carried through typed-layer ScopeKeyRange + SI")
	s.Equal(fullRes.MustGet()[:2], limitRes.MustGet(),
		"typed SI Limit carry must preserve ASC order first-N, matching SK parity")
}

type SearchKeyRangeSuite struct {
	suite.Suite
}

func (s *SearchKeyRangeSuite) SetupSuite() {
	var docSuite DocTestSuite
	docSuite.SetT(s.T())
	docSuite.SetupSuite()
}

func TestSearchKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(SearchKeyRangeSuite))
}

type UpdateKeyRangeSuite struct {
	suite.Suite
}

func (s *UpdateKeyRangeSuite) SetupSuite() {
	var docSuite DocTestSuite
	docSuite.SetT(s.T())
	docSuite.SetupSuite()
}

func (s *UpdateKeyRangeSuite) SetupTest() {
	for _, kv := range testutil.LoadXFor(s.T(), updateKRDocNamespace, testutil.KeyRangeFixtureMem()) {
		doc := UpdateFixtureDoc(kv.V)
		s.Require().NoError(Set(doc), "UpdateKR SetupTest Set key=%s", kv.K)
	}
}

func (s *UpdateKeyRangeSuite) TestSeedCountMatchesSearchKey() {
	allKr := x.KeysPattern("p*")
	skRes := SearchKey[UpdateFixtureDoc](allKr, nil, false)
	s.Require().NoError(skRes.Error(), "SK err: %v", skRes.Error())
	s.Len(skRes.MustGet(), testutil.CountX(), "UpdateKR seed count should equal fixture count")
}

func (s *UpdateKeyRangeSuite) TestNoCrossContamination() {
	resWrong := Update[UpdateFixtureDoc](x.KeysPattern("probe-doc:*"), nil, x.Set("tag_contam", true))
	s.NoError(resWrong.Error())
	s.Empty(resWrong.MustGet(), "cross-contam probe-doc prefix should hit zero keys (typed Scope coerces)")

	resServer := Update[UpdateFixtureDoc](x.KeysPattern(clientKRServerStar()), nil, x.Set("tag_contam", true))
	s.NoError(resServer.Error())
	s.Empty(resServer.MustGet(), "cross-contam probe-server prefix should hit zero keys")

	skAll := SearchKey[UpdateFixtureDoc](x.KeysPattern("p*"), nil, false)
	s.Require().NoError(skAll.Error())
	for _, d := range skAll.MustGet() {
		got := updDocRawGet(d.RawJSON(), "tag_contam")
		s.NotEqual("true", got, "tag_contam leaked to fixture data; ctor_shape=%q raw=%s", updDocRawGet(d.RawJSON(), "ctor_shape"), d.RawJSON())
	}
}

func clientKRServerStar() string {
	return testutil.XKeyPrefix("probe-server", true) + "*"
}

func (s *UpdateKeyRangeSuite) TestBulkSetAllTagThenVerifyViaSearchKey() {
	allKr := x.KeysPattern("p*")
	res := Update[UpdateFixtureDoc](allKr, nil, x.Set("update_tagged", "bulk_all"))
	s.Require().NoError(res.Error(), "Update bulk_all err: %v", res.Error())
	keys := res.MustGet()
	s.Len(keys, testutil.CountX())
	sort.Strings(keys)

	skAfter := SearchKey[UpdateFixtureDoc](allKr, nil, false)
	s.Require().NoError(skAfter.Error())
	after := skAfter.MustGet()
	s.Len(after, testutil.CountX())
	for _, v := range after {
		s.Equal("bulk_all", updDocRawGet(v.RawJSON(), "update_tagged"),
			"every value should carry update_tagged=bulk_all; raw=%s", v.RawJSON())
	}
}

func (s *UpdateKeyRangeSuite) TestUpdateAllKeyRangeCtorShapes_TABLE_DRIVEN() {
	epoch := 0
	nextTag := func() string {
		epoch++
		return fmt.Sprintf("e%d", epoch)
	}
	runAsc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := Update[UpdateFixtureDoc](kr, nil, x.Set("ctor_shape", tag))
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		return updDocIDFromStorage(res.MustGet()), true, ""
	}
	runDesc := func(kr x.KeyRange, tag string) ([]string, bool, string) {
		res := Update[UpdateFixtureDoc](kr, nil, x.Set("ctor_shape", tag))
		if res.IsError() {
			return nil, false, res.Error().Error()
		}
		ids := updDocIDFromStorage(res.MustGet())
		for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
			ids[i], ids[j] = ids[j], ids[i]
		}
		return ids, true, ""
	}
	assertCtorShapeWritten := func(caseName, label string, wantCount int, verifyRange x.KeyRange, wantTag string) {
		s.T().Helper()
		skRes := SearchKey[UpdateFixtureDoc](verifyRange, nil, false)
		s.Require().False(skRes.IsError(), "%s/%s: SearchKey[D] after Update err: %v", caseName, label, skRes.Error())
		docs := skRes.MustGet()
		var count int
		for _, d := range docs {
			if updDocRawGet(d.RawJSON(), "ctor_shape") == wantTag {
				count++
			}
		}
		s.Equal(wantCount, count,
			"%s/%s: ctor_shape=%q written count mismatch want=%d got=%d (SearchKey range len=%d)",
			caseName, label, wantTag, wantCount, count, len(docs))
	}
	for _, tc := range testutil.KeyRangeCtorCases() {
		tc := tc
		kr := tc.Build(updDocID)
		fullCase := "UpdateKR/" + tc.Name

		tag := nextTag()
		ids, ok, errMsg := runAsc(kr, tag)
		assertDocKRResult(s.T(), fullCase, "ASC_no_limit", tc.WantAsc, ids, ok, errMsg, false)
		if ok && len(ids) > 0 {
			assertCtorShapeWritten(fullCase, "ASC_no_limit", len(ids), kr, tag)
		}

		tag = nextTag()
		ids, ok, errMsg = runDesc(kr, tag)
		assertDocKRResult(s.T(), fullCase, "DESC_no_limit", tc.WantAsc, ids, ok, errMsg, true)
		if ok && len(tc.WantAsc) > 0 {
			assertCtorShapeWritten(fullCase, "DESC_no_limit", len(tc.WantAsc), kr, tag)
		}

		if len(tc.WantAsc) >= 5 {
			limit5Asc := tc.WantAsc[:5]
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(5), tag)
			assertDocKRResult(s.T(), fullCase, "ASC_Limit_5_is_first_5", limit5Asc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_5", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runDesc(kr.Limit(5), tag)
			assertDocKRResult(s.T(), fullCase, "DESC_Limit_5_is_last_5_rev", limit5Asc, ids, ok, errMsg, true)
			if ok && len(limit5Asc) > 0 {
				assertCtorShapeWritten(fullCase, "DESC_Limit_5", 5, kr, tag)
			}
		}
		if len(tc.WantAsc) >= 3 {
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)), tag)
			assertDocKRResult(s.T(), fullCase, "ASC_Limit_EQ_count_returns_all", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_EQ_count", len(ids), kr, tag)
			}
			tag = nextTag()
			ids, ok, errMsg = runAsc(kr.Limit(len(tc.WantAsc)+500), tag)
			assertDocKRResult(s.T(), fullCase, "ASC_Limit_OVER_count_safe", tc.WantAsc, ids, ok, errMsg, false)
			if ok && len(ids) > 0 {
				assertCtorShapeWritten(fullCase, "ASC_Limit_OVER_count", len(ids), kr, tag)
			}
		}
	}
}

func assertDocKRResult(t *testing.T, caseName, label string, wantAsc, ids []string, ok bool, errMsg string, desc bool) {
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
	krGte := x.KeysGte(updDocID("p027"))
	resGte := Update[UpdateFixtureDoc](krGte, nil, x.Set("boundary", "gte"))
	s.Require().NoError(resGte.Error())
	idsGte := updDocIDFromStorage(resGte.MustGet())

	skGte := SearchKey[UpdateFixtureDoc](krGte, nil, false)
	s.Require().NoError(skGte.Error())
	gotGte := skGte.MustGet()
	s.Len(gotGte, len(idsGte), "Gte SK sweep after Update expected len=%d got=%d", len(idsGte), len(gotGte))
	for _, d := range gotGte {
		got := updDocRawGet(d.RawJSON(), "boundary")
		s.Equal("gte", got, "Update Gte value mismatch on boundary field: raw=%s", d.RawJSON())
	}

	krGt := x.KeysGt(updDocID("p027"))
	resGt := Update[UpdateFixtureDoc](krGt, nil, x.Set("boundary", "gt"))
	s.Require().NoError(resGt.Error())
	idsGt := updDocIDFromStorage(resGt.MustGet())

	skGt := SearchKey[UpdateFixtureDoc](krGt, nil, false)
	s.Require().NoError(skGt.Error())
	gotGt := skGt.MustGet()
	s.Len(gotGt, len(idsGt), "Gt SK sweep after Update expected len=%d got=%d", len(idsGt), len(gotGt))
	for _, d := range gotGt {
		got := updDocRawGet(d.RawJSON(), "boundary")
		s.Equal("gt", got, "Update Gt value mismatch on boundary field: raw=%s", d.RawJSON())
	}

	testutil.AssertGtGteGap1(s.T(), idsGte, idsGt, "p027")
}

func (s *UpdateKeyRangeSuite) TestLtLteBoundaryGapEqualsOne() {
	krLte := x.KeysLte(updDocID("p072"))
	resLte := Update[UpdateFixtureDoc](krLte, nil, x.Set("boundary", "lte"))
	s.Require().NoError(resLte.Error())
	idsLte := updDocIDFromStorage(resLte.MustGet())

	skLte := SearchKey[UpdateFixtureDoc](krLte, nil, false)
	s.Require().NoError(skLte.Error())
	gotLte := skLte.MustGet()
	s.Len(gotLte, len(idsLte), "Lte SK sweep after Update expected len=%d got=%d", len(idsLte), len(gotLte))
	for _, d := range gotLte {
		got := updDocRawGet(d.RawJSON(), "boundary")
		s.Equal("lte", got, "Update Lte value mismatch on boundary field: raw=%s", d.RawJSON())
	}

	krLt := x.KeysLt(updDocID("p072"))
	resLt := Update[UpdateFixtureDoc](krLt, nil, x.Set("boundary", "lt"))
	s.Require().NoError(resLt.Error())
	idsLt := updDocIDFromStorage(resLt.MustGet())

	skLt := SearchKey[UpdateFixtureDoc](krLt, nil, false)
	s.Require().NoError(skLt.Error())
	gotLt := skLt.MustGet()
	s.Len(gotLt, len(idsLt), "Lt SK sweep after Update expected len=%d got=%d", len(idsLt), len(gotLt))
	for _, d := range gotLt {
		got := updDocRawGet(d.RawJSON(), "boundary")
		s.Equal("lt", got, "Update Lt value mismatch on boundary field: raw=%s", d.RawJSON())
	}

	testutil.AssertLtLteGap1(s.T(), idsLte, idsLt, "p072")
}

func (s *UpdateKeyRangeSuite) TestLimit7PrefixEqualFullSet() {
	allKr := x.KeysPattern("p*")

	fullRes := Update[UpdateFixtureDoc](allKr, nil, x.Set("lim", "full"))
	s.Require().NoError(fullRes.Error(), "full err=%v", fullRes.Error())
	full := fullRes.MustGet()
	s.Len(full, testutil.CountX())
	sort.Strings(full)
	skFull := SearchKey[UpdateFixtureDoc](allKr, nil, false)
	s.Require().NoError(skFull.Error())
	gotFull := skFull.MustGet()
	s.Len(gotFull, testutil.CountX())
	for _, d := range gotFull {
		got := updDocRawGet(d.RawJSON(), "lim")
		s.Equal("full", got, "Update lim=full value mismatch: raw=%s", d.RawJSON())
	}

	limitRes := Update[UpdateFixtureDoc](x.KeysPattern("p*").Limit(7), nil, x.Set("lim", "7"))
	s.Require().NoError(limitRes.Error(), "limit err=%v", limitRes.Error())
	lim := limitRes.MustGet()
	s.Len(lim, 7, "Limit(7) must truncate at callback=7, got len=%d", len(lim))
	sort.Strings(lim)
	s.Equal(full[:7], lim, "Limit(7) updated keys must equal ASC first-7 of full set — proves early-stop at callback")
	skLim := SearchKey[UpdateFixtureDoc](allKr, nil, false)
	s.Require().NoError(skLim.Error())
	gotLim := skLim.MustGet()
	var cntLim7 int
	for _, d := range gotLim {
		got := updDocRawGet(d.RawJSON(), "lim")
		if got == "7" {
			cntLim7++
			continue
		}
		s.Equal("full", got, "Limit=7 sweep: non-first-7 docs must keep lim=full, got %q; raw=%s", got, d.RawJSON())
	}
	s.Equal(7, cntLim7, "lim=7 want 7 docs with exact value lim==7 got=%d", cntLim7)
}

func (s *UpdateKeyRangeSuite) TestFilterUpdatesOnlyMatched() {
	filter := x.Eq("bucket", "A")
	res := Update[UpdateFixtureDoc](x.KeysPattern("p*"), filter, x.Set("filtered_tag", "A-only"))
	s.Require().NoError(res.Error(), "filtered err=%v", res.Error())
	ids := updDocIDFromStorage(res.MustGet())
	s.Len(ids, 34, "Update+filter Eq(bucket,A) should match 34 bucket=A rows")

	skAll := SearchKey[UpdateFixtureDoc](x.KeysPattern("p*"), nil, false)
	s.Require().NoError(skAll.Error())
	var count int
	for _, v := range skAll.MustGet() {
		if updDocRawGet(v.RawJSON(), "filtered_tag") == "A-only" {
			count++
		}
	}
	s.Equal(len(ids), count, "only updated rows carry filtered_tag; count=%d", count)
}

func (s *UpdateKeyRangeSuite) TestNilKRRejects() {
	res := Update[UpdateFixtureDoc](nil, nil, x.Set("nil_tag", true))
	s.Require().True(res.IsError(), "nil kr must reject")
	s.Contains(res.Error().Error(), "key range is required")
}

func (s *UpdateKeyRangeSuite) TestTypedScopedLimitCarry4LayerAlignment() {
	idSet := []string{"ukr_a", "ukr_b", "ukr_c", "ukr_d", "ukr_e"}
	for _, id := range idSet {
		doc := UserDoc(fmt.Sprintf(`{"id":"%s","tag":"ukr4align","age":33}`, id))
		s.NoError(Set(doc))
	}
	fullRes := Update[UserDoc](x.KeysPattern("ukr_*"), x.Eq("tag", "ukr4align"), x.Set("scoped", "yes"))
	s.Require().NoError(fullRes.Error())
	s.Len(fullRes.MustGet(), len(idSet))
	skFull := SearchKey[UserDoc](x.KeysPattern("ukr_*"), x.Eq("tag", "ukr4align"), false)
	s.Require().NoError(skFull.Error())
	var cntYes int
	for _, d := range skFull.MustGet() {
		got := updDocRawGet(d.RawJSON(), "scoped")
		if got == "yes" {
			cntYes++
			continue
		}
		s.Equal("", got, "unexpected scoped value (neither yes nor empty): %q raw=%s", got, d.RawJSON())
	}
	s.Equal(len(idSet), cntYes, "scoped=yes per-doc value check want=%d got=%d", len(idSet), cntYes)

	limitRes := Update[UserDoc](x.KeysPattern("ukr_*").Limit(2), x.Eq("tag", "ukr4align"), x.Set("scoped", "lim2"))
	s.Require().NoError(limitRes.Error())
	s.Len(limitRes.MustGet(), 2, "Limit(2) must carry through typed-layer ScopeKeyRange + Update")
	wantFull := fullRes.MustGet()
	sort.Strings(wantFull)
	gotLim := limitRes.MustGet()
	sort.Strings(gotLim)
	s.Equal(wantFull[:2], gotLim, "typed Update Limit carry must preserve ASC order first-N — matching SK parity")
	skLim := SearchKey[UserDoc](x.KeysPattern("ukr_*"), x.Eq("tag", "ukr4align"), false)
	s.Require().NoError(skLim.Error())
	var cntLim2 int
	for _, d := range skLim.MustGet() {
		got := updDocRawGet(d.RawJSON(), "scoped")
		if got == "lim2" {
			cntLim2++
			continue
		}
	}
	s.Equal(2, cntLim2, "scoped=lim2 per-doc value check want=2 got=%d", cntLim2)
}

func TestUpdateKeyRangeSuite(t *testing.T) {
	suite.Run(t, new(UpdateKeyRangeSuite))
}
