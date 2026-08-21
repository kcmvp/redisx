package x

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test6CtorsDefaultLimitIsMinusOne(t *testing.T) {
	cases := []struct {
		name string
		kr   KeyRange
	}{
		{"KeysBt literal", KeysBt("a", "z")},
		{"KeysBt pattern", KeysBt("a*", "z*")},
		{"KeysGt literal", KeysGt("pivot")},
		{"KeysGt pattern", KeysGt("piv*")},
		{"KeysGte literal", KeysGte("pivot")},
		{"KeysGte pattern", KeysGte("piv*")},
		{"KeysLt literal", KeysLt("pivot")},
		{"KeysLt pattern", KeysLt("piv*")},
		{"KeysLte literal", KeysLte("pivot")},
		{"KeysLte pattern", KeysLte("piv*")},
		{"KeysPattern glob", KeysPattern("*")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, -1, c.kr.GetLimit(), "new ctor must have GetLimit()==-1")
		})
	}
}

func TestLimitNonPositivePanics(t *testing.T) {
	ctors := map[string]func() KeyRange{
		"KeysBt":      func() KeyRange { return KeysBt("a", "z") },
		"KeysGt":      func() KeyRange { return KeysGt("p") },
		"KeysGte":     func() KeyRange { return KeysGte("p") },
		"KeysLt":      func() KeyRange { return KeysLt("p") },
		"KeysLte":     func() KeyRange { return KeysLte("p") },
		"KeysPattern": func() KeyRange { return KeysPattern("*") },
	}
	for name, build := range ctors {
		t.Run(name+"/Limit_0", func(t *testing.T) {
			kr := build()
			require.Panics(t, func() { kr.Limit(0) })
		})
		t.Run(name+"/Limit_neg3", func(t *testing.T) {
			kr := build()
			require.Panics(t, func() { kr.Limit(-3) })
		})
	}
}

func TestLimitChainingLastWins(t *testing.T) {
	ctors := map[string]func() KeyRange{
		"KeysBt":      func() KeyRange { return KeysBt("a", "z") },
		"KeysGt":      func() KeyRange { return KeysGt("p") },
		"KeysGte":     func() KeyRange { return KeysGte("p") },
		"KeysLt":      func() KeyRange { return KeysLt("p") },
		"KeysLte":     func() KeyRange { return KeysLte("p") },
		"KeysPattern": func() KeyRange { return KeysPattern("*") },
	}
	for name, build := range ctors {
		t.Run(name, func(t *testing.T) {
			base := build()
			kr200 := base.Limit(200)
			require.Equal(t, -1, base.GetLimit(), "original must remain unchanged (immutable copy)")
			require.Equal(t, 200, kr200.GetLimit())
			kr100 := kr200.Limit(100)
			require.Equal(t, 200, kr200.GetLimit(), "chained intermediate must remain unchanged")
			require.Equal(t, 100, kr100.GetLimit(), "last-wins: latest Limit() call wins")
		})
	}
}

func TestMatchesStorageKey6Concrete(t *testing.T) {
	cases := []struct {
		name       string
		kr         KeyRange
		key        string
		wantMatch  bool
	}{
		{"KeysBt literal in", KeysBt("a", "z"), "m", true},
		{"KeysBt literal lo-boundary", KeysBt("a", "z"), "a", true},
		{"KeysBt literal hi-boundary", KeysBt("a", "z"), "z", false},
		{"KeysBt literal below", KeysBt("a", "z"), "0", false},
		{"KeysGt literal", KeysGt("p"), "q", true},
		{"KeysGt literal boundary", KeysGt("p"), "p", false},
		{"KeysGte literal", KeysGte("p"), "p", true},
		{"KeysGte literal below", KeysGte("p"), "o", false},
		{"KeysLt literal", KeysLt("p"), "o", true},
		{"KeysLt literal boundary", KeysLt("p"), "p", false},
		{"KeysLte literal", KeysLte("p"), "p", true},
		{"KeysLte literal above", KeysLte("p"), "q", false},
		{"KeysPattern glob star match", KeysPattern("user:*"), "user:001", true},
		{"KeysPattern glob star nomatch", KeysPattern("user:*"), "session:001", false},
		{"KeysPattern leading wildcard", KeysPattern("*:Engineering:*"), "user:Engineering:001", true},
		{"KeysGte pattern pivot range", KeysGte("u:05*"), "u:0599", true},
		{"KeysGte pattern pivot nomatch prefix", KeysGte("u:05*"), "u:0499", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.wantMatch, MatchesStorageKey(c.kr, c.key))
		})
	}
}

func TestUnmarshalKeyRangeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		kr   KeyRange
	}{
		{"KeysBt", KeysBt("user:0100", "user:0200")},
		{"KeysBt limit", KeysBt("a", "z").Limit(7)},
		{"KeysGt", KeysGt("p")},
		{"KeysGte", KeysGte("p")},
		{"KeysLt", KeysLt("p")},
		{"KeysLte", KeysLte("p")},
		{"KeysPattern", KeysPattern("user:*")},
		{"KeysPattern limit", KeysPattern("*").Limit(50)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := c.kr.MarshalJSON()
			require.NoError(t, err)
			parsed, err := UnmarshalKeyRange(b)
			require.NoError(t, err)
			// Op equivalence via Bounds+Pattern pair
			wantLo, wantHi := c.kr.Bounds()
			gotLo, gotHi := parsed.Bounds()
			require.Equal(t, wantLo, gotLo)
			require.Equal(t, wantHi, gotHi)
			wantGlob, wantOK := c.kr.Pattern()
			gotGlob, gotOK := parsed.Pattern()
			require.Equal(t, wantOK, gotOK)
			if wantOK {
				require.Equal(t, wantGlob, gotGlob)
			}
			// Note: UnmarshalKeyRange always returns GetLimit()==-1 by design (§FR RESP
			// contract: LIMIT count is a separate positional argc arg, not inside JSON).
			// So we do NOT assert GetLimit() equality across round-trip here.
		})
	}
}

func TestUnmarshalKeyRangeRejectsInvalidOps(t *testing.T) {
	badCases := []struct {
		name string
		json string
	}{
		{"plain string (legacy glob not json)", "user:*"},
		{"empty object no op", "{}"},
		{"unknown op", `{"op":"nope","pivot":"p"}`},
		{"bt missing both ge lt", `{"op":"bt"}`},
		{"gt missing pivot", `{"op":"gt"}`},
		{"gte missing pivot", `{"op":"gte"}`},
		{"lt missing pivot", `{"op":"lt"}`},
		{"lte missing pivot", `{"op":"lte"}`},
		{"pattern missing p", `{"op":"pattern"}`},
		{"malformed json", `{"op":"bt",`},
	}
	for _, c := range badCases {
		t.Run(c.name, func(t *testing.T) {
			_, err := UnmarshalKeyRange([]byte(c.json))
			require.Error(t, err, "expected error for input %q", c.json)
		})
	}
}

func TestNextLex(t *testing.T) {
	require.Equal(t, "abc\x00", NextLex("abc"))
}
