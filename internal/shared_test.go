package internal

import (
	"testing"
	"time"

	"github.com/kcmvp/redisx/x"
	"github.com/stretchr/testify/require"
)

type sharedUserDoc string

func (sharedUserDoc) Namespace() string  { return "user" }
func (sharedUserDoc) Mem() bool          { return false }
func (sharedUserDoc) KeyAttrs() []string { return []string{"id"} }
func (d sharedUserDoc) RawJSON() string  { return string(d) }
func (sharedUserDoc) TTL() time.Duration { return time.Hour }

func TestAuthKeyStable(t *testing.T) {
	first := AuthKey()
	second := AuthKey()

	if first == "" {
		t.Fatal("expected auth key to be generated")
	}
	if first != second {
		t.Fatalf("expected stable auth key, got %q and %q", first, second)
	}
}

func TestValidateKeyPattern(t *testing.T) {
	t.Run("accepts document scoped pattern", func(t *testing.T) {
		full, err := ValidateKeyPattern[sharedUserDoc]("*")
		require.NoError(t, err)
		require.Equal(t, "user:*", full)
	})

	t.Run("rejects full namespace", func(t *testing.T) {
		_, err := ValidateKeyPattern[sharedUserDoc]("user")
		require.EqualError(t, err, "key pattern must be document-scoped, got storage pattern: user")
	})

	t.Run("rejects prefixed storage pattern", func(t *testing.T) {
		_, err := ValidateKeyPattern[sharedUserDoc]("user:*")
		require.EqualError(t, err, "key pattern must be document-scoped, got storage pattern: user:*")
	})
}

func TestValidateIdxName(t *testing.T) {
	t.Run("accepts logical name", func(t *testing.T) {
		full, err := ValidateIdxName[sharedUserDoc]("age")
		require.NoError(t, err)
		require.Equal(t, "user_age", full)
	})

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := ValidateIdxName[sharedUserDoc]("")
		require.EqualError(t, err, "index name is required")
	})

	t.Run("rejects fully qualified name", func(t *testing.T) {
		_, err := ValidateIdxName[sharedUserDoc]("user_age")
		require.EqualError(t, err, "idx name must be logical, got fully-qualified index name: user_age")
	})
}

func TestScopeKeyRange(t *testing.T) {
	t.Run("KeysPattern scopes prefix and carries limit", func(t *testing.T) {
		scoped := x.KeysPattern("p05*").Limit(7)
		got, err := ScopeKeyRange[sharedUserDoc](scoped)
		require.NoError(t, err)
		kind, pa, pb, lim := x.InspectKeyRange(got)
		require.Equal(t, x.KeyRangePattern, kind)
		require.Equal(t, "user:p05*", pa)
		require.Empty(t, pb)
		require.Equal(t, 7, lim)
	})
	t.Run("KeysBt prefixes both bounds, supports empty ge/lt", func(t *testing.T) {
		scoped := x.KeysBt("p020", "p070")
		got, err := ScopeKeyRange[sharedUserDoc](scoped)
		require.NoError(t, err)
		kind, ge, lt, _ := x.InspectKeyRange(got)
		require.Equal(t, x.KeyRangeBt, kind)
		require.Equal(t, "user:p020", ge)
		require.Equal(t, "user:p070", lt)
	})
	t.Run("KeysBt with empty ge falls back to full namespace", func(t *testing.T) {
		scoped := x.KeysBt("", "p100")
		got, err := ScopeKeyRange[sharedUserDoc](scoped)
		require.NoError(t, err)
		_, ge, _, _ := x.InspectKeyRange(got)
		require.Equal(t, "user", ge)
	})
	t.Run("KeysBt with empty lt falls back to full namespace", func(t *testing.T) {
		scoped := x.KeysBt("p000", "")
		got, err := ScopeKeyRange[sharedUserDoc](scoped)
		require.NoError(t, err)
		_, _, lt, _ := x.InspectKeyRange(got)
		require.Equal(t, "user", lt)
	})
	t.Run("KeysGte scopes pivot", func(t *testing.T) {
		got, err := ScopeKeyRange[sharedUserDoc](x.KeysGte("p050"))
		require.NoError(t, err)
		kind, pivotA, _, _ := x.InspectKeyRange(got)
		require.Equal(t, x.KeyRangeGte, kind)
		require.Equal(t, "user:p050", pivotA)
	})
	t.Run("KeysGt scopes pivot", func(t *testing.T) {
		got, err := ScopeKeyRange[sharedUserDoc](x.KeysGt("p050"))
		require.NoError(t, err)
		kind, pivotA, _, _ := x.InspectKeyRange(got)
		require.Equal(t, x.KeyRangeGt, kind)
		require.Equal(t, "user:p050", pivotA)
	})
	t.Run("KeysLte scopes pivot", func(t *testing.T) {
		got, err := ScopeKeyRange[sharedUserDoc](x.KeysLte("p049"))
		require.NoError(t, err)
		kind, pivotA, _, _ := x.InspectKeyRange(got)
		require.Equal(t, x.KeyRangeLte, kind)
		require.Equal(t, "user:p049", pivotA)
	})
	t.Run("KeysLt scopes pivot", func(t *testing.T) {
		got, err := ScopeKeyRange[sharedUserDoc](x.KeysLt("p050"))
		require.NoError(t, err)
		kind, pivotA, _, _ := x.InspectKeyRange(got)
		require.Equal(t, x.KeyRangeLt, kind)
		require.Equal(t, "user:p050", pivotA)
	})
	t.Run("no limit on input leaves limit=-1 on output (unset)", func(t *testing.T) {
		got, err := ScopeKeyRange[sharedUserDoc](x.KeysPattern("p05*"))
		require.NoError(t, err)
		_, _, _, lim := x.InspectKeyRange(got)
		require.Equal(t, -1, lim)
	})
	t.Run("rejects already prefixed pattern", func(t *testing.T) {
		_, err := ScopeKeyRange[sharedUserDoc](x.KeysPattern("user:p05*"))
		require.ErrorContains(t, err, "document-scoped")
		require.ErrorContains(t, err, "p")
	})
	t.Run("rejects already prefixed Bt ge", func(t *testing.T) {
		_, err := ScopeKeyRange[sharedUserDoc](x.KeysBt("user:p020", "p070"))
		require.ErrorContains(t, err, "ge")
	})
	t.Run("rejects already prefixed Bt lt", func(t *testing.T) {
		_, err := ScopeKeyRange[sharedUserDoc](x.KeysBt("p020", "user:p070"))
		require.ErrorContains(t, err, "lt")
	})
	t.Run("rejects already prefixed Gt pivot", func(t *testing.T) {
		_, err := ScopeKeyRange[sharedUserDoc](x.KeysGt("user:p050"))
		require.ErrorContains(t, err, "pivot")
	})
	t.Run("rejects already prefixed Gte pivot", func(t *testing.T) {
		_, err := ScopeKeyRange[sharedUserDoc](x.KeysGte("user:p050"))
		require.ErrorContains(t, err, "pivot")
	})
	t.Run("rejects already prefixed Lt pivot", func(t *testing.T) {
		_, err := ScopeKeyRange[sharedUserDoc](x.KeysLt("user:p050"))
		require.ErrorContains(t, err, "pivot")
	})
	t.Run("rejects already prefixed Lte pivot", func(t *testing.T) {
		_, err := ScopeKeyRange[sharedUserDoc](x.KeysLte("user:p050"))
		require.ErrorContains(t, err, "pivot")
	})
	t.Run("unknown op propagates error via marshal round trip unrecognized", func(t *testing.T) {
		unknownBytes := []byte(`{"op":"surprise","pivot":"x"}`)
		_, derr := x.UnmarshalKeyRange(unknownBytes)
		require.Error(t, derr)
		require.ErrorContains(t, derr, "unknown key range op")
	})
}

// ————— §11 SSoT Naming Helpers tests —————

func TestSplitStorageKey(t *testing.T) {
	cases := []struct {
		in       string
		wantNs   string
		wantPk   string
		wantErr  bool
		errMatch string
	}{
		{in: "user", wantNs: "user", wantPk: ""},
		{in: "user:acme_0100", wantNs: "user", wantPk: "acme_0100"},
		{in: "_m_user:tenant5_id99", wantNs: "_m_user", wantPk: "tenant5_id99"},
		{in: "_doc_user", wantNs: "_doc_user", wantPk: ""},
		{in: "a:b:c:multi", wantNs: "a", wantPk: "b:c:multi"}, // only FIRST colon separates ns/pk
		{in: "", wantErr: true, errMatch: "empty storage key"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			ns, pk, err := SplitStorageKey(c.in)
			if c.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), c.errMatch)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.wantNs, ns)
			require.Equal(t, c.wantPk, pk)
		})
	}
}

func TestIsInternalStorageNs(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"_doc_user", true},
		{"_idx_user_age", true},
		{"_auth_default", true},
		{"_doc_", true}, // prefix match, even without suffix
		{"_idx_", true},
		{"user", false},
		{"_m_user", false},  // mem layer marker is NOT internal meta
		{"doc_user", false}, // missing leading underscore
		{"idx_user_age", false},
		{"auth_foo", false},
		{"", false},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			require.Equal(t, c.want, IsInternalStorageNs(c.in))
		})
	}
}

func TestStripMemPrefix(t *testing.T) {
	cases := []struct{ in, want string }{
		{"_m_user", "user"},
		{"_m_user:abc", "user:abc"},
		{"_m__m_double", "_m_double"},
		{"user", "user"},
		{"_doc_user", "_doc_user"}, // _doc_ preserved
		{"_idx_user_age", "_idx_user_age"},
		{"auth_foo", "auth_foo"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			require.Equal(t, c.want, StripMemPrefix(c.in))
		})
	}
}

func TestExtractPKSuffixes(t *testing.T) {
	// empty in → nil slice, nil err
	out, err := ExtractPKSuffixes("")
	require.NoError(t, err)
	require.Nil(t, out)

	cases := []struct {
		in   string
		want []string
	}{
		{"acme_0100", []string{"acme", "0100"}},
		{"just_one", []string{"just", "one"}}, // "_" in pk-suffixes is the delimiter, not literal
		{"tenant5_id99", []string{"tenant5", "id99"}},
		{"a_b_c", []string{"a", "b", "c"}},
		{"_leading", []string{"", "leading"}},
		{"trailing_", []string{"trailing", ""}},
		{"no-delimiter", []string{"no-delimiter"}}, // no underscores → 1-element slice
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			out, err := ExtractPKSuffixes(c.in)
			require.NoError(t, err)
			require.Equal(t, c.want, out)
		})
	}
}

func TestParseIndexFullName(t *testing.T) {
	good := []struct {
		in          string
		wantNs      string
		wantLogical string
	}{
		{"user_age", "user", "age"},
		{"_m_cache_hitratio", "_m_cache", "hitratio"},
		{"a_b_c", "a_b", "c"},                           // LAST underscore separates owner/logical
		{"__under_ns_logical", "__under_ns", "logical"}, // leading underscores in ns OK
	}
	for _, c := range good {
		t.Run("ok/"+c.in, func(t *testing.T) {
			ns, log, err := ParseIndexFullName(c.in)
			require.NoError(t, err)
			require.Equal(t, c.wantNs, ns)
			require.Equal(t, c.wantLogical, log)
		})
	}

	bad := []struct {
		in       string
		errMatch string
	}{
		{"", "empty index full name"},
		{"nouderscores", "no join separator"},
		{"_trailing_", "empty logical suffix"},
		{"_prefix_", "empty logical suffix"},
		{"_emptyowner", "empty owner storage namespace"},
	}
	for _, c := range bad {
		t.Run("err/"+c.in, func(t *testing.T) {
			_, _, err := ParseIndexFullName(c.in)
			require.Error(t, err)
			require.Contains(t, err.Error(), c.errMatch)
		})
	}
}
