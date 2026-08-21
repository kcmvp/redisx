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
