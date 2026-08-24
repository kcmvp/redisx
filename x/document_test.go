package x

import (
	"testing"
	"time"

	naming "github.com/kcmvp/redisx/internal/naming"
	"github.com/stretchr/testify/require"
)

type userDoc string

func (userDoc) Namespace() string  { return "user" }
func (userDoc) Mem() bool          { return false }
func (userDoc) KeyAttrs() []string { return []string{"id"} }
func (u userDoc) RawJSON() string  { return string(u) }
func (userDoc) TTL() time.Duration { return time.Hour }

type sameUserDoc string

func (sameUserDoc) Namespace() string  { return "user" }
func (sameUserDoc) Mem() bool          { return false }
func (sameUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u sameUserDoc) RawJSON() string  { return string(u) }
func (sameUserDoc) TTL() time.Duration { return time.Hour }

type memUserDoc string

func (memUserDoc) Namespace() string  { return "user" }
func (memUserDoc) Mem() bool          { return true }
func (memUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u memUserDoc) RawJSON() string  { return string(u) }
func (memUserDoc) TTL() time.Duration { return time.Hour }

type emptyPrefixDoc string

func (emptyPrefixDoc) Namespace() string  { return "" }
func (emptyPrefixDoc) Mem() bool          { return false }
func (emptyPrefixDoc) KeyAttrs() []string { return []string{"id"} }
func (u emptyPrefixDoc) RawJSON() string  { return string(u) }
func (emptyPrefixDoc) TTL() time.Duration { return time.Hour }

type separatorPrefixDoc string

func (separatorPrefixDoc) Namespace() string  { return "user:admin" }
func (separatorPrefixDoc) Mem() bool          { return false }
func (separatorPrefixDoc) KeyAttrs() []string { return []string{"id"} }
func (u separatorPrefixDoc) RawJSON() string  { return string(u) }
func (separatorPrefixDoc) TTL() time.Duration { return time.Hour }

type wildcardPrefixDoc string

func (wildcardPrefixDoc) Namespace() string  { return "user*admin" }
func (wildcardPrefixDoc) Mem() bool          { return false }
func (wildcardPrefixDoc) KeyAttrs() []string { return []string{"id"} }
func (u wildcardPrefixDoc) RawJSON() string  { return string(u) }
func (wildcardPrefixDoc) TTL() time.Duration { return time.Hour }

type questionPrefixDoc string

func (questionPrefixDoc) Namespace() string  { return "user?admin" }
func (questionPrefixDoc) Mem() bool          { return false }
func (questionPrefixDoc) KeyAttrs() []string { return []string{"id"} }
func (u questionPrefixDoc) RawJSON() string  { return string(u) }
func (questionPrefixDoc) TTL() time.Duration { return time.Hour }

type invalidPrefixDoc string

func (invalidPrefixDoc) Namespace() string  { return "user_admin" }
func (invalidPrefixDoc) Mem() bool          { return false }
func (invalidPrefixDoc) KeyAttrs() []string { return []string{"id"} }
func (u invalidPrefixDoc) RawJSON() string  { return string(u) }
func (invalidPrefixDoc) TTL() time.Duration { return time.Hour }

type bareMemPrefixDoc string

func (bareMemPrefixDoc) Namespace() string  { return naming.MemNsPrefix() }
func (bareMemPrefixDoc) Mem() bool          { return false }
func (bareMemPrefixDoc) KeyAttrs() []string { return []string{"id"} }
func (u bareMemPrefixDoc) RawJSON() string  { return string(u) }
func (bareMemPrefixDoc) TTL() time.Duration { return time.Hour }

type emptyKeyAttrDoc string

func (emptyKeyAttrDoc) Namespace() string  { return "userempty" }
func (emptyKeyAttrDoc) Mem() bool          { return false }
func (emptyKeyAttrDoc) KeyAttrs() []string { return []string{""} }
func (u emptyKeyAttrDoc) RawJSON() string  { return string(u) }
func (emptyKeyAttrDoc) TTL() time.Duration { return time.Hour }

type multiKeyDoc string

func (multiKeyDoc) Namespace() string  { return "tenantuser" }
func (multiKeyDoc) Mem() bool          { return false }
func (multiKeyDoc) KeyAttrs() []string { return []string{"tenant", "id"} }
func (u multiKeyDoc) RawJSON() string  { return string(u) }
func (multiKeyDoc) TTL() time.Duration { return time.Hour }

type noKeyAttrDoc string

func (noKeyAttrDoc) Namespace() string  { return "plain" }
func (noKeyAttrDoc) Mem() bool          { return false }
func (noKeyAttrDoc) KeyAttrs() []string { return nil }
func (u noKeyAttrDoc) RawJSON() string  { return string(u) }
func (noKeyAttrDoc) TTL() time.Duration { return time.Hour }

type boolKeyDoc string

func (boolKeyDoc) Namespace() string  { return "flagdoc" }
func (boolKeyDoc) Mem() bool          { return false }
func (boolKeyDoc) KeyAttrs() []string { return []string{"enabled"} }
func (u boolKeyDoc) RawJSON() string  { return string(u) }
func (boolKeyDoc) TTL() time.Duration { return time.Hour }

type memBoolKeyDoc string

func (memBoolKeyDoc) Namespace() string  { return "flagdoc" }
func (memBoolKeyDoc) Mem() bool          { return true }
func (memBoolKeyDoc) KeyAttrs() []string { return []string{"enabled"} }
func (u memBoolKeyDoc) RawJSON() string  { return string(u) }
func (memBoolKeyDoc) TTL() time.Duration { return time.Hour }

type invalidNsDoc string

func (invalidNsDoc) Namespace() string  { return "_bad_ns" }
func (invalidNsDoc) Mem() bool          { return false }
func (invalidNsDoc) KeyAttrs() []string { return []string{"id"} }
func (u invalidNsDoc) RawJSON() string  { return string(u) }
func (invalidNsDoc) TTL() time.Duration { return time.Hour }

func TestStorageKeyValue(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "joins namespace and key value",
			run: func(t *testing.T) {
				require.Equal(t, "user:201", StorageKeyValue[userDoc]("201"))
			},
		},
		{
			name: "returns namespace when value is empty",
			run: func(t *testing.T) {
				require.Equal(t, "user", StorageKeyValue[userDoc](""))
			},
		},
		{
			name: "allows compatible document reuse",
			run: func(t *testing.T) {
				require.Equal(t, "user:201", StorageKeyValue[userDoc]("201"))
				require.Equal(t, "user:202", StorageKeyValue[sameUserDoc]("202"))
			},
		},
		{
			name: "allows mem prefix",
			run: func(t *testing.T) {
				require.Equal(t, "_m_:user:201", StorageKeyValue[memUserDoc]("201"))
			},
		},
		{
			name: "rejects empty prefix",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					StorageKeyValue[emptyPrefixDoc]("201")
				})
			},
		},
		{
			name: "rejects separator in prefix",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					StorageKeyValue[separatorPrefixDoc]("201")
				})
			},
		},
		{
			name: "rejects wildcard in prefix",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					StorageKeyValue[wildcardPrefixDoc]("201")
				})
			},
		},
		{
			name: "rejects question mark in prefix",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					StorageKeyValue[questionPrefixDoc]("201")
				})
			},
		},
		{
			name: "rejects underscore in prefix",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					StorageKeyValue[invalidPrefixDoc]("201")
				})
			},
		},
		{
			name: "rejects bare mem prefix",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					StorageKeyValue[bareMemPrefixDoc]("201")
				})
			},
		},
		{
			name: "rejects empty key attr path during registration",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					StorageKeyValue[emptyKeyAttrDoc]("201")
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func TestMemKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "adds mem prefix", key: "user:1", want: "_m_:user:1"},
		{name: "keeps existing mem prefix (new shape)", key: "_m_:user:1", want: "_m_:user:1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, MemKey(tt.key))
		})
	}
}

func TestStorageKey(t *testing.T) {
	tests := []struct {
		name    string
		run     func() (string, error)
		want    string
		wantErr string
	}{
		{
			name: "uses single key attr from raw json",
			run: func() (string, error) {
				return StorageKey(userDoc(`{"id":"202","name":"Bob"}`))
			},
			want: "user:202",
		},
		{
			name: "joins multiple key attrs with canonical underscore join (naming.JoinPKAttrValues)",
			run: func() (string, error) {
				return StorageKey(multiKeyDoc(`{"tenant":"acme","id":"202"}`))
			},
			want: "tenantuser:acme_202",
		},
		{
			name: "returns prefix when document has no key attrs",
			run: func() (string, error) {
				return StorageKey(noKeyAttrDoc(`{"name":"Bob"}`))
			},
			want: "plain",
		},
		{
			name: "normalizes true bool key attr to one",
			run: func() (string, error) {
				return StorageKey(boolKeyDoc(`{"enabled":true}`))
			},
			want: "flagdoc:1",
		},
		{
			name: "normalizes false bool key attr to zero",
			run: func() (string, error) {
				return StorageKey(boolKeyDoc(`{"enabled":false}`))
			},
			want: "flagdoc:0",
		},
		{
			name: "rejects empty key attr path",
			run: func() (string, error) {
				return StorageKey(emptyKeyAttrDoc(`{"id":"202"}`))
			},
			wantErr: "key attr path is empty",
		},
		{
			name: "rejects missing key attr",
			run: func() (string, error) {
				return StorageKey(userDoc(`{"name":"Bob"}`))
			},
			wantErr: "missing key attr: id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.run()
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIdx(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "builds lowercased full name and scoped pattern",
			run: func(t *testing.T) {
				idx := Idx[userDoc]("ByAge", "tenant:*", "profile.age")
				require.Equal(t, "user_byage", idx.Name())
				require.Equal(t, "user:tenant:*", idx.KeyPattern())
				require.Equal(t, "profile_age", idx.Path())
			},
		},
		{
			name: "rejects empty key pattern",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					Idx[userDoc]("by_age", "", "age")
				})
			},
		},
		{
			name: "rejects key pattern starting with separator",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					Idx[userDoc]("by_age", ":tenant:*", "age")
				})
			},
		},
		{
			name: "rejects empty name",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					Idx[userDoc]("", "*", "age")
				})
			},
		},
		{
			name: "rejects empty json path",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					Idx[userDoc]("by_age", "*", "")
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func TestRawIndex(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "builds lowercased name and normalizes json path dots to underscores",
			run: func(t *testing.T) {
				idx := RawIndex("ScoreRank", "tenant:user:*", "metrics.activity.score")
				require.Equal(t, "scorerank", idx.Name())
				require.Equal(t, "tenant:user:*", idx.KeyPattern())
				require.Equal(t, "metrics_activity_score", idx.Path())
			},
		},
		{
			name: "rejects empty name",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					RawIndex("", "user:*", "age")
				})
			},
		},
		{
			name: "rejects empty key pattern",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					RawIndex("by_age", "", "age")
				})
			},
		},
		{
			name: "rejects empty json path",
			run: func(t *testing.T) {
				require.Panics(t, func() {
					RawIndex("by_age", "user:*", "")
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func TestKey(t *testing.T) {
	tests := []struct {
		name    string
		run     func() (string, error)
		want    string
		wantErr string
	}{
		{
			name: "resolves disk doc single key attr",
			run: func() (string, error) {
				return Key[userDoc](`{"id":"202","name":"Bob"}`)
			},
			want: "user:202",
		},
		{
			name: "resolves mem doc single key attr",
			run: func() (string, error) {
				return Key[memUserDoc](`{"id":"202"}`)
			},
			want: "_m_:user:202",
		},
		{
			name: "resolves multi key attrs joined with underscore",
			run: func() (string, error) {
				return Key[multiKeyDoc](`{"tenant":"acme","id":"202"}`)
			},
			want: "tenantuser:acme_202",
		},
		{
			name: "returns ns only when key attrs empty",
			run: func() (string, error) {
				return Key[noKeyAttrDoc](`{"name":"Bob"}`)
			},
			want: "plain",
		},
		{
			name: "normalizes true bool to 1",
			run: func() (string, error) {
				return Key[boolKeyDoc](`{"enabled":true}`)
			},
			want: "flagdoc:1",
		},
		{
			name: "normalizes false bool to 0 on mem doc",
			run: func() (string, error) {
				return Key[memBoolKeyDoc](`{"enabled":false}`)
			},
			want: "_m_:flagdoc:0",
		},
		{
			name: "returns error when namespace is invalid",
			run: func() (string, error) {
				return Key[invalidNsDoc](`{"id":"1"}`)
			},
			wantErr: "document namespace invalid:",
		},
		{
			name: "returns error for empty key attr path",
			run: func() (string, error) {
				return Key[emptyKeyAttrDoc](`{"id":"1"}`)
			},
			wantErr: "key attr path is empty",
		},
		{
			name: "returns error for missing key attr",
			run: func() (string, error) {
				return Key[userDoc](`{"name":"Bob"}`)
			},
			wantErr: "missing key attr: id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.run()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStorageNsKeyPattern(t *testing.T) {
	require.Equal(t, "user:tenant:*", StorageNsKeyPattern[userDoc]("tenant:*"))
	require.Equal(t, "_m_:user:*", StorageNsKeyPattern[memUserDoc]("*"))
	require.Equal(t, "tenantuser:*", StorageNsKeyPattern[multiKeyDoc]("*"))
	require.Panics(t, func() {
		StorageNsKeyPattern[invalidNsDoc]("*")
	})
}

func TestValidateKeyPattern(t *testing.T) {
	t.Run("accepts document scoped pattern", func(t *testing.T) {
		full, err := ValidateKeyPattern[userDoc]("*")
		require.NoError(t, err)
		require.Equal(t, "user:*", full)
	})

	t.Run("rejects full namespace", func(t *testing.T) {
		_, err := ValidateKeyPattern[userDoc]("user")
		require.EqualError(t, err, "key pattern must be document-scoped, got storage pattern: user")
	})

	t.Run("rejects prefixed storage pattern", func(t *testing.T) {
		_, err := ValidateKeyPattern[userDoc]("user:*")
		require.EqualError(t, err, "key pattern must be document-scoped, got storage pattern: user:*")
	})
}

func TestValidateIdxName(t *testing.T) {
	t.Run("accepts logical name", func(t *testing.T) {
		full, err := ValidateIdxName[userDoc]("age")
		require.NoError(t, err)
		require.Equal(t, "user_age", full)
	})

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := ValidateIdxName[userDoc]("")
		require.EqualError(t, err, "index name is required")
	})

	t.Run("rejects fully qualified name", func(t *testing.T) {
		_, err := ValidateIdxName[userDoc]("user_age")
		require.EqualError(t, err, "idx name must be logical, got fully-qualified index name: user_age")
	})
}

func TestScopeKeyRange(t *testing.T) {
	t.Run("KeysPattern scopes prefix and carries limit", func(t *testing.T) {
		scoped := KeysPattern("p05*").Limit(7)
		got, err := ScopeKeyRange[userDoc](scoped)
		require.NoError(t, err)
		kind, pa, pb, lim := InspectKeyRange(got)
		require.Equal(t, KeyRangePattern, kind)
		require.Equal(t, "user:p05*", pa)
		require.Empty(t, pb)
		require.Equal(t, 7, lim)
	})
	t.Run("KeysBt prefixes both bounds, supports empty ge/lt", func(t *testing.T) {
		scoped := KeysBt("p020", "p070")
		got, err := ScopeKeyRange[userDoc](scoped)
		require.NoError(t, err)
		kind, ge, lt, _ := InspectKeyRange(got)
		require.Equal(t, KeyRangeBt, kind)
		require.Equal(t, "user:p020", ge)
		require.Equal(t, "user:p070", lt)
	})
	t.Run("KeysBt with empty ge falls back to full namespace", func(t *testing.T) {
		scoped := KeysBt("", "p100")
		got, err := ScopeKeyRange[userDoc](scoped)
		require.NoError(t, err)
		_, ge, _, _ := InspectKeyRange(got)
		require.Equal(t, "user", ge)
	})
	t.Run("KeysBt with empty lt falls back to full namespace", func(t *testing.T) {
		scoped := KeysBt("p000", "")
		got, err := ScopeKeyRange[userDoc](scoped)
		require.NoError(t, err)
		_, _, lt, _ := InspectKeyRange(got)
		require.Equal(t, "user", lt)
	})
	t.Run("KeysGte scopes pivot", func(t *testing.T) {
		got, err := ScopeKeyRange[userDoc](KeysGte("p050"))
		require.NoError(t, err)
		kind, pivotA, _, _ := InspectKeyRange(got)
		require.Equal(t, KeyRangeGte, kind)
		require.Equal(t, "user:p050", pivotA)
	})
	t.Run("KeysGt scopes pivot", func(t *testing.T) {
		got, err := ScopeKeyRange[userDoc](KeysGt("p050"))
		require.NoError(t, err)
		kind, pivotA, _, _ := InspectKeyRange(got)
		require.Equal(t, KeyRangeGt, kind)
		require.Equal(t, "user:p050", pivotA)
	})
	t.Run("KeysLte scopes pivot", func(t *testing.T) {
		got, err := ScopeKeyRange[userDoc](KeysLte("p049"))
		require.NoError(t, err)
		kind, pivotA, _, _ := InspectKeyRange(got)
		require.Equal(t, KeyRangeLte, kind)
		require.Equal(t, "user:p049", pivotA)
	})
	t.Run("KeysLt scopes pivot", func(t *testing.T) {
		got, err := ScopeKeyRange[userDoc](KeysLt("p050"))
		require.NoError(t, err)
		kind, pivotA, _, _ := InspectKeyRange(got)
		require.Equal(t, KeyRangeLt, kind)
		require.Equal(t, "user:p050", pivotA)
	})
	t.Run("no limit on input leaves limit=-1 on output (unset)", func(t *testing.T) {
		got, err := ScopeKeyRange[userDoc](KeysPattern("p05*"))
		require.NoError(t, err)
		_, _, _, lim := InspectKeyRange(got)
		require.Equal(t, -1, lim)
	})
	t.Run("rejects already prefixed pattern", func(t *testing.T) {
		_, err := ScopeKeyRange[userDoc](KeysPattern("user:p05*"))
		require.ErrorContains(t, err, "document-scoped")
		require.ErrorContains(t, err, "p")
	})
	t.Run("rejects already prefixed Bt ge", func(t *testing.T) {
		_, err := ScopeKeyRange[userDoc](KeysBt("user:p020", "p070"))
		require.ErrorContains(t, err, "ge")
	})
	t.Run("rejects already prefixed Bt lt", func(t *testing.T) {
		_, err := ScopeKeyRange[userDoc](KeysBt("p020", "user:p070"))
		require.ErrorContains(t, err, "lt")
	})
	t.Run("rejects already prefixed Gt pivot", func(t *testing.T) {
		_, err := ScopeKeyRange[userDoc](KeysGt("user:p050"))
		require.ErrorContains(t, err, "pivot")
	})
	t.Run("rejects already prefixed Gte pivot", func(t *testing.T) {
		_, err := ScopeKeyRange[userDoc](KeysGte("user:p050"))
		require.ErrorContains(t, err, "pivot")
	})
	t.Run("rejects already prefixed Lt pivot", func(t *testing.T) {
		_, err := ScopeKeyRange[userDoc](KeysLt("user:p050"))
		require.ErrorContains(t, err, "pivot")
	})
	t.Run("rejects already prefixed Lte pivot", func(t *testing.T) {
		_, err := ScopeKeyRange[userDoc](KeysLte("user:p050"))
		require.ErrorContains(t, err, "pivot")
	})
	t.Run("unknown op propagates error via marshal round trip unrecognized", func(t *testing.T) {
		unknownBytes := []byte(`{"op":"surprise","pivot":"x"}`)
		_, derr := UnmarshalKeyRange(unknownBytes)
		require.Error(t, derr)
		require.ErrorContains(t, derr, "unknown key range op")
	})
}
