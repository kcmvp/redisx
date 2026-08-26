package x

import (
	"strings"
	"testing"
	"time"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/stretchr/testify/require"
)

// cheatsheetUserDoc / cheatsheetSessionDoc are the EXACT reference types
// from x/document.go "RESP Wire Cheatsheet §0". Any change to Namespace /
// Mem / KeyAttrs / TTL in the cheatsheet comment MUST be mirrored here and
// TestCheatsheetSchemaReference_ExactKeyCoordinates re-run to stay in sync.
//
//	UserDoc    (disk) Namespace="user"    Mem=false KeyAttrs=["tenant","id"] TTL=24h
//	SessionDoc (mem)  Namespace="session" Mem=true  KeyAttrs=["sid"]          TTL=30m
type cheatsheetUserDoc string

func (cheatsheetUserDoc) Namespace() string  { return "user" }
func (cheatsheetUserDoc) Mem() bool          { return false }
func (cheatsheetUserDoc) KeyAttrs() []string { return []string{"tenant", "id"} }
func (u cheatsheetUserDoc) RawJSON() string  { return string(u) }
func (cheatsheetUserDoc) TTL() time.Duration { return 24 * time.Hour }

type cheatsheetSessionDoc string

func (cheatsheetSessionDoc) Namespace() string  { return "session" }
func (cheatsheetSessionDoc) Mem() bool          { return true }
func (cheatsheetSessionDoc) KeyAttrs() []string { return []string{"sid"} }
func (u cheatsheetSessionDoc) RawJSON() string  { return string(u) }
func (cheatsheetSessionDoc) TTL() time.Duration { return 30 * time.Minute }

// TestCheatsheetSchemaReference_ExactKeyCoordinates pins the EXACT 10
// coordinate values written in x/document.go RESP Wire Cheatsheet §0
// (L476-L502) against x-package APIs directly — because x is the SSoT
// contract layer for Document key-derivation logic, not internal/naming.
//
// Covered per reference type:
//   - storageNs          (BuildStorageNs from Namespace()+Mem())
//   - full key value     (Key[T] / StorageKey, for the exact attrs from the
//     cheatsheet comment: user={tenant:"acme", id:"7"} /
//     session={sid:"abc"})
//   - doc meta key       (_doc_:{storageNs})
//   - index full name    (Idx[T].Name / ValidateIdxName — logical index
//     names match cheatsheet: "age" / "last")
//   - index meta key     (_idx_:{indexFullName})
func TestCheatsheetSchemaReference_ExactKeyCoordinates(t *testing.T) {
	// ------------------------------------------------------------
	// UserDoc (disk)
	// ------------------------------------------------------------
	userAcme7 := cheatsheetUserDoc(`{"tenant":"acme","id":"7","age":30,"status":"cold"}`)
	require.Equal(t, "user",
		naming.BuildStorageNs(cheatsheetUserDoc("").Namespace(), cheatsheetUserDoc("").Mem()),
		"cheatsheet UserDoc storageNs = Namespace(\"user\") + Mem=false → \"user\"")

	userFullByGenericAPI, err := Key[cheatsheetUserDoc](string(userAcme7))
	require.NoError(t, err)
	userFullByMethod, err := StorageKey(userAcme7)
	require.NoError(t, err)
	require.Equal(t, "user:acme_7", userFullByGenericAPI,
		"cheatsheet: user:{tenant}_{id} for {acme,7} → Key[T] = user:acme_7")
	require.Equal(t, userFullByGenericAPI, userFullByMethod,
		"Key[T] and StorageKey(d) must agree on the full storage key")
	require.Equal(t, "acme_7",
		naming.JoinPKAttrValues([]string{"acme", "7"}),
		"cheatsheet suffix = {tenant}_{id} for {acme,7} → \"acme_7\"")

	require.Equal(t, "_doc_:user:v_placeholder",
		naming.DocMetaKey(naming.BuildStorageNs("user", false), "placeholder"),
		"cheatsheet UserDoc doc meta = _doc_:{storageNs}:v_<12hex> → \"_doc_:user:v_placeholder\"")

	userAgeIdxName, err := ValidateIdxName[cheatsheetUserDoc]("age")
	require.NoError(t, err)
	require.Equal(t, "user:age", userAgeIdxName,
		"cheatsheet UserDoc index \"age\" full name = {storageNs}:age → \"user:age\"")
	require.Equal(t, userAgeIdxName, Idx[cheatsheetUserDoc]("age", "*", "age").Name(),
		"ValidateIdxName[T] and Idx[T].Name must agree on the full index name")
	require.Equal(t, "_idx_:user:age:v_placeholder", naming.IdxMetaKey(userAgeIdxName, "placeholder"),
		"cheatsheet UserDoc index meta = _idx_:{indexFullName}:v_<12hex> → \"_idx_:user:age:v_placeholder\"")

	// ------------------------------------------------------------
	// SessionDoc (mem)
	// ------------------------------------------------------------
	sessionAbc := cheatsheetSessionDoc(`{"sid":"abc","user_id":1,"last_ts":1000}`)
	require.Equal(t, "_m_:session",
		naming.BuildStorageNs(cheatsheetSessionDoc("").Namespace(), cheatsheetSessionDoc("").Mem()),
		"cheatsheet SessionDoc storageNs = Mem=true → \"_m_:session\"")

	sessFullByGenericAPI, err := Key[cheatsheetSessionDoc](string(sessionAbc))
	require.NoError(t, err)
	sessFullByMethod, err := StorageKey(sessionAbc)
	require.NoError(t, err)
	require.Equal(t, "_m_:session:abc", sessFullByGenericAPI,
		"cheatsheet: _m_:session:{sid} for {abc} → Key[T] = _m_:session:abc")
	require.Equal(t, sessFullByGenericAPI, sessFullByMethod,
		"Key[T] and StorageKey(d) must agree on mem full storage key")

	require.Equal(t, "_doc_:_m_:session:v_placeholder",
		naming.DocMetaKey(naming.BuildStorageNs("session", true), "placeholder"),
		"cheatsheet SessionDoc doc meta = _doc_:_m_:session:v_<12hex>")

	sessLastIdxName, err := ValidateIdxName[cheatsheetSessionDoc]("last")
	require.NoError(t, err)
	require.Equal(t, "_m_:session:last", sessLastIdxName,
		"cheatsheet SessionDoc index \"last\" full name = _m_:session:last")
	require.Equal(t, sessLastIdxName, Idx[cheatsheetSessionDoc]("last", "*", "last_ts").Name(),
		"ValidateIdxName[T] and Idx[T].Name must agree on mem index name")
	require.Equal(t, "_idx_:_m_:session:last:v_placeholder", naming.IdxMetaKey(sessLastIdxName, "placeholder"),
		"cheatsheet SessionDoc index meta = _idx_:_m_:session:last:v_<12hex>")
}

// TestCheatsheetSchemaReference_KeyAttrValueContainingColonRejects mirrors
// Schema.KeyAttrs doc "A pk value that itself contains ':' after joining is
// rejected by Strict Gate (validatePKSuffixNoColon)".
//
// Verified at the x SSoT layer by showing the naming helper used by all
// key builders (BuildStorageKey) panics with illegal ':' when a joined
// pkSuffix contains ':', which is exactly what Strict Gate enforces at
// the command entry before ever calling BuildStorageKey.
func TestCheatsheetSchemaReference_KeyAttrValueContainingColonRejects(t *testing.T) {
	cases := []struct {
		name   string
		values []string
	}{
		{"UserDoc tenant has colon", []string{"acme:eu", "7"}},
		{"UserDoc id has colon", []string{"acme", "7:prod"}},
		{"SessionDoc sid has colon", []string{"abc:preauth"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			joined := naming.JoinPKAttrValues(c.values)
			if !strings.Contains(joined, ":") {
				t.Fatalf("test data broken: JoinPKAttrValues(%v)=%q should contain ':'", c.values, joined)
			}
			require.PanicsWithValue(t,
				"naming.BuildStorageKey: pkSuffix \""+joined+"\" contains illegal ':' "+
					"(storage-key separator); multi-segment pk must join with '_'",
				func() { _ = naming.BuildStorageKey("user", joined) },
				"BuildStorageKey must panic to enforce no ':' inside pk suffix — "+
					"matches Schema.KeyAttrs doc Strict Gate reject rule")
		})
	}
}

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
				require.Equal(t, "user:byage", idx.Name())
				require.Equal(t, "user:tenant:*", idx.KeyPattern())
				require.Equal(t, []string{"profile_age"}, idx.Paths())
			},
		},
		{
			name: "composite multi-field index preserves path order",
			run: func(t *testing.T) {
				idx := Idx[userDoc]("TenantAge", "*", "tenant", "profile.age")
				require.Equal(t, "user:tenantage", idx.Name())
				require.Equal(t, []string{"tenant", "profile_age"}, idx.Paths())
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
				require.Equal(t, []string{"metrics_activity_score"}, idx.Paths())
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
		require.Equal(t, "user:age", full)
	})

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := ValidateIdxName[userDoc]("")
		require.EqualError(t, err, "index name is required")
	})

	t.Run("rejects fully qualified name", func(t *testing.T) {
		_, err := ValidateIdxName[userDoc]("user:age")
		require.EqualError(t, err, "idx name must be logical, got fully-qualified index name: user:age")
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
