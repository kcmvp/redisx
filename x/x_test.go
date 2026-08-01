package x

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcmvp/redisx/x/contract"
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

type conflictUserDoc string

func (conflictUserDoc) Namespace() string  { return "user" }
func (conflictUserDoc) Mem() bool          { return false }
func (conflictUserDoc) KeyAttrs() []string { return []string{"tenant", "id"} }
func (u conflictUserDoc) RawJSON() string  { return string(u) }
func (conflictUserDoc) TTL() time.Duration { return time.Hour }

type conflictTTLUserDoc string

func (conflictTTLUserDoc) Namespace() string  { return "user" }
func (conflictTTLUserDoc) Mem() bool          { return false }
func (conflictTTLUserDoc) KeyAttrs() []string { return []string{"id"} }
func (u conflictTTLUserDoc) RawJSON() string  { return string(u) }
func (conflictTTLUserDoc) TTL() time.Duration { return 2 * time.Hour }

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

func (bareMemPrefixDoc) Namespace() string  { return contract.MemKeyPrefix }
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

func resetDocumentRegistry() {
	documentRegistry = sync.Map{}
	documentTypeRegistry = sync.Map{}
}

func TestComparators(t *testing.T) {
	jsonRecord := `{"name": "ken", "age": 30, "status": "active", "roles": ["admin", "user"], "score": 95.5}`

	tests := []struct {
		name     string
		filter   Filter
		expected bool
	}{
		{"Eq string true", Eq("name", "ken"), true},
		{"Eq string false", Eq("name", "john"), false},
		{"Eq number true", Eq("age", float64(30)), true},
		{"Eq not exists", Eq("email", "ken@a.com"), false},
		{"Neq string true", Neq("name", "john"), true},
		{"Neq string false", Neq("name", "ken"), false},
		{"Neq not exists", Neq("email", "ken@a.com"), true},
		{"Gt true", Gt("age", 20), true},
		{"Gt false", Gt("age", 30), false},
		{"Gt not exists", Gt("salary", 1000), false},
		{"Gt wrong type", Gt("name", 10), false},
		{"Gte true greater", Gte("age", 20), true},
		{"Gte true equal", Gte("age", 30), true},
		{"Gte false", Gte("age", 40), false},
		{"Lt true", Lt("age", 40), true},
		{"Lt false", Lt("age", 30), false},
		{"Lte true less", Lte("age", 40), true},
		{"Lte true equal", Lte("age", 30), true},
		{"Lte false", Lte("age", 20), false},
		{"Contains true", Contains("status", "act"), true},
		{"Contains false", Contains("status", "pen"), false},
		{"Contains not string", Contains("age", "3"), false},
		{"In string true", In("status", "pending", "active"), true},
		{"In string false", In("status", "pending", "banned"), false},
		{"In number true", In("age", float64(20), float64(30)), true},
		{"In not exists", In("email", "ken@a.com"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Eval(jsonRecord)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestCombinators(t *testing.T) {
	jsonRecord := `{"age": 25, "status": "active"}`

	tests := []struct {
		name     string
		filter   Filter
		expected bool
	}{
		{"And true", And(Gt("age", 20), Eq("status", "active")), true},
		{"And false first", And(Lt("age", 20), Eq("status", "active")), false},
		{"And false second", And(Gt("age", 20), Eq("status", "pending")), false},
		{"Or true first", Or(Gt("age", 20), Eq("status", "pending")), true},
		{"Or true second", Or(Lt("age", 20), Eq("status", "active")), true},
		{"Or false both", Or(Lt("age", 20), Eq("status", "pending")), false},
		{"Not true", Not(Lt("age", 20)), true},
		{"Not false", Not(Gt("age", 20)), false},
		{
			name: "Complex true",
			filter: Or(
				Lt("age", 20),
				And(Gt("age", 20), Eq("status", "active")),
			),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Eval(jsonRecord)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestFilterMarshalJSON(t *testing.T) {
	tests := []struct {
		name   string
		filter Filter
		want   string
	}{
		{
			name:   "marshals comparator",
			filter: Eq("status", "active"),
			want:   `{"status":{"$eq":"active"}}`,
		},
		{
			name:   "marshals combinator",
			filter: And(Gte("age", 18), Eq("status", "active")),
			want:   `{"$and":[{"age":{"$gte":18}},{"status":{"$eq":"active"}}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tt.filter.MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(raw))
		})
	}
}

func TestSetMutation(t *testing.T) {
	mutation := Set("profile.age", 18)

	require.Equal(t, "profile.age", mutation.Path())
	require.Equal(t, 18, mutation.Value())

	raw, err := json.Marshal(mutation.Value())
	require.NoError(t, err)
	require.Equal(t, "18", string(raw))
}

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
			name: "rejects conflicting document schema",
			run: func(t *testing.T) {
				require.Equal(t, "user:201", StorageKeyValue[userDoc]("201"))
				require.Panics(t, func() {
					StorageKeyValue[conflictUserDoc]("202")
				})
			},
		},
		{
			name: "rejects conflicting ttl",
			run: func(t *testing.T) {
				require.Equal(t, "user:201", StorageKeyValue[userDoc]("201"))
				require.Panics(t, func() {
					StorageKeyValue[conflictTTLUserDoc]("202")
				})
			},
		},
		{
			name: "allows mem prefix",
			run: func(t *testing.T) {
				require.Equal(t, "_m_user:201", StorageKeyValue[memUserDoc]("201"))
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
			resetDocumentRegistry()
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
		{name: "adds mem prefix", key: "user:1", want: "_m_user:1"},
		{name: "keeps existing mem prefix", key: "_m_user:1", want: "_m_user:1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, MemKey(tt.key))
		})
	}
}

type countedDoc string

var countedDocCalls atomic.Int32

func (countedDoc) Namespace() string {
	countedDocCalls.Add(1)
	return "counted"
}

func (countedDoc) Mem() bool {
	countedDocCalls.Add(1)
	return false
}

func (countedDoc) KeyAttrs() []string {
	countedDocCalls.Add(1)
	return []string{"id"}
}

func (countedDoc) RawJSON() string {
	return `{"id":"1"}`
}

func (countedDoc) TTL() time.Duration {
	countedDocCalls.Add(1)
	return time.Hour
}

func TestDocumentTypeMetadataCache(t *testing.T) {
	resetDocumentRegistry()
	countedDocCalls.Store(0)

	require.Equal(t, "counted:1", StorageKeyValue[countedDoc]("1"))
	require.Equal(t, int32(5), countedDocCalls.Load())

	require.Equal(t, "counted:2", StorageKeyValue[countedDoc]("2"))
	require.Equal(t, int32(5), countedDocCalls.Load())
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
			name: "joins multiple key attrs with separator",
			run: func() (string, error) {
				return StorageKey(multiKeyDoc(`{"tenant":"acme","id":"202"}`))
			},
			want: "tenantuser:acme:202",
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
			resetDocumentRegistry()
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
		{
			name: "rejects conflicting document schema during registration",
			run: func(t *testing.T) {
				_ = Idx[userDoc]("by_age", "*", "age")
				require.Panics(t, func() {
					Idx[conflictUserDoc]("by_age", "*", "age")
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetDocumentRegistry()
			tt.run(t)
		})
	}
}
