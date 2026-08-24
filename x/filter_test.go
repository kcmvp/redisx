package x

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

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

func TestMarshalUpdate(t *testing.T) {
	tests := []struct {
		name       string
		values     []Mutation
		wantErr    bool
		assertJSON func(t *testing.T, doc string)
	}{
		{
			name: "marshal scalar updates",
			values: []Mutation{
				Set("status", "active"),
				Set("age", 18),
				Set("verified", true),
			},
			assertJSON: func(t *testing.T, doc string) {
				t.Helper()
				if got := gjson.Get(doc, "status").String(); got != "active" {
					t.Fatalf("expected status=active, got %q", got)
				}
				if got := gjson.Get(doc, "age").Float(); got != 18 {
					t.Fatalf("expected age=18, got %v", got)
				}
				if !gjson.Get(doc, "verified").Bool() {
					t.Fatalf("expected verified=true")
				}
			},
		},
		{
			name: "marshal nested path",
			values: []Mutation{
				Set("profile.age", 20),
			},
			assertJSON: func(t *testing.T, doc string) {
				t.Helper()
				if got := gjson.Get(doc, "profile.age").Float(); got != 20 {
					t.Fatalf("expected profile.age=20, got %v", got)
				}
			},
		},
		{
			name:    "reject empty update",
			values:  nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := MarshalUpdate(tt.values...)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.assertJSON != nil {
				tt.assertJSON(t, string(out))
			}
		})
	}
}

func TestParseUpdate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		wantLen int
	}{
		{"parse flat object", `{"status":"active","age":18,"verified":true}`, false, 3},
		{"parse nested object", `{"profile":{"age":18},"status":"active"}`, false, 2},
		{"reject invalid json", `{invalid`, true, 0},
		{"reject non object", `[]`, true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pairs, err := ParseUpdate(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(pairs) != tt.wantLen {
				t.Fatalf("expected %d pairs, got %d", tt.wantLen, len(pairs))
			}
		})
	}
}
