package xcmd

import (
	"testing"

	"github.com/kcmvp/redisx/x"
	"github.com/tidwall/gjson"
)

func TestMarshalUpdate(t *testing.T) {
	tests := []struct {
		name       string
		values     []x.Mutation
		wantErr    bool
		assertJSON func(t *testing.T, doc string)
	}{
		{
			name: "marshal scalar updates",
			values: []x.Mutation{
				x.Set("status", "active"),
				x.Set("age", 18),
				x.Set("verified", true),
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
			values: []x.Mutation{
				x.Set("profile.age", 20),
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
