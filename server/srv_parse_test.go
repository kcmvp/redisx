package server

import (
	"testing"
)

func TestParseFilter(t *testing.T) {
	jsonRecord := `{"name": "ken", "age": 30, "status": "active", "score": 95.5}`

	tests := []struct {
		name       string
		jsonFilter string
		expectErr  bool
		expected   bool // expected result when evaluating jsonRecord
	}{
		// Empty filters
		{"Empty string", ``, false, true},
		{"Empty object", `{}`, false, true},
		{"Invalid JSON", `{invalid`, true, false},

		// Basic equality
		{"Implicit Eq string", `{"name": "ken"}`, false, true},
		{"Implicit Eq false", `{"name": "john"}`, false, false},
		{"Explicit Eq string", `{"name": {"$eq": "ken"}}`, false, true},
		{"Explicit Eq number", `{"age": {"$eq": 30}}`, false, true},

		// Other comparators
		{"Neq true", `{"name": {"$neq": "john"}}`, false, true},
		{"Neq false", `{"name": {"$neq": "ken"}}`, false, false},
		
		{"Gt true", `{"age": {"$gt": 20}}`, false, true},
		{"Gt false", `{"age": {"$gt": 40}}`, false, false},
		
		{"Gte true", `{"age": {"$gte": 30}}`, false, true},
		
		{"Lt true", `{"age": {"$lt": 40}}`, false, true},
		{"Lt false", `{"age": {"$lt": 20}}`, false, false},
		
		{"Lte true", `{"age": {"$lte": 30}}`, false, true},
		
		{"Contains true", `{"status": {"$contains": "act"}}`, false, true},
		{"Contains false", `{"status": {"$contains": "pen"}}`, false, false},
		
		{"In true", `{"status": {"$in": ["pending", "active"]}}`, false, true},
		{"In false", `{"status": {"$in": ["pending", "banned"]}}`, false, false},
		{"In not array", `{"status": {"$in": "active"}}`, true, false},

		// Logical Combinators
		{
			name:       "Implicit AND (multiple keys)",
			jsonFilter: `{"age": {"$gt": 20}, "status": "active"}`,
			expectErr:  false,
			expected:   true,
		},
		{
			name:       "Implicit AND false",
			jsonFilter: `{"age": {"$gt": 40}, "status": "active"}`,
			expectErr:  false,
			expected:   false,
		},
		{
			name:       "Explicit AND",
			jsonFilter: `{"$and": [{"age": {"$gt": 20}}, {"status": "active"}]}`,
			expectErr:  false,
			expected:   true,
		},
		{
			name:       "Explicit OR true",
			jsonFilter: `{"$or": [{"age": {"$lt": 20}}, {"status": "active"}]}`,
			expectErr:  false,
			expected:   true,
		},
		{
			name:       "Explicit OR false",
			jsonFilter: `{"$or": [{"age": {"$lt": 20}}, {"status": "pending"}]}`,
			expectErr:  false,
			expected:   false,
		},
		{
			name:       "Complex Nested",
			jsonFilter: `{"$or": [{"age": {"$lt": 20}}, {"$and": [{"age": {"$gt": 18}}, {"status": "active"}]}]}`,
			expectErr:  false,
			expected:   true,
		},

		// Error cases
		{"Unsupported operator", `{"age": {"$unknown": 18}}`, true, false},
		{"And not array", `{"$and": {"age": 18}}`, true, false},
		{"Or not array", `{"$or": {"age": 18}}`, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := parseFilter(tt.jsonFilter)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if filter == nil {
				if !tt.expected {
					t.Errorf("got nil filter (passes everything) but expected false")
				}
				return
			}

			result := filter.Eval(jsonRecord)
			if result != tt.expected {
				t.Errorf("expected eval result %v, got %v", tt.expected, result)
			}
		})
	}
}
