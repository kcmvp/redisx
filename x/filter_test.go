package x

import (
	"testing"
)

func TestComparators(t *testing.T) {
	jsonRecord := `{"name": "ken", "age": 30, "status": "active", "roles": ["admin", "user"], "score": 95.5}`

	tests := []struct {
		name     string
		filter   Filter
		expected bool
	}{
		// Eq
		{"Eq string true", Eq("name", "ken"), true},
		{"Eq string false", Eq("name", "john"), false},
		{"Eq number true", Eq("age", float64(30)), true}, // gjson unmarshals numbers to float64
		{"Eq not exists", Eq("email", "ken@a.com"), false},

		// Neq
		{"Neq string true", Neq("name", "john"), true},
		{"Neq string false", Neq("name", "ken"), false},
		{"Neq not exists", Neq("email", "ken@a.com"), true}, // not exists means not equal

		// Gt
		{"Gt true", Gt("age", 20), true},
		{"Gt false", Gt("age", 30), false},
		{"Gt not exists", Gt("salary", 1000), false},
		{"Gt wrong type", Gt("name", 10), false},

		// Gte
		{"Gte true greater", Gte("age", 20), true},
		{"Gte true equal", Gte("age", 30), true},
		{"Gte false", Gte("age", 40), false},

		// Lt
		{"Lt true", Lt("age", 40), true},
		{"Lt false", Lt("age", 30), false},

		// Lte
		{"Lte true less", Lte("age", 40), true},
		{"Lte true equal", Lte("age", 30), true},
		{"Lte false", Lte("age", 20), false},

		// Contains
		{"Contains true", Contains("status", "act"), true},
		{"Contains false", Contains("status", "pen"), false},
		{"Contains not string", Contains("age", "3"), false},

		// In
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
		// And
		{"And true", And(Gt("age", 20), Eq("status", "active")), true},
		{"And false first", And(Lt("age", 20), Eq("status", "active")), false},
		{"And false second", And(Gt("age", 20), Eq("status", "pending")), false},

		// Or
		{"Or true first", Or(Gt("age", 20), Eq("status", "pending")), true},
		{"Or true second", Or(Lt("age", 20), Eq("status", "active")), true},
		{"Or false both", Or(Lt("age", 20), Eq("status", "pending")), false},

		// Not
		{"Not true", Not(Lt("age", 20)), true},
		{"Not false", Not(Gt("age", 20)), false},

		// Complex
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
