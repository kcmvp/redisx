package x

import (
	"encoding/json"
)

// Expression represents a MongoDB-style JSON query expression.
type Expression map[string]any

// Filter is a pure function that evaluates a JSON record string.
type Filter interface {
	// Eval evaluates the JSON record and returns true if it matches the filter.
	Eval(jsonRecord string) bool
	// MarshalJSON serializes the filter into a MongoDB-style JSON expression.
	MarshalJSON() ([]byte, error)
}

type filterFunc struct {
	eval func(jsonRecord string) bool
	expr Expression
}

func (f *filterFunc) Eval(jsonRecord string) bool {
	return f.eval(jsonRecord)
}

func (f *filterFunc) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.expr)
}

// And returns a Filter that passes if all provided filters pass.
func And(filters ...Filter) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			for _, f := range filters {
				if !f.Eval(jsonRecord) {
					return false
				}
			}
			return true
		},
		expr: Expression{"$and": filters},
	}
}

// Or returns a Filter that passes if at least one of the provided filters passes.
func Or(filters ...Filter) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			for _, f := range filters {
				if f.Eval(jsonRecord) {
					return true
				}
			}
			return false
		},
		expr: Expression{"$or": filters},
	}
}

// Not returns a Filter that passes if the provided filter fails.
func Not(f Filter) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			return !f.Eval(jsonRecord)
		},
		expr: Expression{"$not": f},
	}
}
