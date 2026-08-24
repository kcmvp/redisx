package x

import (
	"encoding/json"
	"strings"

	naming "github.com/kcmvp/redisx/internal/naming"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
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

func (f *filterFunc) Eval(jsonRecord string) bool { return f.eval(jsonRecord) }

func (f *filterFunc) MarshalJSON() ([]byte, error) { return json.Marshal(f.expr) }

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
		eval: func(jsonRecord string) bool { return !f.Eval(jsonRecord) },
		expr: Expression{"$not": f},
	}
}

// SECTION: Comparators

// Eq returns a filter that passes if the JSON record's field equals the provided value.
func Eq(field string, value any) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() {
				return false
			}
			return rs.Value() == value
		},
		expr: Expression{field: Expression{"$eq": value}},
	}
}

// Neq returns a filter that passes if the JSON record's field does not equal the provided value.
func Neq(field string, value any) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() {
				return true
			}
			return rs.Value() != value
		},
		expr: Expression{field: Expression{"$neq": value}},
	}
}

// Gt returns a filter that passes if the JSON record's field is strictly greater than the provided value.
func Gt(field string, value float64) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.Number {
				return false
			}
			return rs.Float() > value
		},
		expr: Expression{field: Expression{"$gt": value}},
	}
}

// Gte returns a filter that passes if the JSON record's field is greater than or equal to the provided value.
func Gte(field string, value float64) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.Number {
				return false
			}
			return rs.Float() >= value
		},
		expr: Expression{field: Expression{"$gte": value}},
	}
}

// Lt returns a filter that passes if the JSON record's field is strictly less than the provided value.
func Lt(field string, value float64) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.Number {
				return false
			}
			return rs.Float() < value
		},
		expr: Expression{field: Expression{"$lt": value}},
	}
}

// Lte returns a filter that passes if the JSON record's field is less than or equal to the provided value.
func Lte(field string, value float64) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.Number {
				return false
			}
			return rs.Float() <= value
		},
		expr: Expression{field: Expression{"$lte": value}},
	}
}

// Contains returns a filter that passes if the JSON record's field is a string
// and contains the provided substring.
func Contains(field string, substring string) Filter {
	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() || rs.Type != gjson.String {
				return false
			}
			return strings.Contains(rs.String(), substring)
		},
		expr: Expression{field: Expression{"$contains": substring}},
	}
}

// In returns a filter that passes if the JSON record's field equals any of the provided values.
func In[T comparable](field string, values ...T) Filter {
	anyValues := lo.Map(values, func(v T, _ int) any { return v })

	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() {
				return false
			}
			val := rs.Value()
			for _, v := range anyValues {
				if val == v {
					return true
				}
			}
			return false
		},
		expr: Expression{field: Expression{"$in": anyValues}},
	}
}

// SECTION: Mutation

// Mutation describes one JSON path update used by UPDATE flows.
type Mutation interface {
	Path() string
	Value() any
}

// ScalarType limits Set values to the scalar types currently supported by
// redisx update operations.
type ScalarType interface {
	~int | ~int32 | ~int64 | ~float32 | ~float64 | ~string | ~bool
}

type pair[T ScalarType] struct {
	path string
	val  T
}

func (v pair[T]) Path() string { return v.path }

func (v pair[T]) Value() any { return v.val }

// Set creates one JSON path update payload.
//
// Example:
//
//	x.Set("status", "active")
//	x.Set("profile.age", 18)
func Set[T ScalarType](path string, value T) Mutation {
	return pair[T]{path: path, val: value}
}

// x/internal usage guard:
// At least one symbol from the naming sub-package must be referenced inside
// each split file to prevent goimports from stripping the import during
// future refactors. This single reference is a no-op and keeps the import
// stable.
var _ = naming.ValidateDocLogicalNamespace
