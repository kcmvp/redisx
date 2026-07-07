package x

import (
	"strings"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"
)

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
				return true // Field doesn't exist means it's not equal
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

// Contains returns a filter that passes if the JSON record's field is a string and contains the provided substring.
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
	// Convert typed slice to []any for json marshaling
	anyValues := lo.Map(values, func(v T, _ int) any { return v })

	return &filterFunc{
		eval: func(jsonRecord string) bool {
			rs := gjson.Get(jsonRecord, field)
			if !rs.Exists() {
				return false
			}
			val := rs.Value()
			// Need to compare dynamically
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
