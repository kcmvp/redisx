package x

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
