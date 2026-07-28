package x

import "time"

// Document is an abstraction for Redis documents.
type Document interface {
	Prefix() string
	Key() string
	Value() string
	TTL() time.Duration
}
