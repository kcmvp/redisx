package x

import "time"

// Document is an abstraction for Redis documents.
type Document interface {
	Prefix() string
	Key() string
	Value() string
	TTL() time.Duration
	StorageKey() string
}

// DefaultStorageKey provides a default implementation for Document.StorageKey.
func DefaultStorageKey(d interface{ Prefix() string; Key() string }) string {
	return d.Prefix() + d.Key()
}
