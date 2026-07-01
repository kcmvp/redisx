package storage

import "time"

// Schema represents the configuration required to create a JSON index in the backend storage (BuntDB).
// By defining an index, you enable the respx server to perform highly efficient lookups on specific JSON fields
// across your stored values using the QueryX command.
type Schema interface {
	// Name returns the unique identifier for the index.
	// This name is used by the client when executing a QueryX command to specify which index to search against.
	Name() string

	// Pattern returns the pattern that determines which keys should be included in this index.
	// For example, returning "user:*" will only index JSON values whose keys start with "user:".
	// Returning "*" will index all keys in the database.
	Pattern() string

	// Path returns the specific path within the JSON document that should be indexed.
	// For example, returning "age" or "address.city" will index those specific fields from the JSON values.
	Path() string

	// Ttl define the TTL of the data in this schema
	Ttl() time.Duration
}
