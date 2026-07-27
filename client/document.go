package client

import (
	"time"
)

// Document is an abstraction for Redis documents.
type Document interface {
	Prefix() string
	Key() string
	Value() string
	Ttl() time.Duration
}

// SaveDoc saves a document to Redis.
func SaveDoc[D Document](doc D) error {
	fullKey := doc.Prefix() + doc.Key()
	return SetWithTTL(fullKey, doc.Value(), doc.Ttl())
}

// FetchDoc retrieves a document's value from Redis by its prefix and key.
// The provided doc is expected to supply the correct Prefix() and Key().
func FetchDoc[D Document](doc D) (string, error) {
	fullKey := doc.Prefix() + doc.Key()
	return Get(fullKey)
}

// DeleteDoc deletes a document from Redis.
func DeleteDoc[D Document](doc D) (bool, error) {
	fullKey := doc.Prefix() + doc.Key()
	return Delete(fullKey)
}
