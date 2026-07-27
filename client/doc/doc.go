package doc

import (
	"time"

	"github.com/kcmvp/redisx/client"
	"github.com/kcmvp/redisx/x"
	"github.com/samber/mo"
)

// Document is an abstraction for Redis documents.
type Document interface {
	Prefix() string
	Key() string
	Value() string
	Ttl() time.Duration
}

func fullKey[D Document](d D) string {
	return d.Prefix() + d.Key()
}

// Get retrieves the value for the given document using the shared resp client.
func Get[D Document](d D) (string, error) {
	return client.Get(fullKey(d))
}

// SetWithTTL stores the document's value for the given document key with its TTL.
func SetWithTTL[D Document](d D) error {
	return client.SetWithTTL(fullKey(d), d.Value(), d.Ttl())
}

// Set stores the document's value for the given document key without TTL.
func Set[D Document](d D) error {
	return client.Set(fullKey(d), d.Value())
}

// SetNX stores the document's value for the given document key only if the key does not exist.
func SetNX[D Document](d D) (bool, error) {
	return client.SetNX(fullKey(d), d.Value())
}

// Delete removes the specified document.
func Delete[D Document](d D) (bool, error) {
	return client.Delete(fullKey(d))
}

// Keys returns all keys matching the document's prefix and the given sub-pattern.
func Keys[D Document](d D, pattern string) mo.Result[[]string] {
	return client.Keys(d.Prefix() + pattern)
}

// SearchIndex executes the SEARCHINDEX command on the shared connection.
func SearchIndex(indexName string, filter x.Filter, desc bool) mo.Result[[]string] {
	return client.SearchIndex(indexName, filter, desc)
}

// SearchKey executes the SEARCHKEY command using the document's prefix + sub-pattern.
func SearchKey[D Document](d D, pattern string, filter x.Filter, desc bool) mo.Result[[]string] {
	return client.SearchKey(d.Prefix() + pattern, filter, desc)
}

// Update executes the UPDATE command using the document's prefix + sub-pattern.
func Update[D Document](d D, pattern string, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	return client.Update(d.Prefix() + pattern, filter, values...)
}
