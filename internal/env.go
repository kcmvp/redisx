package internal

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/samber/lo"
)

const (
	RespxAuthKeyEnv = "RESPX_AUTH_KEY"
	XCmdQuery       = "query"
)

// Index represents an index configuration for BuntDB.
// Name is the name of the index, Pattern is the JSON path (e.g. "user.*"), and Path is the JSON field path (e.g. "age").
type Index struct {
	Name    string
	Pattern string
	Path    string
}

// IndexFunc is a function that returns a list of indexes to be created on server startup.
// It returns a slice of lo.Tuple2, where A is the index name and B is the key pattern.
// Note: We'll map the tuple slightly differently, or use the Index struct for better type safety.
// Based on the request, using lo.Tuple2[string, string] where A=name, B=pattern.
type IndexFunc func() []lo.Tuple2[string, string]

func AuthKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
