package proto

import "time"

// SchemaPayload is the wire form of one client-side schema declaration
// together with its inline secondary indexes. The client produces it
// (client.RegisterSchema) and the server consumes it through the SCHEMA
// command, so the two sides share a single field definition.
//
// The field names/tags mirror server docSpec's user-visible fields so the
// payload maps 1:1 onto writeDocSpec's input.
type SchemaPayload struct {
	Namespace string         `json:"namespace"`
	Mem       bool           `json:"mem"`
	KeyAttrs  []string       `json:"key_attrs"`
	TTL       time.Duration  `json:"ttl_ns"`
	Indexes   []IndexPayload `json:"indexes,omitempty"`
}

// IndexPayload is the wire form of one x.Index: its fully-qualified runtime
// name, the storage-key glob pattern it is scoped to, and its normalized JSON
// paths ('.' already replaced by '_').
type IndexPayload struct {
	Name       string   `json:"name"`
	KeyPattern string   `json:"key_pattern"`
	Paths      []string `json:"paths"`
}
