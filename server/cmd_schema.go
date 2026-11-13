package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/x"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

// schemaCommand implements SCHEMA <json-payload>: the wire entry point for one
// client-side schema declaration (client.RegisterSchema) — the doc schema plus
// its inline secondary indexes.
//
// The server applies it immediately and idempotently, so the caller may push it
// as often as it likes (for example to re-declare after a reconnect; the server
// itself neither persists nor replays anything):
//
//   - the doc spec goes through writeDocSpec, which is a no-op while its MD5
//     fingerprint is unchanged;
//   - index mounting ignores an already-mounted index name, so re-declaring
//     an index is not an error.
//
// SCHEMA is ctrl-only by omission: it is deliberately absent from
// appPortCmdAllowlist, so the app port rejects it through the Gate-1
// privilege gate.
func schemaCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'schema' command: usage: SCHEMA <json-payload>")
		return
	}
	var payload proto.SchemaPayload
	dec := json.NewDecoder(strings.NewReader(string(cmd.Args[1])))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&payload); err != nil {
		conn.WriteError("ERR SCHEMA schema: " + err.Error())
		return
	}
	if err := db.registerSchemaPayload(payload); err != nil {
		conn.WriteError("ERR SCHEMA " + err.Error())
		return
	}
	conn.WriteString("OK")
}

// registerSchemaPayload applies one SCHEMA declaration: the doc spec goes
// through writeDocSpec (the single persistence entry point), then every
// declared index is mounted idempotently.
func (db *DB) registerSchemaPayload(p proto.SchemaPayload) error {
	spec := docSpec{
		Namespace: p.Namespace,
		Mem:       p.Mem,
		KeyAttrs:  append([]string(nil), p.KeyAttrs...),
		TTL:       p.TTL,
	}
	if err := db.writeDocSpec(spec); err != nil {
		return err
	}
	for _, ip := range p.Indexes {
		if ip.Name == "" || ip.KeyPattern == "" || len(ip.Paths) == 0 {
			return fmt.Errorf("index %q requires name, key_pattern and at least one path", ip.Name)
		}
		for _, path := range ip.Paths {
			if path == "" {
				return fmt.Errorf("index %q has an empty path", ip.Name)
			}
		}
		if err := db.registerIndexIfAbsent(x.RawIndex(ip.Name, ip.KeyPattern, ip.Paths...)); err != nil {
			return err
		}
	}
	return nil
}

// registerIndexIfAbsent mounts idx unless an index with the same name is
// already mounted, in which case it is a silent no-op. BuntDB rejects duplicate
// index names, and a caller may legitimately push the same schema more than
// once, so an existing index must never surface as an error.
func (db *DB) registerIndexIfAbsent(idx x.Index) error {
	db.idxRegMu.RLock()
	_, exists := db.idxRegSpec[idx.Name()]
	db.idxRegMu.RUnlock()
	if exists {
		slog.Debug("index already registered, skipping", "name", idx.Name())
		return nil
	}
	err := db.registerIndexes(idx)
	if errors.Is(err, buntdb.ErrIndexExists) {
		slog.Debug("index already registered, skipping", "name", idx.Name())
		return nil
	}
	return err
}
