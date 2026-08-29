package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"

	"github.com/kcmvp/redisx/x"
)

// ——— ——— 工具函数 / Pure helpers ——— ———

// appearsDocIntentJSON reports whether a raw SET/SETEX/SETNX value string
// "looks like" typed-document JSON — i.e. parses as a JSON object or
// JSON array. When namespace is missing and the value matches, we emit
// a "schema not registered" error instead of a generic "need separator"
// error, which is a much better diagnostic for callers migrating from
// raw KV to typed docs.
func appearsDocIntentJSON(v string) bool {
	if !gjson.Valid(v) {
		return false
	}
	r := gjson.Parse(v)
	return r.IsObject() || r.IsArray()
}

// normalizeKRJSONArg normalises the first positional argument of
// UPDATE / SEARCHKEY to a valid x.KeyRange JSON payload.
//
// Accepts two forms:
//   - already-JSON `{ "op": "pattern", "p": "<ns>:*" }` — returned as-is
//   - legacy shorthand string `<ns>:*` (or any string without a leading
//     '{') — wrapped into `{"op":"pattern","p":"<raw>"}` via json.Marshal
//
// If json.Marshal fails for a non-JSON raw, the input string is returned
// unchanged so downstream UnmarshalKeyRange can surface the exact parse
// error instead of a misleading wrapper failure.
func normalizeKRJSONArg(raw string) []byte {
	if len(raw) > 0 && raw[0] == '{' {
		return []byte(raw)
	}
	b, err := json.Marshal(map[string]string{"op": "pattern", "p": raw})
	if err != nil {
		return []byte(raw)
	}
	return b
}

// parseFilter recursively parses a MongoDB-style JSON filter string into
// an x.Filter tree. Empty / "{}" passes everything.
//
// Supported operators:
//   - Top-level keys combine with implicit $and
//   - `$and` / `$or` — array of sub-filters
//   - `$eq`, `$neq` — scalar equality
//   - `$gt`, `$gte`, `$lt`, `$lte` — numeric comparisons (parsed as float64)
//   - `$contains` — string substring match
//   - `$in` — array value, any match passes
//   - `"field": <scalar>` — shorthand implicit `$eq`
func parseFilter(jsonStr string) (x.Filter, error) {
	if jsonStr == "" || jsonStr == "{}" {
		return nil, nil // empty filter passes everything
	}

	if !gjson.Valid(jsonStr) {
		return nil, errors.New("invalid JSON filter format")
	}

	root := gjson.Parse(jsonStr)
	return parseNode(root)
}

// parseNode recursively parses one gjson JSON object node into an
// x.Filter. See parseFilter for the supported operator grammar.
//
// Field-comparison sub-objects (e.g. `{"age":{"$gt":18}}`) are parsed in
// the `default` branch; an unrecognised operator bubbles up as a parse
// error via the closure-local `parseErr` variable.
func parseNode(node gjson.Result) (x.Filter, error) {
	if node.Type != gjson.JSON {
		return nil, errors.New("expected JSON object")
	}

	var filters []x.Filter
	var parseErr error

	node.ForEach(func(key, value gjson.Result) bool {
		k := key.String()

		switch k {
		case "$and":
			if !value.IsArray() {
				parseErr = errors.New("$and must be an array")
				return false
			}
			var subFilters []x.Filter
			value.ForEach(func(_, subNode gjson.Result) bool {
				f, err := parseNode(subNode)
				if err != nil {
					parseErr = err
					return false
				}
				// nil sub-filter = empty object = passes everything;
				// skip it so And/Or never receive a nil Filter (would
				// panic on Eval).
				if f != nil {
					subFilters = append(subFilters, f)
				}
				return true
			})
			if parseErr != nil {
				return false
			}
			filters = append(filters, x.And(subFilters...))
		case "$or":
			if !value.IsArray() {
				parseErr = errors.New("$or must be an array")
				return false
			}
			var subFilters []x.Filter
			value.ForEach(func(_, subNode gjson.Result) bool {
				f, err := parseNode(subNode)
				if err != nil {
					parseErr = err
					return false
				}
				if f != nil {
					subFilters = append(subFilters, f)
				}
				return true
			})
			if parseErr != nil {
				return false
			}
			filters = append(filters, x.Or(subFilters...))
		default:
			// Field comparison. E.g. "age": {"$gt": 18} or "status": "active"
			if value.Type == gjson.JSON {
				// Object with operators
				value.ForEach(func(opKey, opVal gjson.Result) bool {
					op := opKey.String()
					switch op {
					case "$eq":
						filters = append(filters, x.Eq(k, opVal.Value()))
					case "$neq":
						filters = append(filters, x.Neq(k, opVal.Value()))
					case "$gt":
						filters = append(filters, x.Gt(k, opVal.Float()))
					case "$gte":
						filters = append(filters, x.Gte(k, opVal.Float()))
					case "$lt":
						filters = append(filters, x.Lt(k, opVal.Float()))
					case "$lte":
						filters = append(filters, x.Lte(k, opVal.Float()))
					case "$contains":
						filters = append(filters, x.Contains(k, opVal.String()))
					case "$in":
						if !opVal.IsArray() {
							parseErr = fmt.Errorf("$in operator requires an array for field %s", k)
							return false
						}
						var inValues []any
						opVal.ForEach(func(_, v gjson.Result) bool {
							inValues = append(inValues, v.Value())
							return true
						})
						filters = append(filters, x.In(k, inValues...))
					default:
						parseErr = fmt.Errorf("unsupported operator: %s", op)
						return false
					}
					return true
				})
			} else {
				// Implicit $eq. E.g. "status": "active"
				filters = append(filters, x.Eq(k, value.Value()))
			}
		}

		return parseErr == nil
	})

	if parseErr != nil {
		return nil, parseErr
	}

	if len(filters) == 0 {
		return nil, nil
	}
	if len(filters) == 1 {
		return filters[0], nil
	}
	// Implicit AND for multiple keys in the same object
	return x.And(filters...), nil
}

// ——— ——— 握手 / 元命令 / Auth & Hello & Client & Ping & Quit ——— ———

// authCommand implements the AUTH wire command for RedisX's dual-port
// (app + ctrl) model.
//
// Flow:
//  1. If the connection is already authenticated and the caller repeats
//     the same key we answer OK; if they send a DIFFERENT key we close
//     the connection (security: no silent re-auth downgrade).
//  2. If --ctrl-auth is configured on the ctrl port we REFUSE the
//     app-auth key (and vice versa on the app port) to prevent leaking
//     the wrong role.
//  3. acquireAuthConn() checks the global per-key connection limit and
//     validates existence of the key via `_auth_:<key>` storage lookup
//     (or the bootstrap internalAuthKey literal). Internal-NS Write
//     Guard forbids wire clients from mutating `_auth_:` keys directly
//     through SET / DEL; AUTH key rotation is a ctrl-only operation.
func authCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR authentication failed")
		slog.Warn("auth format error", "remote", conn.RemoteAddr())
		_ = conn.Close()
		return
	}
	providedKey := string(cmd.Args[1])
	prevAuth, _ := conn.Context().(string)

	if prevAuth != "" {
		if prevAuth == providedKey {
			conn.WriteString("OK")
			return
		}
		conn.WriteError("ERR connection already authenticated")
		slog.Warn("auth rejected on authenticated connection", "remote", conn.RemoteAddr(), "current_auth_key", prevAuth, "provided_auth_key", providedKey)
		_ = conn.Close()
		return
	}

	if err := acquireAuthConn(db, providedKey); err == nil {
		// If the provided AUTH key is a port-role-bound key (exists in EITHER
		// the app or ctrl accept sets), immediately validate that it matches the role
		// of the listener the client connected to. This is fail-fast, so that
		// a connection dialed to the app port with a ctrl-only key gets
		// WRONGPASS on AUTH itself rather than only on the next command.
		// External limit keys and IPC keys (values not in EITHER set) continue
		// to pass through without role validation here.
		appValues, ctrlValues, defaultApp, defaultCtrl, lerr := loadAllAuthPortKeys(db)
		if lerr == nil && (len(appValues) > 0 || len(ctrlValues) > 0) {
			_, inApp := appValues[providedKey]
			_, inCtrl := ctrlValues[providedKey]
			if inApp || inCtrl {
				role := connPortRole(conn)
				switch role {
				case portRoleApp:
					if !inApp {
						msg := "WRONGPASS invalid or wrong auth key for app port"
						if defaultCtrl != "" && providedKey == defaultCtrl {
							msg += " (looks like you supplied the ctrl_0 key to the app port)"
						}
						conn.WriteError(msg)
						slog.Warn("AUTH role mismatch: ctrl-only key provided on app port, rejected", "remote", conn.RemoteAddr(), "auth_key", providedKey)
						_ = conn.Close()
						return
					}
				case portRoleCtrl:
					if !inCtrl {
						msg := "WRONGPASS invalid or wrong auth key for ctrl port"
						if defaultApp != "" && providedKey == defaultApp {
							msg += " (looks like you supplied the app_0 key to the ctrl port)"
						}
						conn.WriteError(msg)
						slog.Warn("AUTH role mismatch: app-only key provided on ctrl port, rejected", "remote", conn.RemoteAddr(), "auth_key", providedKey)
						_ = conn.Close()
						return
					}
				}
			}
		}
		conn.SetContext(providedKey)
		conn.WriteString("OK")
		slog.Info("connection authenticated", "remote", conn.RemoteAddr(), "auth_key", providedKey)
	} else {
		msg := "ERR authentication failed"
		if strings.Contains(err.Error(), "connection limit exceeded") {
			msg = "ERR auth key connection limit exceeded"
		}
		if strings.Contains(err.Error(), "invalid auth limit") {
			msg = "ERR invalid auth key limit"
		}
		if strings.Contains(err.Error(), "not found") && providedKey != internalAuthKey {
			msg = "ERR authentication failed"
		}
		conn.WriteError(msg)
		slog.Warn("auth failed", "remote", conn.RemoteAddr(), "auth_key", providedKey, "error", err)
		_ = conn.Close()
		return
	}
}

// helloCommand implements HELLO handshake for client/server identity probing.
//
// SSoT: Minimal identity response — intentionally returns ONLY the server
// string "redisx". This follows the "backend command upgrade, frontend App
// no update needed" decoupling principle: the client owns its command
// catalogue, feature flags, and ctrl/app UI distinctions. The server's
// only job during HELLO is to prove it's a RedisX peer so the client
// SDK can enable typed-doc / colon-mode routing internally.
//
// All further metadata is accessed via dedicated wire commands (KEYS,
// SEARCHKEY, etc.) — never via HELLO field inflation.
func helloCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteAny(map[string]any{"server": "redisx"})
}

// clientCommand answers CLIENT * subcommands. RedisX temporarily
// implements the minimum viable subset to keep generic Redis clients
// happy (redis-py, ioredis, Jedis all issue CLIENT SETINFO lib-name/…
// after HELLO). Currently a no-op that always answers OK.
func clientCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteString("OK")
}

// pingCommand answers PING with PONG. No argc stricter than len(cmd.Args)
// ≥ 1 needed because dispatcher already validates argv≥1; redis-cli
// customarily supports `PING <msg>` to round-trip arbitrary payloads
// but we use the strict zero-arg form to avoid leaking echoes.
func pingCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteString("PONG")
}

// quitCommand closes the redcon connection and writes OK so redis-cli's
// "exit / Ctrl-D" path stays indistinguishable from Redis native
// behaviour. Calling conn.Close() while writing is idempotent in our
// redcon fork; the slog-level event lives at the listener layer.
func quitCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteString("OK")
	_ = conn.Close()
}

// ——— ——— Pub/Sub ——— ———

// publishCommand implements PUBLISH. Delegates to the redcon.PubSub
// handle attached to both listeners; RedisX does not namespace pub/sub
// into storage layers — channels are global like vanilla Redis.
func publishCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
}

// subscribeCommand implements SUBSCRIBE. 1..n channel names; subscribes
// each in order and does NOT WriteOK per channel manually — redcon's
// PubSub machinery already emits the `subscribe` streaming push to the
// connection on Subscribe().
func subscribeCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	for i := 1; i < len(cmd.Args); i++ {
		ps.Subscribe(conn, string(cmd.Args[i]))
	}
}

// pSubscribeCommand implements PSUBSCRIBE (glob-pattern subscribe).
// Semantics mirror subscribeCommand; pattern matching is handled by
// redcon's implementation. RedisX doesn't split channels by storage
// layer, so channel names don't route through resolveLayer.
func pSubscribeCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	for i := 1; i < len(cmd.Args); i++ {
		ps.Psubscribe(conn, string(cmd.Args[i]))
	}
}
