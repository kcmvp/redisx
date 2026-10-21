package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
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

// defaultUsername is the only ACL user RedisX exposes: the implicit Redis
// "default" user. Both username-carrying entry points — AUTH <user> <pass>
// and HELLO <proto> AUTH <user> <pass> — accept exactly this name.
const defaultUsername = "default"

// parseAuthArgs normalises the two wire shapes of AUTH into a (username, key)
// pair. The authentication shape is orthogonal to the RESP version: the request
// framing of both forms is byte-identical in RESP2 and RESP3.
//
//	AUTH <pass>         -> ("", <pass>)       legacy single-argument form
//	AUTH <user> <pass>  -> (<user>, <pass>)   ACL form (Redis 6+)
//
// ok=false means the argument count matches neither shape.
func parseAuthArgs(cmd redcon.Command) (username, key string, ok bool) {
	switch len(cmd.Args) {
	case 2:
		return "", string(cmd.Args[1]), true
	case 3:
		return string(cmd.Args[1]), string(cmd.Args[2]), true
	default:
		return "", "", false
	}
}

// authenticateConn is the single acceptance path shared by AUTH (legacy and
// ACL) and HELLO ... AUTH, so all three entry points share exactly the same
// accept/reject semantics. It performs, in order:
//
//  1. acquireAuthConn — per-key connection limit + `_auth_:<key>` existence
//     (or the bootstrap internalAuthKey literal).
//  2. port-role binding — a port-bound key presented on the wrong listener is
//     rejected fail-fast with WRONGPASS, instead of only on the client's next
//     command.
//  3. conn.SetContext — binds the key to THIS connection as its identity. The
//     scope is deliberately per-connection: Redis semantics require every new
//     connection to authenticate, so a reconnecting client always re-AUTHs.
//
// A non-empty wireErr is the exact error text to answer with; closeConn reports
// whether the connection must be torn down afterwards (every current failure is
// fatal — "no silent re-auth downgrade").
func authenticateConn(conn redcon.Conn, db *DB, key string) (wireErr string, closeConn bool) {
	if err := acquireAuthConn(db, key); err != nil {
		slog.Warn("auth failed", "remote", conn.RemoteAddr(), "auth_key", key, "error", err)
		switch {
		case strings.Contains(err.Error(), "connection limit exceeded"):
			return "ERR auth key connection limit exceeded", true
		case strings.Contains(err.Error(), "invalid auth limit"):
			return "ERR invalid auth key limit", true
		default:
			return "ERR authentication failed", true
		}
	}

	// If the provided key is a port-role-bound key (exists in EITHER the app or
	// ctrl accept set), validate that it matches the role of the listener the
	// client connected to. External limit keys and IPC keys (values in NEITHER
	// set) continue to pass through without role validation here.
	appValues, ctrlValues, defaultApp, defaultCtrl, lerr := loadAllAuthPortKeys(db)
	if lerr == nil && (len(appValues) > 0 || len(ctrlValues) > 0) {
		_, inApp := appValues[key]
		_, inCtrl := ctrlValues[key]
		if inApp || inCtrl {
			role := connPortRole(conn)
			switch role {
			case portRoleApp:
				if !inApp {
					msg := "WRONGPASS invalid or wrong auth key for app port"
					if defaultCtrl != "" && key == defaultCtrl {
						msg += " (looks like you supplied the ctrl_0 key to the app port)"
					}
					slog.Warn("AUTH role mismatch: ctrl-only key provided on app port, rejected", "remote", conn.RemoteAddr(), "auth_key", key)
					return msg, true
				}
			case portRoleCtrl:
				if !inCtrl {
					msg := "WRONGPASS invalid or wrong auth key for ctrl port"
					if defaultApp != "" && key == defaultApp {
						msg += " (looks like you supplied the app_0 key to the ctrl port)"
					}
					slog.Warn("AUTH role mismatch: app-only key provided on ctrl port, rejected", "remote", conn.RemoteAddr(), "auth_key", key)
					return msg, true
				}
			}
		}
	}

	conn.SetContext(key)
	slog.Info("connection authenticated", "remote", conn.RemoteAddr(), "auth_key", key)
	return "", false
}

// authCommand implements the AUTH wire command for RedisX's dual-port
// (app + ctrl) model, accepting both the legacy and the ACL argument shape.
//
// Flow:
//  1. Argument shape: `AUTH <pass>` or `AUTH <user> <pass>` (ACL). RedisX has
//     no user catalogue, so any username other than "default" is rejected with
//     Redis' WRONGPASS text.
//  2. If the connection is already authenticated and the caller repeats
//     the same key we answer OK; if they send a DIFFERENT key we close
//     the connection (security: no silent re-auth downgrade).
//  3. Existence, per-key connection limit and port-role binding are delegated
//     to authenticateConn, which HELLO ... AUTH shares.
//  4. Internal-NS Write Guard forbids wire clients from mutating `_auth_:` keys
//     directly through SET / DEL; AUTH key rotation is a ctrl-only operation.
func authCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	username, providedKey, ok := parseAuthArgs(cmd)
	if !ok {
		conn.WriteError("ERR authentication failed")
		slog.Warn("auth format error", "remote", conn.RemoteAddr())
		_ = conn.Close()
		return
	}
	if username != "" && username != defaultUsername {
		conn.WriteError("WRONGPASS invalid username-password pair or user is disabled.")
		slog.Warn("auth rejected: unknown username", "remote", conn.RemoteAddr(), "username", username)
		_ = conn.Close()
		return
	}

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

	if wireErr, closeConn := authenticateConn(conn, db, providedKey); wireErr != "" {
		conn.WriteError(wireErr)
		if closeConn {
			_ = conn.Close()
		}
		return
	}
	conn.WriteString("OK")
}

// isHelloOption reports whether tok is one of the HELLO option keywords, i.e.
// whether the (optional) leading protocol version was omitted.
func isHelloOption(tok []byte) bool {
	switch strings.ToLower(string(tok)) {
	case "auth", "setname":
		return true
	default:
		return false
	}
}

// helloCommand implements the HELLO handshake, including its inline
// authentication form. Redis' grammar is:
//
//	HELLO [<proto> [AUTH <user> <pass>] [SETNAME <name>]]
//
// RedisX implements the subset real clients need:
//
//   - proto: 2 or 3. Anything else answers NOPROTO. The reply always reports
//     proto=2 because RedisX's encoder (tidwall/redcon) has no RESP3 reply
//     types (%, ~, |, >): claiming 3 would make clients wait for frame shapes
//     the server can never emit. go-redis' ReadMapLen accepts an RESP2 array
//     as a map, so helloOK stays true for RESP3-requesting clients.
//   - AUTH: the single-round-trip authentication path used by go-redis
//     (`HELLO 2 AUTH default <pass>`) and redis-py. Routed through the same
//     authenticateConn as AUTH, so acceptance and error texts are identical.
//   - SETNAME: accepted and ignored (RedisX keeps no client registry).
//
// Without inline AUTH, HELLO still demands an already-authenticated port and
// answers with Redis' own NOAUTH wording.
//
// Otherwise the response stays a minimal identity map — the client owns its
// command catalogue and UI distinctions, and all further metadata is read via
// dedicated wire commands (KEYS, SEARCHKEY, …), never via HELLO field inflation.
func helloCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	args := cmd.Args[1:]
	if len(args) > 0 && !isHelloOption(args[0]) {
		ver, err := strconv.Atoi(string(args[0]))
		if err != nil || (ver != 2 && ver != 3) {
			conn.WriteError("NOPROTO unsupported protocol version")
			return
		}
		args = args[1:]
	}

	var username, password string
	hasAuth := false
	for i := 0; i < len(args); {
		switch strings.ToLower(string(args[i])) {
		case "auth":
			if i+2 >= len(args) {
				conn.WriteError("ERR syntax error in HELLO")
				return
			}
			username, password, hasAuth = string(args[i+1]), string(args[i+2]), true
			i += 3
		case "setname":
			if i+1 >= len(args) {
				conn.WriteError("ERR syntax error in HELLO")
				return
			}
			i += 2
		default:
			conn.WriteError("ERR syntax error in HELLO option '" + string(args[i]) + "'")
			return
		}
	}

	if hasAuth {
		// Failed inline auth deliberately does NOT close the connection (unlike
		// AUTH): go-redis retries the legacy `AUTH <password>` on the same socket
		// after a HELLO error, so tearing it down would replace the server's
		// diagnostic with a bare EOF. The connection stays unauthenticated, and
		// every following command still hits the NOAUTH gate.
		if username != defaultUsername {
			conn.WriteError("WRONGPASS invalid username-password pair or user is disabled.")
			slog.Warn("hello auth rejected: unknown username", "remote", conn.RemoteAddr(), "username", username)
			return
		}
		prevAuth, _ := conn.Context().(string)
		switch {
		case prevAuth == password:
			// Already authenticated with this very key: idempotent, no re-acquire.
		case prevAuth != "":
			conn.WriteError("ERR connection already authenticated")
			slog.Warn("hello auth rejected on authenticated connection", "remote", conn.RemoteAddr(), "current_auth_key", prevAuth)
			return
		default:
			if wireErr, _ := authenticateConn(conn, db, password); wireErr != "" {
				conn.WriteError(wireErr)
				return
			}
		}
	}

	if !connAuthenticated(conn) {
		role := connPortRole(conn)
		if authRequired, err := portAuthRequired(db, role); authRequired {
			conn.WriteError("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> or AUTH <user> <pass> can be used to authenticate and select the protocol")
			slog.Warn("unauthenticated HELLO on port that requires AUTH",
				"remote", conn.RemoteAddr(), "cmd", cmdHello, "port_role", role.string(), "storage_error", err)
			return
		}
	}

	// RESP2 array of key/value pairs (redcon has no RESP3 map encoder).
	// `proto` reports the protocol actually in use, not the requested one.
	conn.WriteAny(map[string]any{"server": "redisx", "proto": 2})
}

// clientCommand answers CLIENT * subcommands. RedisX implements the minimum
// viable subset that keeps generic Redis clients happy: redis-py, ioredis, Jedis
// and go-redis all issue CLIENT SETINFO lib-name/… after HELLO, and go-redis
// additionally issues CLIENT MAINT_NOTIFICATIONS once it negotiated RESP3.
//
// MAINT_NOTIFICATIONS is rejected explicitly: RedisX has no maintenance/push
// channel, so answering +OK (as we used to) tells an RESP3 client that push
// notifications are enabled while nothing will ever arrive.
func clientCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) >= 2 && strings.EqualFold(string(cmd.Args[1]), "maint_notifications") {
		conn.WriteError("ERR Unknown subcommand or wrong number of arguments for 'MAINT_NOTIFICATIONS'. Try CLIENT HELP.")
		return
	}
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
