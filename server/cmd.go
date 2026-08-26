package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/x"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
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
				subFilters = append(subFilters, f)
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
				subFilters = append(subFilters, f)
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

// normalizeIndexPathFields extracts index paths from a REGIDX raw JSON
// payload, accepting either the single-field "path": "foo.bar" legacy
// form or the multi-field "paths": ["a","b.c"] form.
//
// The extracted strings are NOT canonicalised here — downstream
// writeIndexSpec / indexJSONComposite rely on dot→underscore
// replacement, which still happens inside regIdxCommand and
// canonicalIdxMD5 as their respective SSoT sites.
func normalizeIndexPathFields(rawMap map[string]any) ([]string, error) {
	if rawArr, ok := rawMap["paths"].([]any); ok && len(rawArr) > 0 {
		out := make([]string, 0, len(rawArr))
		for i, v := range rawArr {
			s, _ := v.(string)
			if s == "" {
				return nil, fmt.Errorf("paths[%d] is empty or not a string", i)
			}
			out = append(out, s)
		}
		return out, nil
	}
	if single, ok := rawMap["path"].(string); ok && single != "" {
		return []string{single}, nil
	}
	return nil, fmt.Errorf("either \"path\" (string, single-field) or \"paths\" ([string], multi-field) is required")
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

// clientCommand answers CLIENT * subcommands. RedisX intentionally
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

// ——— ——— 业务命令处理器 / KV + Typed Doc + Index + Registry ——— ———

// setCommand implements SET.
//
// Trust-User Colon rule (SSoT, via classifyArg1):
//   - arg1 contains ":" → KV native path (write-through to storage as-is,
//     NO schema, NO typed-doc registration checks, NX/EX/PX work exactly
//     like Redis). Internal-NS Write Guard enforced by
//     validateKVMutationKey(arg1) which rejects `_doc_:` / `_idx_:` /
//     `_auth_:` head segments (wire callers cannot mutate meta keys).
//   - arg1 does NOT contain ":" → typed-doc path: argv[1] is a namespace
//     (logical OR storageNs), value must be a JSON object or array of
//     objects. Each object is (a) checked against the registered
//     docSpec, (b) has its storage key derived via deriveDocKey
//     (SSoT: Schema → StorageKey), (c) written atomically via
//     db.setBatchAtomic. Schema TTL overrides caller TTL when caller
//     TTL is 0.
//
// Optional trailing flags: EX <seconds>, PX <milliseconds>, NX. Flags
// are order-independent, matching Redis. Note NX on doc-pattern is
// evaluated per-object, NOT across the whole batch: each pk either
// exists already (skipped, counts as N=0) or not (inserted, counts in
// the OK path).
func setCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	arg1 := string(cmd.Args[1])
	valueRaw := string(cmd.Args[2])

	var ttl time.Duration
	var nx bool
	if len(cmd.Args) > 3 {
		for i := 3; i < len(cmd.Args); i++ {
			arg := strings.ToUpper(string(cmd.Args[i]))
			if arg == "EX" && i+1 < len(cmd.Args) {
				secs, err := strconv.Atoi(string(cmd.Args[i+1]))
				if err != nil {
					conn.WriteError("ERR value is not an integer or out of range")
					return
				}
				ttl = time.Duration(secs) * time.Second
				i++
			} else if arg == "PX" && i+1 < len(cmd.Args) {
				msecs, err := strconv.Atoi(string(cmd.Args[i+1]))
				if err != nil {
					conn.WriteError("ERR value is not an integer or out of range")
					return
				}
				ttl = time.Duration(msecs) * time.Millisecond
				i++
			} else if arg == "NX" {
				nx = true
			}
		}
	}

	switch classifyArg1(arg1) {
	case argShapeKV:
		if err := validateKVMutationKey(arg1); err != nil {
			conn.WriteError("ERR SET " + err.Error())
			return
		}
		if nx {
			set, err := db.SetNXWithTtl(arg1, valueRaw, ttl)
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return
			}
			if set {
				conn.WriteString("OK")
			} else {
				conn.WriteNull()
			}
			return
		}
		if err := db.SetWithTtl(arg1, valueRaw, ttl); err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
		return
	case argShapeDoc:
		ns := arg1
		doc, ok := db.lookupDocByLogicalOrStorageNs(ns)
		if !ok {
			if appearsDocIntentJSON(valueRaw) {
				conn.WriteError(fmt.Sprintf("ERR SET namespace %q not registered — doc-pattern requires REGSCH first", ns))
			} else {
				conn.WriteError(fmt.Sprintf("ERR SET kv-pattern key must contain namespace separator %q", naming.StorageKeySeparator()))
			}
			return
		}
		if !gjson.Valid(valueRaw) {
			conn.WriteError("ERR SET doc-pattern: value must be valid JSON object or array of objects")
			return
		}
		root := gjson.Parse(valueRaw)
		var objects []string
		if root.IsArray() {
			root.ForEach(func(_, v gjson.Result) bool {
				if v.IsObject() {
					objects = append(objects, v.Raw)
				}
				return true
			})
			if len(objects) == 0 {
				conn.WriteError("ERR SET doc-pattern: array must contain JSON objects")
				return
			}
		} else if root.IsObject() {
			objects = []string{root.Raw}
		} else {
			conn.WriteError("ERR SET doc-pattern: value must be JSON object or array of objects")
			return
		}
		batch := make([]batchedWrite, 0, len(objects))
		for i, obj := range objects {
			dk, err := deriveDocKey(doc.Spec, doc.StorageNs, obj)
			if err != nil {
				conn.WriteError(fmt.Errorf("ERR SET doc-pattern: object[%d]: %w", i, err).Error())
				return
			}
			finalTTL := ttl
			if finalTTL == 0 {
				finalTTL = doc.Spec.TTL
			}
			batch = append(batch, batchedWrite{
				Key:   dk.FullStorageKey,
				Value: obj,
				TTL:   finalTTL,
			})
		}
		n, err := db.setBatchAtomic(batch, nx)
		if err != nil {
			if nx && errors.Is(err, errNxPreconditionFailed) {
				conn.WriteNull()
				return
			}
			conn.WriteError("ERR SET " + err.Error())
			return
		}
		if nx {
			if n == 0 {
				conn.WriteNull()
			} else {
				conn.WriteString("OK")
			}
			return
		}
		conn.WriteString("OK")
		return
	}
}

// setExCommand implements SETEX (TTL in seconds, mandatory, no NX).
//
// Semantics mirror setCommand's KV/doc paths with the single
// simplification that TTL is always present and argc is strictly 4.
// Internal-NS Write Guard is enforced identically via
// validateKVMutationKey on the KV branch.
func setExCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	secs, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}
	ttl := time.Duration(secs) * time.Second
	arg1 := string(cmd.Args[1])
	val := string(cmd.Args[3])
	switch classifyArg1(arg1) {
	case argShapeKV:
		if err := validateKVMutationKey(arg1); err != nil {
			conn.WriteError("ERR SETEX " + err.Error())
			return
		}
		if err := db.SetWithTtl(arg1, val, ttl); err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
		return
	case argShapeDoc:
		ns := arg1
		doc, ok := db.lookupDocByLogicalOrStorageNs(ns)
		if !ok {
			if appearsDocIntentJSON(val) {
				conn.WriteError(fmt.Sprintf("ERR SETEX namespace %q not registered — doc-pattern requires REGSCH first", ns))
			} else {
				conn.WriteError(fmt.Sprintf("ERR SETEX kv-pattern key must contain namespace separator %q", naming.StorageKeySeparator()))
			}
			return
		}
		if !gjson.Valid(val) {
			conn.WriteError("ERR SETEX doc-pattern: value must be valid JSON object or array")
			return
		}
		root := gjson.Parse(val)
		var objects []string
		if root.IsArray() {
			root.ForEach(func(_, v gjson.Result) bool {
				if v.IsObject() {
					objects = append(objects, v.Raw)
				}
				return true
			})
			if len(objects) == 0 {
				conn.WriteError("ERR SETEX doc-pattern: array must contain JSON objects")
				return
			}
		} else if root.IsObject() {
			objects = []string{root.Raw}
		} else {
			conn.WriteError("ERR SETEX doc-pattern: value must be JSON object or array of objects")
			return
		}
		batch := make([]batchedWrite, 0, len(objects))
		for i, obj := range objects {
			dk, err := deriveDocKey(doc.Spec, doc.StorageNs, obj)
			if err != nil {
				conn.WriteError(fmt.Errorf("ERR SETEX doc-pattern: object[%d]: %w", i, err).Error())
				return
			}
			batch = append(batch, batchedWrite{
				Key:   dk.FullStorageKey,
				Value: obj,
				TTL:   ttl,
			})
		}
		if _, err := db.setBatchAtomic(batch, false); err != nil {
			conn.WriteError("ERR SETEX " + err.Error())
			return
		}
		conn.WriteString("OK")
		return
	}
}

// setNxCommand implements SETNX (write if not-exists; integer return:
// 1 = written, 0 = precondition failed).
//
// Doc-path semantics differ slightly from KV-path semantics on
// duplicate PK in a single batch: setBatchAtomic reports
// errNxPreconditionFailed on ANY duplicate in the batch so callers get
// deterministic "nothing written" rather than partial writes. KV-path
// uses BuntDB native SetNX (single-key, no batch ambiguity).
func setNxCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	arg1 := string(cmd.Args[1])
	val := string(cmd.Args[2])
	switch classifyArg1(arg1) {
	case argShapeKV:
		if err := validateKVMutationKey(arg1); err != nil {
			conn.WriteError("ERR SETNX " + err.Error())
			return
		}
		set, err := db.SetNX(arg1, val)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else if set {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return
	case argShapeDoc:
		ns := arg1
		doc, ok := db.lookupDocByLogicalOrStorageNs(ns)
		if !ok {
			if appearsDocIntentJSON(val) {
				conn.WriteError(fmt.Sprintf("ERR SETNX namespace %q not registered — doc-pattern requires REGSCH first", ns))
			} else {
				conn.WriteError(fmt.Sprintf("ERR SETNX kv-pattern key must contain namespace separator %q", naming.StorageKeySeparator()))
			}
			return
		}
		if !gjson.Valid(val) {
			conn.WriteError("ERR SETNX doc-pattern: value must be valid JSON object or array")
			return
		}
		root := gjson.Parse(val)
		var objects []string
		if root.IsArray() {
			root.ForEach(func(_, v gjson.Result) bool {
				if v.IsObject() {
					objects = append(objects, v.Raw)
				}
				return true
			})
			if len(objects) == 0 {
				conn.WriteError("ERR SETNX doc-pattern: array must contain JSON objects")
				return
			}
		} else if root.IsObject() {
			objects = []string{root.Raw}
		} else {
			conn.WriteError("ERR SETNX doc-pattern: value must be JSON object or array of objects")
			return
		}
		batch := make([]batchedWrite, 0, len(objects))
		for i, obj := range objects {
			dk, err := deriveDocKey(doc.Spec, doc.StorageNs, obj)
			if err != nil {
				conn.WriteError(fmt.Errorf("ERR SETNX doc-pattern: object[%d]: %w", i, err).Error())
				return
			}
			batch = append(batch, batchedWrite{
				Key:   dk.FullStorageKey,
				Value: obj,
				TTL:   doc.Spec.TTL,
			})
		}
		n, err := db.setBatchAtomic(batch, true)
		if err != nil {
			if errors.Is(err, errNxPreconditionFailed) {
				conn.WriteInt(0)
				return
			}
			conn.WriteError("ERR SETNX " + err.Error())
			return
		}
		if n > 0 {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return
	}
}

// getCommand implements GET.
//
// Trust-User Colon rule:
//   - arg1 contains ":" → KV native path (single full key; NOT an array
//     form — multi-key reads are intentionally not exposed on raw KV to
//     match the DEL constraint).
//   - arg1 does NOT contain ":" → typed-doc path: argc MUST be 3
//     (GET <ns> <pk-suffix>), storageNs derived via
//     lookupDocByLogicalOrStorageNs, final key constructed via
//     naming.BuildStorageKey(storageNs, suffix).
//
// Buntdb.ErrNotFound maps to RESP WriteNull (nil bulk), matching Redis
// native GET semantics on miss.
func getCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 || len(cmd.Args) > 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command — usage: GET <kv-full-key>  or  GET <doc-ns> <pk-suffix>")
		return
	}
	arg1 := string(cmd.Args[1])
	switch classifyArg1(arg1) {
	case argShapeKV:
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR GET kv-pattern accepts exactly 1 key argument; doc-pattern uses GET <ns> <pk-suffix>")
			return
		}
		if err := validateKVFullKey(arg1); err != nil {
			conn.WriteError("ERR GET " + err.Error())
			return
		}
		res := db.Get(arg1)
		if res.IsError() {
			if errors.Is(res.Error(), buntdb.ErrNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError("ERR " + res.Error().Error())
			}
		} else {
			conn.WriteBulk([]byte(res.MustGet()))
		}
		return
	case argShapeDoc:
		if len(cmd.Args) != 3 {
			if _, docRegistered := db.lookupDocByLogicalOrStorageNs(arg1); docRegistered {
				conn.WriteError(fmt.Sprintf("ERR GET doc-pattern requires <ns> <pk-suffix>; namespace %q alone is not a query (use SEARCHKEY or KEYS on ctrl port)", arg1))
			} else {
				conn.WriteError(fmt.Sprintf("ERR GET kv-pattern key must contain namespace separator %q", naming.StorageKeySeparator()))
			}
			return
		}
		ns := arg1
		suffix := string(cmd.Args[2])
		if err := validatePKSuffixNoColon(suffix); err != nil {
			conn.WriteError("ERR GET " + err.Error())
			return
		}
		doc, ok := db.lookupDocByLogicalOrStorageNs(ns)
		if !ok {
			conn.WriteError(fmt.Sprintf("ERR GET namespace %q not registered — doc-pattern requires REGSCH first", ns))
			return
		}
		fullKey := naming.BuildStorageKey(doc.StorageNs, suffix)
		res := db.Get(fullKey)
		if res.IsError() {
			if errors.Is(res.Error(), buntdb.ErrNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError("ERR " + res.Error().Error())
			}
		} else {
			conn.WriteBulk([]byte(res.MustGet()))
		}
		return
	}
}

// delCommand implements DEL.
//
// Two allowed forms (KV-path DEL Single-Key Guard):
//
//  1. argc == 2: single-key delete. classifyArg1(arg1) routes into KV
//     (argShapeKV: one raw full key, validateKVMutationKey enforces
//     Internal-NS Write Guard) OR doc namespace alone (an ERROR —
//     caller should pass at least one pk-suffix to del).
//  2. argc > 2: MULTI delete on typed docs ONLY. If arg1 still looks
//     like raw KV (classifyArg1 == argShapeKV) we refuse outright and
//     tell the caller to issue separate DELs per key or switch to
//     doc-path form. Otherwise arg1 is a registered ns, remaining
//     argv are pk-suffixes, we BuildStorageKey each one, then
//     everything is deleted in one BuntDB transaction via
//     db.deleteBatchAtomic.
//
// Note on "native Redis DEL M N" semantics: Redis allows DEL k1 k2 … kn
// of unrelated keys. RedisX KV native path intentionally disallows
// this. Callers that really need a multi-raw-key delete on arbitrary
// non-doc keys can call `PIPELINE; DEL a; DEL b; EXEC`.
func delCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	if len(cmd.Args) == 2 {
		arg1 := string(cmd.Args[1])
		switch classifyArg1(arg1) {
		case argShapeKV:
			if err := validateKVMutationKey(arg1); err != nil {
				conn.WriteError("ERR DEL " + err.Error())
				return
			}
			var deleted bool
			deleted, err := db.Delete(arg1)
			if err != nil {
				conn.WriteError("ERR " + err.Error())
			} else if deleted {
				conn.WriteInt(1)
			} else {
				conn.WriteInt(0)
			}
			return
		case argShapeDoc:
			if _, docRegistered := db.lookupDocByLogicalOrStorageNs(arg1); docRegistered {
				conn.WriteError(fmt.Sprintf("ERR DEL doc-pattern requires <ns> <pk-suffix>; namespace %q alone is not a delete target", arg1))
			} else {
				conn.WriteError(fmt.Sprintf("ERR DEL kv-pattern key must contain namespace separator %q", naming.StorageKeySeparator()))
			}
			return
		}
	}
	arg1 := string(cmd.Args[1])
	if classifyArg1(arg1) == argShapeKV {
		conn.WriteError("ERR DEL kv-pattern takes exactly one full key; to delete multiple KV-path keys issue separate DEL commands, or use doc-path form `DEL <registered-ns> <pk1> [pk2 …]` to delete multiple pk-suffix values for a single registered typed-document namespace.")
		return
	}
	doc, ok := db.lookupDocByLogicalOrStorageNs(arg1)
	if !ok {
		conn.WriteError(fmt.Sprintf("ERR DEL namespace %q not registered — doc-pattern requires REGSCH first", arg1))
		return
	}
	suffixes := make([]string, 0, len(cmd.Args)-2)
	for i := 2; i < len(cmd.Args); i++ {
		s := string(cmd.Args[i])
		if err := validatePKSuffixNoColon(s); err != nil {
			conn.WriteError("ERR DEL " + err.Error())
			return
		}
		suffixes = append(suffixes, s)
	}
	keys := make([]string, 0, len(suffixes))
	for _, s := range suffixes {
		keys = append(keys, naming.BuildStorageKey(doc.StorageNs, s))
	}
	n, err := db.deleteBatchAtomic(keys)
	if err != nil {
		conn.WriteError("ERR DEL " + err.Error())
		return
	}
	conn.WriteInt(n)
}

// keysCommand implements KEYS.
//
// Guardrails (applied BEFORE touching storage):
//   - no leading glob wildcard (would force full DB scan + defeat
//     BuntDB ordered index usefulness)
//   - no leading underscore (forbids enumerating `_doc_:` / `_idx_:` /
//     `_auth_:` meta keys — wire-level enumeration of registry data is
//     a ctrl-only exposure and goes via REGSCH/REGIDX introspection,
//     not raw KEYS).
//
// Routing via SSoTs:
//   - resolveLayer(keyPattern) — storage-layer decision is delegated to
//     the exact same SSoT that SET/GET/DELETE use; hand-written
//     HasMemPrefix/IsInternalStorageNs branches were removed here to
//     keep layer semantics in one place (db.go resolveLayer).
//   - classifyArg1(keyPattern) == argShapeKV → raw KV pattern, pass
//     straight through to db.Keys (which itself calls applyKeyRange and
//     isLeadingWildcard guarded again).
//   - otherwise (doc-path shape):
//   - if pattern contains ANY glob: forbidden (ctrl must pass bare
//     namespace only). Enumeration within a doc-ns with filters goes
//     via SEARCHKEY, not KEYS.
//   - otherwise the pattern is a bare storageNs: look it up via
//     lookupDocByStorageNs to ensure REGSCH has run, then rewrite
//     to `<ns>:*` via naming.BuildStorageKey(ns, "*").
//
// Output is a RESP array of bulk strings exactly matching Redis native
// KEYS output shape; lengths >0 on a single mem/disk layer combined so
// callers see `_m_:app:u1` and `app:u1` both in the same listing when
// both exist (Metadata Symmetric Storage SSoT).
func keysCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	keyPattern := string(cmd.Args[1])
	if hasLeadingWildcard(keyPattern) {
		conn.WriteError("ERR forbidden key pattern")
		return
	}
	if naming.HasUnderscorePrefix(keyPattern) {
		conn.WriteError("ERR forbidden key pattern")
		return
	}

	layer, constrained, rlErr := resolveLayer(keyPattern)
	if rlErr != nil {
		conn.WriteError("ERR KEYS " + rlErr.Error())
		return
	}
	// mem-layer or internal-ns or leading-wildcard unconstrained: pass
	// pattern through as-is. These are the only code paths that let
	// `_m_:app:*` or a pre-formulated `app:*` KV pattern reach storage.
	if constrained {
		switch layer {
		case storageMem:
			// falls through → no doc-path path transformation needed
		default:
			switch classifyArg1(keyPattern) {
			case argShapeKV:
				// raw KV pattern: pass through unchanged
			default:
				if strings.ContainsAny(keyPattern, "*?[") {
					conn.WriteError("ERR KEYS for doc-path requires bare <storageNs> without wildcards or colons. Glob patterns like 'abc*' on a naked namespace fragment are forbidden; patterns containing ':' (like 'app:prefix*') are treated as raw KV-path and allowed freely. Use SEARCHKEY <ns> <keyrange_json> for typed-document scoped queries.")
					return
				}
				if _, ok := db.lookupDocByStorageNs(keyPattern); !ok {
					conn.WriteError(fmt.Sprintf("ERR KEYS namespace %q is not registered — doc-path KEYS requires REGSCH first. For raw KV enumeration, pass a pattern containing ':' (e.g. '%s:*' or '%s:prefix*').", keyPattern, keyPattern, keyPattern))
					return
				}
				keyPattern = naming.BuildStorageKey(keyPattern, "*")
			}
		}
	}

	res := db.Keys(keyPattern)
	if res.IsError() {
		conn.WriteError("ERR " + res.Error().Error())
	} else {
		keys := res.MustGet()
		conn.WriteArray(len(keys))
		for _, key := range keys {
			conn.WriteBulk([]byte(key))
		}
	}
}

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

// searchIndexCommand implements SEARCHINDEX.
//
// Wire form (argv after cmd-name, so nArgs = len(cmd.Args)-1):
//
//	SEARCHINDEX <fullIndexName> <keyRangeJSON> <filterJSON> \
//	            [ASC|DESC] [LIMIT N]
//
// Index name format (SSoT: naming.ParseIdxFullName): `<ownerNs>!_!<logical>`.
// The ownerNs portion must either be an internal storage ns (rare:
// indexes over `_auth_:` etc.) or match a registered doc
// (lookupDocByStorageNs). Index objects on non-registered owner
// namespaces cannot be searched because applyKeyRange needs a
// documented layer to route to.
//
// Optional tail args parsed by positional switch on nArgs (3..6):
//   - nArgs==4 → ASC/DESC only
//   - nArgs==5 → LIMIT only
//   - nArgs==6 → ASC/DESC + LIMIT
//
// Buntdb.ErrNotFound maps to empty RESP array (same as empty index on
// matching filter). All other errors surface via `ERR` wire message.
func searchIndexCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	nArgs := len(cmd.Args) - 1
	if nArgs < 3 || nArgs > 6 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	indexName := string(cmd.Args[1])
	krJSON := string(cmd.Args[2])
	filterJSON := string(cmd.Args[3])
	if len(krJSON) == 0 || krJSON[0] != '{' {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	kr, krErr := x.UnmarshalKeyRange([]byte(krJSON))
	if krErr != nil {
		conn.WriteError("ERR invalid key range: " + krErr.Error())
		return
	}

	ownerNs, _, parseErr := naming.ParseIdxFullName(indexName)
	if parseErr != nil {
		conn.WriteError("ERR SEARCHINDEX invalid index name: " + parseErr.Error())
		return
	}
	if !naming.IsInternalStorageNs(ownerNs) {
		if _, ok := db.lookupDocByStorageNs(ownerNs); !ok {
			conn.WriteError(fmt.Sprintf("ERR SEARCHINDEX index %q belongs to namespace %q which is not registered — doc-pattern requires REGSCH first", indexName, ownerNs))
			return
		}
	}

	desc := false
	switch nArgs {
	case 4:
		a := strings.ToUpper(string(cmd.Args[4]))
		if a != "ASC" && a != "DESC" {
			conn.WriteError("ERR invalid order: " + string(cmd.Args[4]))
			return
		}
		desc = a == "DESC"
	case 5:
		a := strings.ToUpper(string(cmd.Args[4]))
		if a != "LIMIT" {
			conn.WriteError("ERR invalid argument: " + string(cmd.Args[4]))
			return
		}
		countStr := string(cmd.Args[5])
		count, err := strconv.Atoi(countStr)
		if err != nil || count <= 0 {
			conn.WriteError("ERR invalid count for LIMIT: " + countStr)
			return
		}
		kr = kr.Limit(count)
	case 6:
		a4 := strings.ToUpper(string(cmd.Args[4]))
		a5 := strings.ToUpper(string(cmd.Args[5]))
		if a4 != "ASC" && a4 != "DESC" {
			conn.WriteError("ERR invalid order: " + string(cmd.Args[4]))
			return
		}
		desc = a4 == "DESC"
		if a5 != "LIMIT" {
			conn.WriteError("ERR invalid argument: " + string(cmd.Args[5]))
			return
		}
		countStr := string(cmd.Args[6])
		count, err := strconv.Atoi(countStr)
		if err != nil || count <= 0 {
			conn.WriteError("ERR invalid count for LIMIT: " + countStr)
			return
		}
		kr = kr.Limit(count)
	}

	filter, err := parseFilter(filterJSON)
	if err != nil {
		conn.WriteError("ERR invalid query: " + err.Error())
		return
	}

	res := db.SearchIndex(indexName, kr, filter, desc)
	if res.IsError() {
		if errors.Is(res.Error(), buntdb.ErrNotFound) {
			conn.WriteArray(0)
		} else {
			conn.WriteError("ERR " + res.Error().Error())
		}
	} else {
		results := res.MustGet()
		conn.WriteArray(len(results))
		for _, val := range results {
			conn.WriteBulkString(val)
		}
	}
}

// updateCommand implements UPDATE (JSON-patch style).
//
// Key design: UPDATE works on typed documents ONLY, NEVER on raw KV
// (SSoT: doc-pattern UPDATE-only; raw-KV mutators go through SET /
// DEL). This is enforced via storageNsFromKRAnchor + registered-schema
// check. Callers wanting raw SET should use SET with a colon-key.
//
// Wire form:
//
//	UPDATE <keyRangeJSON|shorthand_pattern> <filterJSON> <updateJSON> \
//	       [LIMIT N]
//
// keyRangeJSON is first forwarded through normalizeKRJSONArg() to
// accept both `{…}` object form and the `<ns>:*` shorthand string form.
//
// updateJSON semantics forwarded directly to x.ParseUpdate (SSoT for
// JSON patch = `x.ParseUpdate`): list of {op, path, value} ops e.g.
// `[{"op":"replace","path":"/age","value":21}]`.
//
// The layer routing decision (disk vs mem) is handled SSoT-style by
// db.Update via resolveLayer + x.LayerRoutingConstrained callback;
// cmd.go only validates that the anchor is a registered storageNs (to
// produce a good error instead of an opaque 0-updates result).
func updateCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	nArgs := len(cmd.Args) - 1
	if nArgs != 3 && nArgs != 5 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	krJSON := string(cmd.Args[1])
	filterJSON := string(cmd.Args[2])
	updateJSON := string(cmd.Args[3])
	krBytes := normalizeKRJSONArg(krJSON)
	kr, krErr := x.UnmarshalKeyRange(krBytes)
	if krErr != nil {
		conn.WriteError("ERR invalid key range: " + krErr.Error())
		return
	}

	anchor := x.LayerRoutingAnchor(kr)
	storageNs, nsErr := storageNsFromKRAnchor(anchor)
	if nsErr != nil || storageNs == "" {
		conn.WriteError("ERR UPDATE operates on typed documents only; key-range must be anchored to a registered storage namespace prefix (format: '<storageNs>:<range-suffixes>'). Use SET on the KV-path (arg1 contains ':' for full-key writes) to mutate arbitrary raw keys, or use doc-path UPDATE with a KeyRange scoped to a single registered namespace.")
		return
	}
	if !naming.IsInternalStorageNs(storageNs) {
		if _, ok := db.lookupDocByStorageNs(storageNs); !ok {
			conn.WriteError(fmt.Sprintf("ERR UPDATE namespace %q (resolved from key-range anchor %q) is not registered — doc-pattern requires REGSCH first", storageNs, anchor))
			return
		}
	}

	if nArgs == 5 {
		a := strings.ToUpper(string(cmd.Args[4]))
		if a != "LIMIT" {
			conn.WriteError("ERR invalid argument: " + string(cmd.Args[4]))
			return
		}
		countStr := string(cmd.Args[5])
		count, err := strconv.Atoi(countStr)
		if err != nil || count <= 0 {
			conn.WriteError("ERR invalid count for LIMIT: " + countStr)
			return
		}
		kr = kr.Limit(count)
	}

	filter, err := parseFilter(filterJSON)
	if err != nil {
		conn.WriteError("ERR invalid query: " + err.Error())
		return
	}

	pairs, err := x.ParseUpdate(updateJSON)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	res := db.Update(kr, filter, pairs...)
	if res.IsError() {
		conn.WriteError("ERR " + res.Error().Error())
	} else {
		updatedKeys := res.MustGet()
		conn.WriteArray(len(updatedKeys))
		for _, key := range updatedKeys {
			conn.WriteBulk([]byte(key))
		}
	}
}

// searchKeyCommand implements SEARCHKEY (filter → pick keys, without
// loading a named index). Often called instead of KEYS when a caller
// wants server-side filtering or ordered iteration over a doc
// namespace, since KEYS on doc-path only returns the raw storage keys
// in BuntDB AscendKeys order with no filter.
//
// Wire form (nArgs = argv after command):
//
//	SEARCHKEY <keyRangeJSON|shorthand> <filterJSON> [ASC|DESC] \
//	          [LIMIT N]
//
// Key-range routing: SEARCHKEY is more permissive than UPDATE — it can
// scan raw KV-paths too. This is why the storageNs validation block
// (L1046) only fires when anchor is colon-free AND looks like a doc
// storageNs (so missing REGSCH → actionable error). Anchors with a
// colon (raw KV form) skip that validation branch entirely and let
// applyKeyRange sort out layer/disallow paths.
//
// Tail argc parsed the same 3/4/5 switch as SEARCHINDEX.
func searchKeyCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	nArgs := len(cmd.Args) - 1
	if nArgs < 2 || nArgs > 5 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	krJSON := string(cmd.Args[1])
	filterJSON := string(cmd.Args[2])
	krBytes := normalizeKRJSONArg(krJSON)
	kr, krErr := x.UnmarshalKeyRange(krBytes)
	if krErr != nil {
		conn.WriteError("ERR invalid key range: " + krErr.Error())
		return
	}

	anchor := x.LayerRoutingAnchor(kr)
	if anchor == "" || hasLeadingWildcard(anchor) {
		conn.WriteError("ERR SEARCHKEY key-range must be anchored to a namespace (no leading wildcard); prefix your range with a registered namespace like \"ns:*\" or use SEARCHINDEX for ad-hoc queries")
		return
	}
	if !strings.Contains(anchor, ":") {
		if storageNs, nsErr := storageNsFromKRAnchor(anchor); nsErr == nil && storageNs != "" {
			if _, ok := db.lookupDocByStorageNs(storageNs); !ok {
				conn.WriteError(fmt.Sprintf("ERR SEARCHKEY namespace %q (resolved from key-range anchor %q) is not registered — doc-pattern requires REGSCH first", storageNs, anchor))
				return
			}
		}
	}

	desc := false
	switch nArgs {
	case 3:
		a := strings.ToUpper(string(cmd.Args[3]))
		if a != "ASC" && a != "DESC" {
			conn.WriteError("ERR invalid order: " + string(cmd.Args[3]))
			return
		}
		desc = a == "DESC"
	case 4:
		a := strings.ToUpper(string(cmd.Args[3]))
		if a != "LIMIT" {
			conn.WriteError("ERR invalid argument: " + string(cmd.Args[3]))
			return
		}
		countStr := string(cmd.Args[4])
		count, err := strconv.Atoi(countStr)
		if err != nil || count <= 0 {
			conn.WriteError("ERR invalid count for LIMIT: " + countStr)
			return
		}
		kr = kr.Limit(count)
	case 5:
		a3 := strings.ToUpper(string(cmd.Args[3]))
		a4 := strings.ToUpper(string(cmd.Args[4]))
		if a3 != "ASC" && a3 != "DESC" {
			conn.WriteError("ERR invalid order: " + string(cmd.Args[3]))
			return
		}
		desc = a3 == "DESC"
		if a4 != "LIMIT" {
			conn.WriteError("ERR invalid argument: " + string(cmd.Args[4]))
			return
		}
		countStr := string(cmd.Args[5])
		count, err := strconv.Atoi(countStr)
		if err != nil || count <= 0 {
			conn.WriteError("ERR invalid count for LIMIT: " + countStr)
			return
		}
		kr = kr.Limit(count)
	}

	filter, err := parseFilter(filterJSON)
	if err != nil {
		conn.WriteError("ERR invalid query: " + err.Error())
		return
	}

	res := db.SearchKey(kr, filter, desc)
	if res.IsError() {
		if errors.Is(res.Error(), buntdb.ErrNotFound) {
			conn.WriteArray(0)
		} else {
			conn.WriteError("ERR " + res.Error().Error())
		}
	} else {
		results := res.MustGet()
		conn.WriteArray(len(results))
		for _, val := range results {
			conn.WriteBulkString(val)
		}
	}
}

// regSchemaCommand implements REGSCH (register typed-document schema).
//
// JSON payload must match the public x.Schema fields; unknown fields
// are rejected (via json.Decoder.DisallowUnknownFields) to avoid silent
// typos like "indexes" on the payload (the review guard below
// explicitly catches `"indexes"` first so users are told to use REGIDX
// rather than generic "unknown field").
//
// Semantic validation (field types, TTL positive, pk+attribute paths
// unique, ValidateDocLogicalNamespace compliance) lives inside
// db.writeDocSpec and is the SSoT for schema checks; cmd.go only does
// JSON-shape + unknown-field guard.
//
// Internal-NS Write Guard doesn't apply here because REGSCH itself IS
// the designated ctrl write path for `_doc_:` meta keys — wire
// clients cannot SET _doc_:abc directly, they must go through this
// command.
func regSchemaCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'regsch' command: usage: REGSCH <json-spec>")
		return
	}
	rawJSON := string(cmd.Args[1])
	if !gjson.Valid(rawJSON) {
		conn.WriteError("ERR REGSCH invalid JSON format")
		return
	}
	var rawMap map[string]any
	if err := json.Unmarshal([]byte(rawJSON), &rawMap); err != nil {
		conn.WriteError("ERR REGSCH invalid JSON: " + err.Error())
		return
	}
	if _, has := rawMap["indexes"]; has {
		conn.WriteError("ERR REGSCH schema contains reserved field 'indexes'; register indexes via REGIDX, not inside REGSCH payload")
		return
	}
	var spec docSpec
	dec := json.NewDecoder(strings.NewReader(rawJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		conn.WriteError("ERR REGSCH schema: " + err.Error())
		return
	}
	if err := db.writeDocSpec(spec); err != nil {
		conn.WriteError("ERR REGSCH " + err.Error())
		return
	}
	conn.WriteString("OK")
}

// dropSchemaCommand implements DROPSCH.
//
// Usage (argv strict): DROPSCH <logicalNs>
//
// Semantic constraints enforced in db.dropSchemaByLogicalNs (SSoT):
//   - DROPSCH refuses to drop if any attached indexes remain, listing
//     the index names so callers know which DROPIDX calls to issue
//     first. (Registry coarse-grained write lock held for the whole
//     op; caller must not mix parallel REGSCH/DROPSCH.)
//   - Both mem and disk layer meta-keys + storage ns scopes are
//     symmetrically cleaned up regardless of whether the schema was
//     declared Mem=true or disk-only (Metadata Symmetric Storage
//     SSoT).
func dropSchemaCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	argc := len(cmd.Args)
	if argc < 2 || argc > 2 {
		conn.WriteError("ERR wrong number of arguments for 'dropsch' command: usage: DROPSCH <logical_ns>")
		return
	}
	logicalNs := string(cmd.Args[1])
	if err := naming.ValidateDocLogicalNamespace(logicalNs); err != nil {
		conn.WriteError("ERR DROPSCH " + err.Error())
		return
	}
	if err := db.dropSchemaByLogicalNs(logicalNs); err != nil {
		conn.WriteError("ERR DROPSCH " + err.Error())
		return
	}
	conn.WriteString("OK")
}

// regIdxCommand implements REGIDX (register a typed index on a
// registered typed-document owner namespace).
//
// Accepts three JSON forms, tried in precedence order:
//
//  1. { "full_name": "<ownerNs>!_!<logical>" } — fully-qualified form
//     from ParseIdxFullName; takes precedence when present.
//  2. { "owner_ns": "<storageNs>", "logical": "age_idx" } + optional
//     "owner_doc_logical_ns" + "owner_mem" — when "owner_ns" is a raw
//     storageNs, but the caller wants to override owner_mem from a
//     `_m_:` prefix embedded inside "owner_ns" we used to write a
//     hand-rolled naming.HasMemPrefix check; it now goes through the
//     resolveLayer SSoT (append a trailing colon to turn the string
//     into a storage-key shape so classifyArg1 → resolveLayer
//     semantics apply identically to runtime data keys).
//  3. { "owner_doc_logical_ns": "user", "logical": "age_idx",
//     "owner_mem": true } — pure logical-ns shortcut without a
//     storageNs override on owner_ns.
//
// Path fields come from normalizeIndexPathFields() (accepts "path" or
// "paths"). Dot → underscore normalisation happens HERE in
// regIdxCommand (before write) rather than in writeIndexSpec, so that
// indexes registered with identical semantic paths (via either JSON
// form) compare equal through canonicalIdxMD5 and avoid duplicate
// meta writes (SSoT for MD5 content = canonicalIdxMD5, which also
// runs the same dot→underscore substitution independently for
// idempotency).
func regIdxCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	argc := len(cmd.Args)
	if argc != 2 {
		conn.WriteError("ERR wrong number of arguments for 'regidx' command: usage: " + proto.UsageRegisterIndex)
		return
	}
	var spec idxSpec
	rawJSON := string(cmd.Args[1])
	if !gjson.Valid(rawJSON) {
		conn.WriteError("ERR REGIDX invalid JSON format")
		return
	}
	var rawMap map[string]any
	if err := json.Unmarshal([]byte(rawJSON), &rawMap); err != nil {
		conn.WriteError("ERR REGIDX invalid JSON: " + err.Error())
		return
	}
	if v, ok := rawMap["full_name"].(string); ok && v != "" {
		ownerNs, logical, err := naming.ParseIdxFullName(v)
		if err != nil {
			conn.WriteError("ERR REGIDX " + err.Error())
			return
		}
		spec.OwnerNs = ownerNs
		spec.Logical = logical
	} else if on, _ := rawMap["owner_ns"].(string); on != "" {
		lg, _ := rawMap["logical"].(string)
		if lg == "" {
			if v := gjson.Get(rawJSON, "owner_doc_logical_ns").String(); v != "" {
				ownerMem := false
				// SSoT routing check: prefer resolveLayer over
				// hand-rolled naming.HasMemPrefix(on). Append ':'
				// to turn `on` into a key-shape argument the same
				// way runtime SET/GET would see it.
				if on != "" {
					if l, _, err := resolveLayer(on + ":"); err == nil {
						ownerMem = l == storageMem
					}
				}
				if !ownerMem {
					ownerMem = gjson.Get(rawJSON, "owner_mem").Bool()
				}
				if err := naming.ValidateDocLogicalNamespace(v); err != nil {
					conn.WriteError("ERR REGIDX " + err.Error())
					return
				}
				on = naming.BuildStorageNs(v, ownerMem)
				rawMap["owner_ns"] = on
			}
			lg = gjson.Get(rawJSON, "logical").String()
		}
		if on == "" || lg == "" {
			conn.WriteError("ERR REGIDX either full_name or (owner_ns + logical) is required")
			return
		}
		if err := naming.ValidateLogicalIndexName(lg); err != nil {
			conn.WriteError("ERR REGIDX " + err.Error())
			return
		}
		spec.OwnerNs = on
		spec.Logical = strings.ToLower(lg)
	} else if v := gjson.Get(rawJSON, "owner_doc_logical_ns").String(); v != "" {
		lg := gjson.Get(rawJSON, "logical").String()
		if lg == "" {
			conn.WriteError("ERR REGIDX logical is required")
			return
		}
		ownerMem := gjson.Get(rawJSON, "owner_mem").Bool()
		if err := naming.ValidateDocLogicalNamespace(v); err != nil {
			conn.WriteError("ERR REGIDX " + err.Error())
			return
		}
		if err := naming.ValidateLogicalIndexName(lg); err != nil {
			conn.WriteError("ERR REGIDX " + err.Error())
			return
		}
		spec.OwnerNs = naming.BuildStorageNs(v, ownerMem)
		spec.Logical = strings.ToLower(lg)
	} else {
		conn.WriteError("ERR REGIDX either full_name or owner_ns + logical is required")
		return
	}
	paths, err := normalizeIndexPathFields(rawMap)
	if err != nil {
		conn.WriteError("ERR REGIDX " + err.Error())
		return
	}
	spec.Paths = paths
	spec.KeyPattern, _ = rawMap["key_pattern"].(string)
	// restore owner_ns/logical from explicit resolution above (takes precedence over raw JSON)
	if spec.OwnerNs == "" {
		spec.OwnerNs, _ = rawMap["owner_ns"].(string)
	}
	if spec.Logical == "" {
		spec.Logical, _ = rawMap["logical"].(string)
	}
	if spec.OwnerNs == "" || spec.Logical == "" {
		conn.WriteError("ERR REGIDX owner_ns and logical are required")
		return
	}
	if spec.KeyPattern == "" {
		spec.KeyPattern = naming.StorageNsScope(spec.OwnerNs) + "*"
	}
	for i, p := range spec.Paths {
		spec.Paths[i] = strings.ReplaceAll(p, ".", "_")
	}
	if err := db.writeIndexSpec(spec); err != nil {
		conn.WriteError("ERR REGIDX " + err.Error())
		return
	}
	conn.WriteString("OK")
}

// dropIndexCommand implements DROPIDX.
//
// Two call forms:
//
//  1. DROPIDX <fullName>    — argv==2. ParseIdxFullName compatible,
//     forwards straight to db.dropIndexByFullName. Layer choice is
//     implicit: the fullName already embeds storageNs (which includes
//     the `_m_:` prefix when Mem=true), so a single drop call is
//     enough — the coarse-grained registry write lock inside
//     dropIndexByFullName guarantees idempotency across concurrent
//     REGIDX/DROPIDX.
//  2. DROPIDX <logicalNs> <logicalIdxName>   — argv==3. Convenience
//     form for operators who don't know the fullName format. We try
//     BOTH the mem variant (`_m_:<ns>!_!<idx>`) AND the disk variant
//     (`<ns>!_!<idx>`) sequentially; we only surface an error if
//     NEITHER deletion succeeded beyond "not registered" (i.e. it's
//     fine for a schema that only ever lived on one layer — its
//     sister-layer index will simply say "not registered", which we
//     suppress). The combined `errMem != nil && errDisk != nil`
//     branch prefers identical errors via errors.Is, or falls back to
//     joining "mem: X; disk: Y" when the two differ materially.
func dropIndexCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 || len(cmd.Args) > 3 {
		conn.WriteError("ERR wrong number of arguments for 'dropidx' command: usage: DROPIDX <fullName>  or  DROPIDX <logicalNs> <logicalIdxName>")
		return
	}
	var err error
	if len(cmd.Args) == 2 {
		err = db.dropIndexByFullName(string(cmd.Args[1]))
	} else {
		ns := string(cmd.Args[1])
		logical := string(cmd.Args[2])
		if errV := naming.ValidateDocLogicalNamespace(ns); errV != nil {
			conn.WriteError("ERR DROPIDX " + errV.Error())
			return
		}
		if errV := naming.ValidateLogicalIndexName(logical); errV != nil {
			conn.WriteError("ERR DROPIDX " + errV.Error())
			return
		}
		memNs := naming.BuildStorageNs(ns, true)
		diskNs := naming.BuildStorageNs(ns, false)
		memFull := naming.BuildIdxFullName(memNs, strings.ToLower(logical))
		diskFull := naming.BuildIdxFullName(diskNs, strings.ToLower(logical))
		errMem := db.dropIndexByFullName(memFull)
		errDisk := db.dropIndexByFullName(diskFull)
		if errMem != nil && errDisk != nil {
			if errors.Is(errMem, errDisk) || strings.Contains(errMem.Error(), "not registered") && strings.Contains(errDisk.Error(), "not registered") {
				err = errMem
			} else {
				err = fmt.Errorf("mem: %v; disk: %v", errMem, errDisk)
			}
		} else if errMem != nil && !strings.Contains(errMem.Error(), "not registered") {
			err = errMem
		} else if errDisk != nil && !strings.Contains(errDisk.Error(), "not registered") {
			err = errDisk
		}
	}
	if err != nil {
		conn.WriteError("ERR DROPIDX " + err.Error())
		return
	}
	conn.WriteString("OK")
}
