package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	naming "github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/x"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
)

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

	appAuth, adminAuth, configured := getAuthConfig()
	if configured && providedKey != internalAuthKey {
		role := connPortRole(conn)
		switch role {
		case portRoleApp:
			if adminAuth != "" && providedKey == adminAuth {
				msg := "WRONGPASS invalid password for app port. AUTH with the --auth key, not the --admin-auth key, then retry."
				conn.WriteError(msg)
				slog.Warn("auth rejected: app-port used admin-auth key",
					"remote", conn.RemoteAddr())
				_ = conn.Close()
				return
			}
		case portRoleAdmin:
			if appAuth != "" && providedKey == appAuth {
				msg := "WRONGPASS invalid password for admin port. AUTH with the --admin-auth key, not the --auth key, then retry."
				conn.WriteError(msg)
				slog.Warn("auth rejected: admin-port used app-auth key",
					"remote", conn.RemoteAddr())
				_ = conn.Close()
				return
			}
		}
	}

	if err := acquireAuthConn(db, providedKey); err == nil {
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

func helloCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	const serverVersion = "1.0.0"
	role := connPortRole(conn)
	adminRole := role == portRoleAdmin || role == portRoleUnknown
	storageMode := "hybrid"
	statsNamespaces := 0
	statsIndexes := 0
	if db != nil {
		db.docRegMu.Lock()
		statsNamespaces = len(db.docRegSpec)
		db.docRegMu.Unlock()
		db.idxRegMu.Lock()
		statsIndexes = len(db.idxRegSpec)
		db.idxRegMu.Unlock()
	}
	basicGroup := []any{
		map[string]any{"name": "ping", "usage": "PING — round-trip latency check"},
		map[string]any{"name": "!version", "aliases": []any{"version"}, "usage": "version — print redisx server version"},
		map[string]any{"name": "!clear", "aliases": []any{"clear", "cls"}, "usage": "clear — clear terminal screen"},
		map[string]any{"name": "!help", "aliases": []any{"help", "commands", "?"}, "usage": "commands [cmd]  — list all commands (optionally one command detail)"},
		map[string]any{"name": "quit", "aliases": []any{"exit"}, "usage": "quit — exit shell (Ctrl-C also works)"},
	}
	sharedKv := []any{
		map[string]any{"name": "set", "usage": usageOrProto("SET", "SET key val [EX s|PX ms] [NX]")},
		map[string]any{"name": "setex", "usage": usageOrProto("SETEX", "SETEX key ttl val")},
		map[string]any{"name": "setnx", "usage": usageOrProto("SETNX", "SETNX key val")},
		map[string]any{"name": "get", "usage": usageOrProto("GET", "GET key")},
		map[string]any{"name": "del", "usage": usageOrProto("DEL", "DEL key [key...]")},
		map[string]any{"name": "exists", "usage": usageOrProto("EXISTS", "EXISTS key")},
		map[string]any{"name": "keys", "usage": usageOrProto("KEYS", "KEYS pattern")},
	}
	extended := []any{
		map[string]any{"name": "update", "usage": usageOrProto("UPDATE", "UPDATE key <json_patch>  — merge-UPDATE a JSON value (typed doc enabled: writes validated)")},
		map[string]any{"name": "searchkey", "usage": usageOrProto("SEARCHKEY", "SEARCHKEY <predicate_json>  — typed doc+index search: filter → pick key")},
		map[string]any{"name": "searchindex", "usage": usageOrProto("SEARCHINDEX", "SEARCHINDEX <ns> <idx_name> <query>  — query a named typed index")},
	}
	meta := make([]any, 0, 4)
	{
		// 4 registry pair commands. They are ordinary commands at the
		// dispatcher level; all semantic checks live inside the handlers.
		meta = append(meta, map[string]any{
			"name":     "regsch",
			"role":     "shared_biz",
			"min_args": 1,
			"max_args": 1,
			"usage":    proto.UsageRegisterSchema,
		})
		meta = append(meta, map[string]any{
			"name":     "dropsch",
			"role":     "shared_biz",
			"min_args": 1,
			"max_args": 1,
			"usage":    proto.UsageDropSchema,
		})
		meta = append(meta, map[string]any{
			"name":     "regidx",
			"role":     "shared_biz",
			"min_args": 1,
			"max_args": 1,
			"usage":    proto.UsageRegisterIndex,
		})
		meta = append(meta, map[string]any{
			"name":     "dropidx",
			"role":     "shared_biz",
			"min_args": 1,
			"max_args": 2,
			"usage":    proto.UsageDropIndex,
		})
	}
	out := map[string]any{
		"server":     "redisx",
		"version":    serverVersion,
		"proto":      2,
		"id":         1,
		"mode":       "standalone",
		"role":       "master",
		"admin_role": adminRole,
		"dual_port":  true,
		"features": map[string]any{
			"typed_docs":    true,
			"typed_indexes": true,
			"live_rebuild":  false,
			"write_hooks":   false,
			"search_index":  true,
			"pubsub":        true,
		},
		"stats": map[string]any{
			"namespaces": statsNamespaces,
			"indexes":    statsIndexes,
		},
		"storage": map[string]any{
			"mode": storageMode,
		},
		"commands": map[string]any{
			"group_order": []any{"Basic", "Extended", "Meta Management"},
			"groups": map[string]any{
				"Basic":           append(basicGroup, sharedKv...),
				"Extended":        extended,
				"Meta Management": meta,
			},
			"meta_admin_only": true,
		},
	}
	conn.WriteAny(out)
}

func clientCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteString("OK")
}

func pingCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteString("PONG")
}

func quitCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteString("OK")
	_ = conn.Close()
}

func usageOrProto(_ string, fallback string) string {
	// Registry commands are ordinary commands; usage strings live inline at
	// each handler and in the proto.Usage* constants that feed the handshake
	// meta group. This helper stays as a pass-through so legacy basic KV
	// command argc-error call sites keep working unchanged.
	return fallback
}

func appearsDocIntentJSON(v string) bool {
	if !gjson.Valid(v) {
		return false
	}
	r := gjson.Parse(v)
	return r.IsObject() || r.IsArray()
}

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
				conn.WriteError(fmt.Sprintf("ERR GET doc-pattern requires <ns> <pk-suffix>; namespace %q alone is not a query (use SEARCHKEY or KEYS on admin port)", arg1))
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

	if !naming.HasMemPrefix(keyPattern) && !naming.IsInternalStorageNs(keyPattern) {
		if strings.Contains(keyPattern, naming.StorageKeySeparator()) {
		} else if strings.ContainsAny(keyPattern, "*?[") {
			conn.WriteError("ERR KEYS for doc-path requires bare <storageNs> without wildcards or colons. Glob patterns like 'abc*' on a naked namespace fragment are forbidden; patterns containing ':' (like 'app:prefix*') are treated as raw KV-path and allowed freely. Use SEARCHKEY <ns> <keyrange_json> for typed-document scoped queries.")
			return
		} else {
			if !naming.IsInternalStorageNs(keyPattern) {
				if _, ok := db.lookupDocByStorageNs(keyPattern); !ok {
					conn.WriteError(fmt.Sprintf("ERR KEYS namespace %q is not registered — doc-path KEYS requires REGSCH first. For raw KV enumeration, pass a pattern containing ':' (e.g. '%s:*' or '%s:prefix*').", keyPattern, keyPattern, keyPattern))
					return
				}
			}
			keyPattern = naming.BuildStorageKey(keyPattern, "*")
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

func publishCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
}

func subscribeCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	for i := 1; i < len(cmd.Args); i++ {
		ps.Subscribe(conn, string(cmd.Args[i]))
	}
}

func pSubscribeCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	for i := 1; i < len(cmd.Args); i++ {
		ps.Psubscribe(conn, string(cmd.Args[i]))
	}
}

// X Commands

// parseFilter recursively parses a MongoDB-style JSON string into an x.Filter.
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
				ownerMem := naming.HasMemPrefix(on)
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

func mustMarshal(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "[]"
	}
	return string(b)
}

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
