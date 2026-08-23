package server

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/internal/xcmd"
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
	meta := make([]any, 0, len(proto.Registry))
	{
		order := []string{
			proto.CmdRegisterDoc, proto.CmdListDocs, proto.CmdDescribeDoc,
			proto.CmdRegisterIndex, proto.CmdListIndexes, proto.CmdDropIndex,
		}
		for _, name := range order {
			spec, ok := proto.Registry[strings.ToLower(name)]
			if ok {
				meta = append(meta, map[string]any{
					"name":     strings.ToLower(spec.CmdWord),
					"role":     "admin_only",
					"min_args": spec.Argc.MinArgs,
					"max_args": spec.Argc.MaxArgs,
					"usage":    spec.Usage,
				})
			}
		}
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

func usageOrProto(cmdWord, fallback string) string {
	spec, ok := proto.Registry[strings.ToLower(cmdWord)]
	if ok && spec.Usage != "" {
		return spec.Usage
	}
	return fallback
}

func setCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

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

	if nx {
		set, err := db.SetNXWithTtl(string(cmd.Args[1]), string(cmd.Args[2]), ttl)
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

	if err := db.SetWithTtl(string(cmd.Args[1]), string(cmd.Args[2]), ttl); err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
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
	if err := db.SetWithTtl(string(cmd.Args[1]), string(cmd.Args[3]), ttl); err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
}

func setNxCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	set, err := db.SetNX(string(cmd.Args[1]), string(cmd.Args[2]))
	if err != nil {
		conn.WriteError("ERR " + err.Error())
	} else if set {
		conn.WriteInt(1)
	} else {
		conn.WriteInt(0)
	}
}

func getCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	res := db.Get(string(cmd.Args[1]))
	if res.IsError() {
		if errors.Is(res.Error(), buntdb.ErrNotFound) {
			conn.WriteNull()
		} else {
			conn.WriteError("ERR " + res.Error().Error())
		}
	} else {
		conn.WriteBulk([]byte(res.MustGet()))
	}
}

func delCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var deleted bool
	deleted, err := db.Delete(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError("ERR " + err.Error())
	} else if deleted {
		conn.WriteInt(1)
	} else {
		conn.WriteInt(0)
	}
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
	if strings.HasPrefix(keyPattern, "_") && !strings.HasPrefix(keyPattern, x.MemNsPrefix) {
		conn.WriteError("ERR forbidden key pattern")
		return
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
	// Strict RESP positional argc shape (fr-searchindex.md §2):
	// shifted +1 from SEARCHKEY argc table because the first positional arg
	// is indexName. LIMIT count is TWO tokens (the keyword + the numeric
	// value), so the 4th argc shape covers "LIMIT count" (2 trailing tokens).
	//
	//   argc after cmd word | shape
	//   -------------------+---------------------------------------------------
	//   3                  |  idx_name  kr_json  filter_json                  (ASC, no LIMIT)
	//   4                  |  idx_name  kr_json  filter_json  ASC|DESC        (dir only)
	//   5                  |  idx_name  kr_json  filter_json  LIMIT count     (count only, ASC default)
	//   6                  |  idx_name  kr_json  filter_json  ASC|DESC LIMIT count
	//   2 / 7+             |  WRONG ARGS
	nArgs := len(cmd.Args) - 1
	if nArgs < 3 || nArgs > 6 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	indexName := string(cmd.Args[1])
	krJSON := string(cmd.Args[2])
	filterJSON := string(cmd.Args[3])
	// Zero-legacy: Arg#2 must be JSON object (not a legacy glob string).
	if len(krJSON) == 0 || krJSON[0] != '{' {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	kr, krErr := x.UnmarshalKeyRange([]byte(krJSON))
	if krErr != nil {
		conn.WriteError("ERR invalid key range: " + krErr.Error())
		return
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
	// Strict RESP positional argc shape (fr-update-keyrange.mdx §2):
	//
	//   argc after cmd word | shape
	//   -------------------+-----------------------------------------------
	//   3                  |  <kr_json> <filter_json> <update_json>             (no LIMIT)
	//   5                  |  <kr_json> <filter_json> <update_json> LIMIT count  (count only, ASC default)
	//   1 / 2 / 4 / 6+    |  WRONG ARGS
	nArgs := len(cmd.Args) - 1
	if nArgs != 3 && nArgs != 5 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	krJSON := string(cmd.Args[1])
	filterJSON := string(cmd.Args[2])
	updateJSON := string(cmd.Args[3])
	// Zero-legacy: Arg#1 must be a JSON object (not a legacy glob string).
	if len(krJSON) == 0 || krJSON[0] != '{' {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	kr, krErr := x.UnmarshalKeyRange([]byte(krJSON))
	if krErr != nil {
		conn.WriteError("ERR invalid key range: " + krErr.Error())
		return
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

	pairs, err := xcmd.ParseUpdate(updateJSON)
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
	// Strict RESP positional argc shape (fr-searchkeyrange.md §2):
	//
	//   argc after cmd word | shape
	//   -------------------+------------------------------------------------
	//   2                  |  <kr_json> <filter_json>                 (ASC,  no LIMIT)
	//   3                  |  <kr_json> <filter_json> ASC|DESC        (dir only, no LIMIT)
	//   4                  |  <kr_json> <filter_json> LIMIT count     (count only, ASC default)
	//   5                  |  <kr_json> <filter_json> ASC|DESC LIMIT count
	//   1 / 6+             |  WRONG ARGS
	nArgs := len(cmd.Args) - 1
	if nArgs < 2 || nArgs > 5 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	krJSON := string(cmd.Args[1])
	filterJSON := string(cmd.Args[2])
	// Zero-legacy: Arg#1 must be a JSON object (not a legacy glob string).
	if len(krJSON) == 0 || krJSON[0] != '{' {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	kr, krErr := x.UnmarshalKeyRange([]byte(krJSON))
	if krErr != nil {
		conn.WriteError("ERR invalid key range: " + krErr.Error())
		return
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
		// wire count > 0 assert first → kr.Limit(count) never panics.
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

const adminSkeletonFmt = "ERR %s is not implemented yet — schema registry (D5) and admin command wiring still pending"

func regDocCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "regdoc"))
}

func lsDocCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "lsdoc"))
}

func desDocCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "desdoc"))
}

func lsIdxCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "lsidx"))
}

func regIdxCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "regidx"))
}

func delIdxCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "delidx"))
}
