package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/kcmvp/redisx/x"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

// ——— Read / query commands ———

// parseOrderAndLimit extracts optional ASC/DESC and LIMIT from trailing
// command arguments. It eliminates the duplicated LIMIT/ASC/DESC argc
// parsing that previously appeared in searchIndexCommand, searchKeyCommand,
// and updateCommand.
//
// hasOrder controls whether an ASC/DESC directive is expected:
//   - true  → SEARCHINDEX / SEARCHKEY (support both order + limit)
//   - false → UPDATE (limit only, no order)
//
// Returns the (possibly limited) KeyRange, the sort direction, and whether
// the caller should return immediately (an error was already written to conn).
func parseOrderAndLimit(
	conn redcon.Conn,
	cmd redcon.Command,
	kr x.KeyRange,
	optStartIdx int,
	hasOrder bool,
) (x.KeyRange, bool, bool) {
	nArgs := len(cmd.Args) - 1
	desc := false

	if hasOrder {
		// optionalCount = number of trailing optional arg slots.
		// nArgs = len(cmd.Args)-1; optStartIdx = index of first optional arg.
		// Optional slots = nArgs - optStartIdx + 1.
		// SEARCHKEY (optStartIdx=3): nArgs=2→0, nArgs=3→1, nArgs=4→2, nArgs=5→3.
		// SEARCHINDEX (optStartIdx=4): nArgs=3→0, nArgs=4→1, nArgs=5→2, nArgs=6→3.
		optionalCount := nArgs - optStartIdx + 1
		switch {
		case optionalCount <= 0:
			// No optional args
		case optionalCount == 1:
			a := strings.ToUpper(string(cmd.Args[optStartIdx]))
			if a != "ASC" && a != "DESC" {
				conn.WriteError("ERR invalid order: " + string(cmd.Args[optStartIdx]))
				return kr, false, true
			}
			desc = a == "DESC"
		case optionalCount == 2:
			// LIMIT-only form (no order directive)
			aFirst := strings.ToUpper(string(cmd.Args[optStartIdx]))
			if aFirst != "LIMIT" {
				conn.WriteError("ERR invalid argument: " + string(cmd.Args[optStartIdx]))
				return kr, false, true
			}
			countStr := string(cmd.Args[optStartIdx+1])
			count, err := strconv.Atoi(countStr)
			if err != nil || count <= 0 {
				conn.WriteError("ERR invalid count for LIMIT: " + countStr)
				return kr, false, true
			}
			kr = kr.Limit(count)
		case optionalCount == 3:
			// ASC/DESC + LIMIT + count
			aOrd := strings.ToUpper(string(cmd.Args[optStartIdx]))
			if aOrd != "ASC" && aOrd != "DESC" {
				conn.WriteError("ERR invalid order: " + string(cmd.Args[optStartIdx]))
				return kr, false, true
			}
			desc = aOrd == "DESC"
			aLim := strings.ToUpper(string(cmd.Args[optStartIdx+1]))
			if aLim != "LIMIT" {
				conn.WriteError("ERR invalid argument: " + string(cmd.Args[optStartIdx+1]))
				return kr, false, true
			}
			countStr := string(cmd.Args[optStartIdx+2])
			count, err := strconv.Atoi(countStr)
			if err != nil || count <= 0 {
				conn.WriteError("ERR invalid count for LIMIT: " + countStr)
				return kr, false, true
			}
			kr = kr.Limit(count)
		default:
			// optionalCount > 3: too many optional args
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return kr, false, true
		}
	} else {
		// No order, only optional LIMIT
		optionalCount := nArgs - optStartIdx + 1
		if optionalCount == 2 {
			a := strings.ToUpper(string(cmd.Args[optStartIdx]))
			if a != "LIMIT" {
				conn.WriteError("ERR invalid argument: " + string(cmd.Args[optStartIdx]))
				return kr, false, true
			}
			countStr := string(cmd.Args[optStartIdx+1])
			count, err := strconv.Atoi(countStr)
			if err != nil || count <= 0 {
				conn.WriteError("ERR invalid count for LIMIT: " + countStr)
				return kr, false, true
			}
			kr = kr.Limit(count)
		}
	}

	return kr, desc, false
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
//   - `_doc_:` / `_idx_:` meta namespaces are READ-enumerable so
//     operators can list registered doc/idx specs and GET their
//     definitions (writes to them stay rejected via
//     validateKVMutationKey). `_auth_:` (credentials) and any other
//     underscore-prefixed shape remain forbidden.
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
	// Registry introspection: bare `_doc_` / `_idx_` expand to their
	// `:*` glob; explicit `_doc_:` / `_idx_:` prefixed patterns pass.
	// `_auth_:` and unknown underscore shapes stay forbidden.
	switch {
	case keyPattern == naming.DocMetaNsPrefix():
		keyPattern = naming.DocMetaNsPrefix() + ":*"
	case keyPattern == naming.IdxMetaNsPrefix():
		keyPattern = naming.IdxMetaNsPrefix() + ":*"
	default:
		if naming.HasUnderscorePrefix(keyPattern) &&
			!strings.HasPrefix(keyPattern, naming.DocMetaNsPrefix()+":") &&
			!strings.HasPrefix(keyPattern, naming.IdxMetaNsPrefix()+":") {
			conn.WriteError("ERR forbidden key pattern")
			return
		}
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

	kr, desc, earlyReturn := parseOrderAndLimit(conn, cmd, kr, 4, true)
	if earlyReturn {
		return
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

	kr, _, earlyReturn := parseOrderAndLimit(conn, cmd, kr, 4, false)
	if earlyReturn {
		return
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

	kr, desc, earlyReturn := parseOrderAndLimit(conn, cmd, kr, 3, true)
	if earlyReturn {
		return
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
