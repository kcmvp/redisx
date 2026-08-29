package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
)

// ——— SET-family commands ———

// execDocPathSet is the shared doc-path handler for SET / SETEX / SETNX.
// It eliminates the ~80-line doc-path duplication that previously appeared
// in each of the three command handlers.
//
// cmdName is used only for error-message prefixes (e.g. "SET", "SETEX", "SETNX").
// useSchemaTTL controls whether a zero callerTTL falls back to the registered
// schema's TTL (true for SET/SETNX, false for SETEX where TTL is always explicit).
func execDocPathSet(conn redcon.Conn, db *DB, arg1, val string, callerTTL time.Duration, nx bool, cmdName string, useSchemaTTL bool) {
	ns := arg1
	doc, ok := db.lookupDocByLogicalOrStorageNs(ns)
	if !ok {
		if appearsDocIntentJSON(val) {
			conn.WriteError(fmt.Sprintf("ERR %s namespace %q not registered — doc-pattern requires REGSCH first", cmdName, ns))
		} else {
			conn.WriteError(fmt.Sprintf("ERR %s kv-pattern key must contain namespace separator %q", cmdName, naming.StorageKeySeparator()))
		}
		return
	}
	if !gjson.Valid(val) {
		conn.WriteError(fmt.Sprintf("ERR %s doc-pattern: value must be valid JSON object or array of objects", cmdName))
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
			conn.WriteError(fmt.Sprintf("ERR %s doc-pattern: array must contain JSON objects", cmdName))
			return
		}
	} else if root.IsObject() {
		objects = []string{root.Raw}
	} else {
		conn.WriteError(fmt.Sprintf("ERR %s doc-pattern: value must be JSON object or array of objects", cmdName))
		return
	}
	batch := make([]batchedWrite, 0, len(objects))
	for i, obj := range objects {
		dk, err := deriveDocKey(doc.Spec, doc.StorageNs, obj)
		if err != nil {
			conn.WriteError(fmt.Errorf("ERR %s doc-pattern: object[%d]: %w", cmdName, i, err).Error())
			return
		}
		finalTTL := callerTTL
		if useSchemaTTL && finalTTL == 0 {
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
			if cmdName == "SETNX" {
				conn.WriteInt(0)
			} else {
				conn.WriteNull()
			}
			return
		}
		conn.WriteError(fmt.Sprintf("ERR %s %s", cmdName, err.Error()))
		return
	}
	if nx {
		if cmdName == "SETNX" {
			if n > 0 {
				conn.WriteInt(1)
			} else {
				conn.WriteInt(0)
			}
		} else {
			if n == 0 {
				conn.WriteNull()
			} else {
				conn.WriteString("OK")
			}
		}
		return
	}
	conn.WriteString("OK")
}

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
		execDocPathSet(conn, db, arg1, valueRaw, ttl, nx, "SET", true)
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
		execDocPathSet(conn, db, arg1, val, ttl, false, "SETEX", false)
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
		execDocPathSet(conn, db, arg1, val, 0, true, "SETNX", true)
		return
	}
}
