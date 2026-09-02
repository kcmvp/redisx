package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
)

// ——— Registry management commands ———

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

// regSchemaCommand implements REGSCH (register typed-document schema).
//
// JSON payload must match the public x.Schema fields; unknown fields
// are rejected (via json.Decoder.DisallowUnknownFields) to avoid silent
// typos.
//
// Semantic validation (namespace shape via ValidateDocLogicalNamespace,
// non-empty key-attr entries) lives inside db.writeDocSpec and is the
// SSoT for schema checks; regSchemaCommand only does JSON-shape +
// unknown-field guard before delegating to writeDocSpec.
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
	var spec docSpec
	dec := json.NewDecoder(strings.NewReader(rawJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		conn.WriteError("ERR REGSCH schema: " + err.Error())
		return
	}
	if spec.TTL <= 0 {
		spec.TTL = -1
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
		conn.WriteError("ERR wrong number of arguments for 'regidx' command: usage: " + "REGIDX <json-spec>")
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
//     forwarded straight to db.dropIndexByFullName. Layer choice is
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
