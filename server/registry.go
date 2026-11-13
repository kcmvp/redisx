package server

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"

	"github.com/kcmvp/redisx/x"
)

// ——— Type definitions ———

// docSpec stores a single registered typed-document schema (the server-
// internal enriched version of x.Schema). It always contains exactly one live
// copy per storageNs (the "no multi-version" invariant). The three internal
// fields Version/CreatedAt/UpdatedAt are excluded from the canonical MD5
// fingerprint, so metadata-only drifts do not invalidate version identity.
type docSpec struct {
	Namespace string        `json:"namespace"`
	Mem       bool          `json:"mem"`
	KeyAttrs  []string      `json:"key_attrs"`
	TTL       time.Duration `json:"ttl_ns"`

	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// StorageNs returns the actual storage-layer namespace used for meta + data
// keys: it is either "<ns>" (disk) or "_m_:<ns>" (memory). See Rule1 in
// checker.go / naming package for the full Spec-vs-MetaKey-vs-x.Schema map.
func (p docSpec) storageNs() string {
	return naming.BuildStorageNs(p.Namespace, p.Mem)
}

// asSchema returns an x.Schema view over docSpec so that the SSoT
// x.Validate function can be invoked without requiring docSpec to
// directly implement x.Schema (which is blocked because field names
// clash with Go method names in Go).
func (p docSpec) asSchema() x.Schema {
	return schemaAdapter{p: p}
}

type schemaAdapter struct{ p docSpec }

func (a schemaAdapter) Namespace() string  { return a.p.Namespace }
func (a schemaAdapter) Mem() bool          { return a.p.Mem }
func (a schemaAdapter) KeyAttrs() []string { return append([]string(nil), a.p.KeyAttrs...) }
func (a schemaAdapter) TTL() time.Duration { return a.p.TTL }

var _ x.Schema = schemaAdapter{}

// idxSpec is the in-process (memory-only) description of a registered btree
// index. Indexes are NEVER persisted: they are declared in code via
// x.RegisterSchema(x.IndexOn(...)) and rebuilt from scratch on every boot, so
// this struct exists purely to answer SearchIndex's owner-layer lookup.
type idxSpec struct {
	OwnerNs string
	Logical string
}

// ——— Meta helpers: fingerprint + meta-key load/delete (no multi-version) ———

// md5VersionHex computes a 12-char truncated MD5 over canonical JSON-encoded v.
// Callers always pass an anonymous struct that intentionally excludes Version/
// CreatedAt/UpdatedAt, so fingerprints are semantic-only and stable over time.
func md5VersionHex(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("md5VersionHex: marshal: %w", err)
	}
	sum := md5.Sum(raw)
	return hex.EncodeToString(sum[:])[:12], nil
}

// canonicalDocMD5 returns the canonical MD5 for a docSpec. Only the four
// user-visible x.Schema fields are hashed (Namespace/Mem/KeyAttrs/TTL); the
// three server-internal fields (Version/CreatedAt/UpdatedAt) are deliberately
// excluded.
func canonicalDocMD5(spec docSpec) (string, error) {
	if spec.TTL <= 0 {
		spec.TTL = -1
	}
	if len(spec.KeyAttrs) == 0 {
		spec.KeyAttrs = []string{}
	}
	return md5VersionHex(struct {
		Namespace string        `json:"namespace"`
		Mem       bool          `json:"mem"`
		KeyAttrs  []string      `json:"key_attrs"`
		TTL       time.Duration `json:"ttl_ns"`
	}{
		Namespace: spec.Namespace,
		Mem:       spec.Mem,
		KeyAttrs:  append([]string(nil), spec.KeyAttrs...),
		TTL:       spec.TTL,
	})
}

// indexJSONComposite builds a buntdb-index comparator over one or more JSON
// paths. Single path delegates to buntdb.IndexJSON; multiple paths produce a
// lexicographic multi-field comparator that evaluates each gjson path in order
// and returns the first non-equal result.
func indexJSONComposite(paths ...string) func(a, b string) bool {
	if len(paths) == 0 {
		panic("indexJSONComposite: at least one path is required")
	}
	if len(paths) == 1 {
		return buntdb.IndexJSON(paths[0])
	}
	return func(a, b string) bool {
		for _, p := range paths {
			ra := gjson.Get(a, p)
			rb := gjson.Get(b, p)
			if ra.Less(rb, false) {
				return true
			}
			if rb.Less(ra, false) {
				return false
			}
		}
		return false
	}
}

// ——— Meta key read helpers ———

// loadDocSpec reads the single persisted doc spec key for a storageNs.
// exists=false means the namespace is not registered (registry fail-closed
// invariant for typed-doc paths). writeDocSpec is the only writer; this is
// the single read-path for the "exactly one live version" invariant.
func (db *DB) loadDocSpec(storageNs string) (spec docSpec, exists bool, err error) {
	if storageNs == "" {
		return spec, false, errors.New("loadDocSpec: storageNs is required")
	}
	layer, constrained, lerr := resolveLayer(storageNs)
	if lerr != nil || !constrained {
		return spec, false, fmt.Errorf("loadDocSpec: storageNs %q is not a concrete ns (err=%v constrained=%v)", storageNs, lerr, constrained)
	}
	glob := naming.DocMetaGlobFor(storageNs)
	_ = db.store(layer).View(func(tx *buntdb.Tx) error {
		_ = tx.AscendKeys(glob, func(k, raw string) bool {
			if e := json.Unmarshal([]byte(raw), &spec); e != nil {
				err = fmt.Errorf("loadDocSpec: %s unmarshal: %w", k, e)
				return false
			}
			exists = true
			return false
		})
		return nil
	})
	return spec, exists, err
}

// deleteMetaKeys deletes all persisted meta keys that match a glob inside a
// running BuntDB tx. The "what" label is used for error messages (callers pass
// e.g. "deleteDocSpecMeta" so the origin of a delete failure is clear).
// Missing keys (ErrNotFound) are ignored because the
// invariant of "exactly one live version" means zero matches just means
// nothing to delete.
//
// Do NOT call Write operations inside AscendKeys callbacks (BuntDB iter-then-
// write rule); doomed is collected first, then deleted in a second pass.
func deleteMetaKeys(tx *buntdb.Tx, glob string, what string) error {
	var doomed []string
	_ = tx.AscendKeys(glob, func(k, _ string) bool {
		doomed = append(doomed, k)
		return true
	})
	for _, k := range doomed {
		if _, e := tx.Delete(k); e != nil && !errors.Is(e, buntdb.ErrNotFound) {
			return fmt.Errorf("%s: %s: %w", what, k, e)
		}
	}
	return nil
}

// deleteDocSpecMeta deletes the single persisted doc meta key for storageNs
// inside a running BuntDB tx. Exactly one live key per storageNs is the
// invariant; if none exist the call is a no-op (the caller then decides
// whether this is "not registered" via a pre-probe).
func deleteDocSpecMeta(tx *buntdb.Tx, storageNs string) error {
	return deleteMetaKeys(tx, naming.DocMetaGlobFor(storageNs), "deleteDocSpecMeta")
}

// ——— Core writers: writeDocSpec ———

// writeDocSpec is the single entry point for doc-spec writes (x.RegisterSchema
// declarations passed to Start, plus runtime client.RegisterSchema pushes).
// Input spec carries only the
// four user-visible x.Schema fields; Version/CreatedAt/UpdatedAt are computed
// & stamped here internally.
//
// Invariants enforced:
//   - MD5 unchanged → noop.
//   - MD5 changed → atomic BuntDB Update(): deleteDocSpecMeta then Set the
//     new v_<md5> meta key (so the "exactly one live version" invariant is
//     kept within a single tx).
//   - Only after the Update succeeds do we overwrite docRegSpec memory map
//     under docRegMu (no partial state on disk-write failure).
//   - CreatedAt preserved across upgrades; UpdatedAt bumped only on change.
func (db *DB) writeDocSpec(spec docSpec) error {
	if spec.TTL <= 0 {
		spec.TTL = -1
	}
	if err := x.Validate(spec.asSchema()); err != nil {
		return fmt.Errorf("writeDocSpec: %w", err)
	}
	storageNs := spec.storageNs()
	versionHex, err := canonicalDocMD5(spec)
	if err != nil {
		return fmt.Errorf("writeDocSpec %q: md5: %w", storageNs, err)
	}
	oldSpec, hadOld, err := db.loadDocSpec(storageNs)
	if err != nil {
		return err
	}
	if hadOld && oldSpec.Version == versionHex {
		slog.Debug("writeDocSpec: unchanged fingerprint, skipping",
			"ns", spec.Namespace, "storage_ns", storageNs, "version", versionHex)
		return nil
	}

	now := time.Now().UTC()
	if hadOld {
		spec.CreatedAt = oldSpec.CreatedAt
		spec.UpdatedAt = now
	} else {
		spec.CreatedAt = now
		spec.UpdatedAt = time.Time{}
	}
	spec.Version = versionHex

	raw, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("writeDocSpec %q: marshal: %w", storageNs, err)
	}
	layer, constrained, lerr := resolveLayer(storageNs)
	if lerr != nil || !constrained {
		return fmt.Errorf("writeDocSpec %q: storage_ns is not a concrete ns (err=%v constrained=%v)", storageNs, lerr, constrained)
	}
	targetKey := naming.DocMetaKey(storageNs, versionHex)

	if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		if err := deleteDocSpecMeta(tx, storageNs); err != nil {
			return err
		}
		_, _, setErr := tx.Set(targetKey, string(raw), nil)
		return setErr
	}); err != nil {
		return fmt.Errorf("writeDocSpec %q: persist %s: %w", storageNs, targetKey, err)
	}

	db.docRegMu.Lock()
	db.docRegSpec[storageNs] = spec
	db.docRegMu.Unlock()

	if hadOld {
		slog.Info("writeDocSpec: spec upgraded (new fingerprint)",
			"ns", spec.Namespace, "storage_ns", storageNs,
			"old_version", oldSpec.Version, "new_version", versionHex,
			"created_at", spec.CreatedAt.Format(time.RFC3339Nano),
			"updated_at", spec.UpdatedAt.Format(time.RFC3339Nano))
	} else {
		slog.Info("writeDocSpec: new spec persisted",
			"ns", spec.Namespace, "storage_ns", storageNs,
			"version", versionHex,
			"created_at", spec.CreatedAt.Format(time.RFC3339Nano))
	}
	return nil
}

// ——— Index registration (memory-only) ———

// registerIndexes mounts one or more x.Index values as in-process btree
// indexes. Indexes are NEVER persisted: they are declared in code via
// x.RegisterSchema(x.IndexOn(...)) and rebuilt from scratch on every boot, so
// there is no meta key, no MD5 fingerprint and no reload path. Called by: boot
// declared-index mounting, the runtime SCHEMA command
// (registerIndexIfAbsent) and the CreateIndex alias.
//
// Duplicate declarations are rejected: buntdb's CreateIndex fails when an
// index with the same name already exists, so re-declaring an index in code
// is reported as an error.
func (db *DB) registerIndexes(indexes ...x.Index) error {
	if db == nil {
		return errors.New("registerIndexes: db is nil")
	}
	for _, idx := range indexes {
		fullName := idx.Name()
		if fullName == "" {
			return errors.New("registerIndexes: index name is required")
		}
		ownerNs, logical, err := naming.ParseIdxFullName(fullName)
		if err != nil {
			return fmt.Errorf("registerIndexes %q: %w", fullName, err)
		}
		keyPattern := idx.KeyPattern()
		layer, constrained, lerr := resolveLayer(keyPattern)
		if lerr != nil || !constrained {
			return fmt.Errorf("registerIndexes %q: key_pattern must be a concrete (non-wildcard) ns (err=%v constrained=%v)", fullName, lerr, constrained)
		}
		paths := idx.Paths()
		if len(paths) == 0 {
			return fmt.Errorf("registerIndexes %q: at least one json path is required", fullName)
		}
		if err := db.store(layer).CreateIndex(fullName, keyPattern, indexJSONComposite(paths...)); err != nil {
			return fmt.Errorf("registerIndexes %q: create btree: %w", fullName, err)
		}
		db.idxRegMu.Lock()
		db.idxRegSpec[fullName] = idxSpec{OwnerNs: ownerNs, Logical: logical}
		db.idxRegMu.Unlock()
		slog.Debug("index registered",
			"name", fullName, "owner_ns", ownerNs, "key_pattern", keyPattern, "paths", paths)
	}
	return nil
}

// registerDeclaredIndexes mounts every index declared via
// x.RegisterSchema(x.IndexOn(...)) on the given schemas. Indexes are
// code-driven and never persisted, so this runs once per boot right after
// registerSchemas; schemas that declare no indexes are simply skipped.
func (db *DB) registerDeclaredIndexes(schemas ...x.Schema) error {
	var indexes []x.Index
	for _, sch := range schemas {
		if def, ok := sch.(interface{ Indexes() []x.Index }); ok {
			indexes = append(indexes, def.Indexes()...)
		}
	}
	if len(indexes) == 0 {
		return nil
	}
	return db.registerIndexes(indexes...)
}

// registerSchemas converts x.Schema values into docSpec and delegates to
// writeDocSpec (the single persistence entry point). Used by: boot
// x.RegisterSchema declarations. x.Schema only provides the 4 user-visible
// fields; the 3 internal fields (Version/CreatedAt/UpdatedAt) are stamped
// inside writeDocSpec.
//
// The persisted doc meta record is an AUDIT trail only — it is never replayed
// into the in-memory registry on boot (schemas come exclusively from code, so
// a namespace the current binary no longer declares is intentionally absent).
func (db *DB) registerSchemas(schemas ...x.Schema) error {
	for _, sch := range schemas {
		spec := docSpec{
			Namespace: sch.Namespace(),
			Mem:       sch.Mem(),
			KeyAttrs:  append([]string(nil), sch.KeyAttrs()...),
			TTL:       sch.TTL(),
		}
		if err := db.writeDocSpec(spec); err != nil {
			return err
		}
	}
	return nil
}
