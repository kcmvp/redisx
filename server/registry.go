package server

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/samber/lo"
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

// idxSpec stores a single registered secondary index (server-internal version
// of x.Index). Same "exactly one live copy per fullName" invariant as docSpec.
// Composite indexes are buntdb-native JSON-path comparators (see
// indexJSONComposite) that are re-created whenever the canonical fingerprint
// changes.
//
// UnmarshalJSON accepts both "path" (single) and "paths" (array) keys for
// backwards-compatibility with legacy JSON payloads; "path" is rewritten into
// a single-element "paths" array.
type idxSpec struct {
	OwnerNs    string   `json:"owner_ns"`
	Logical    string   `json:"logical"`
	KeyPattern string   `json:"key_pattern"`
	Paths      []string `json:"paths"`

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

// FullName returns the canonical composite key used for idx meta-keys and the
// buntdb native index handle: "<ownerNs>!_!<logical>".
func (i idxSpec) fullName() string { return naming.BuildIdxFullName(i.OwnerNs, i.Logical) }

func (i *idxSpec) UnmarshalJSON(b []byte) error {
	type rawIdx idxSpec
	aux := &struct {
		Path string `json:"path"`
		*rawIdx
	}{
		rawIdx: (*rawIdx)(i),
	}
	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}
	if len(aux.Paths) == 0 && aux.Path != "" {
		aux.Paths = []string{aux.Path}
	}
	return nil
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

// canonicalIdxMD5 mirrors canonicalDocMD5 for idxSpec. Dots inside JSON paths
// are collapsed to underscores before hashing so buntdb comparator key-paths
// are always stable regardless of input spelling.
func canonicalIdxMD5(spec idxSpec) (string, error) {
	paths := lo.Map(spec.Paths, func(p string, _ int) string {
		return strings.ReplaceAll(p, ".", "_")
	})
	if len(paths) == 0 {
		paths = []string{}
	}
	return md5VersionHex(struct {
		OwnerNs    string   `json:"owner_ns"`
		Logical    string   `json:"logical"`
		KeyPattern string   `json:"key_pattern"`
		Paths      []string `json:"paths"`
	}{
		OwnerNs:    spec.OwnerNs,
		Logical:    spec.Logical,
		KeyPattern: spec.KeyPattern,
		Paths:      paths,
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
// invariant for typed-doc paths). Boot (loadDocSpecs) and writeDocSpec are
// the only writers; this is the single read-path for the "exactly one live
// version" invariant.
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

// loadIdxSpec mirrors loadDocSpec for an index fullName.
func (db *DB) loadIdxSpec(idxFullName string) (spec idxSpec, exists bool, err error) {
	if idxFullName == "" {
		return spec, false, errors.New("loadIdxSpec: idxFullName is required")
	}
	ownerNs, _, perr := naming.ParseIdxFullName(idxFullName)
	if perr != nil {
		return spec, false, fmt.Errorf("loadIdxSpec: %w", perr)
	}
	layer, constrained, lerr := resolveLayer(ownerNs)
	if lerr != nil || !constrained {
		return spec, false, fmt.Errorf("loadIdxSpec: ownerNs %q is not a concrete ns (err=%v constrained=%v)", ownerNs, lerr, constrained)
	}
	glob := naming.IdxMetaGlobFor(idxFullName)
	_ = db.store(layer).View(func(tx *buntdb.Tx) error {
		_ = tx.AscendKeys(glob, func(k, raw string) bool {
			if e := json.Unmarshal([]byte(raw), &spec); e != nil {
				err = fmt.Errorf("loadIdxSpec: %s unmarshal: %w", k, e)
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
// e.g. "deleteDocSpecMeta" / "deleteIdxSpecMeta" so the origin of a delete
// failure is clear). Missing keys (ErrNotFound) are ignored because the
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

// deleteIdxSpecMeta mirrors deleteDocSpecMeta for an index fullName.
func deleteIdxSpecMeta(tx *buntdb.Tx, idxFullName string) error {
	return deleteMetaKeys(tx, naming.IdxMetaGlobFor(idxFullName), "deleteIdxSpecMeta")
}

// ——— Core writers: writeDocSpec / writeIndexSpec ———

// writeDocSpec is the single entry point for doc-spec writes (REGSCH command,
// boot registerSchemas, ctrl seed fixtures). Input spec carries only the
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

// writeIndexSpec mirrors writeDocSpec for indexes. Extra behaviour vs
// writeDocSpec:
//   - On fingerprint change (paths/order/key_pattern drift) the buntdb native
//     btree index is dropped then re-created before the meta-key is written;
//     the query comparator is thus always in sync with the stored meta-key.
//   - CreateIndex happens OUTSIDE idxRegMu (btree operations are potentially
//     expensive). The lock is held only for the final idxRegSpec map write.
func (db *DB) writeIndexSpec(spec idxSpec) error {
	if spec.OwnerNs == "" {
		return errors.New("writeIndexSpec: owner_ns is required")
	}
	if spec.Logical == "" {
		return errors.New("writeIndexSpec: logical is required")
	}
	fullName := spec.fullName()
	if spec.KeyPattern == "" {
		return fmt.Errorf("writeIndexSpec %q: key_pattern is required", fullName)
	}
	if len(spec.Paths) == 0 {
		return fmt.Errorf("writeIndexSpec %q: at least one path is required", fullName)
	}
	for i, p := range spec.Paths {
		if p == "" {
			return fmt.Errorf("writeIndexSpec %q: paths[%d] is empty", fullName, i)
		}
	}
	patternLayer, constrained, err := resolveLayer(spec.KeyPattern)
	if err != nil {
		return fmt.Errorf("writeIndexSpec %q: %w", fullName, err)
	}
	if !constrained {
		return fmt.Errorf("writeIndexSpec %q: key_pattern cannot start with wildcard", fullName)
	}

	versionHex, err := canonicalIdxMD5(spec)
	if err != nil {
		return fmt.Errorf("writeIndexSpec %q: md5: %w", fullName, err)
	}
	oldSpec, hadOld, err := db.loadIdxSpec(fullName)
	if err != nil {
		return err
	}
	if hadOld && oldSpec.Version == versionHex {
		slog.Debug("writeIndexSpec: unchanged fingerprint, skipping",
			"full", fullName, "version", versionHex)
		return nil
	}

	metaLayer, metaConstrained, metaLerr := resolveLayer(spec.OwnerNs)
	if metaLerr != nil || !metaConstrained {
		return fmt.Errorf("writeIndexSpec %q: owner_ns %q is not a concrete ns (err=%v constrained=%v)", fullName, spec.OwnerNs, metaLerr, metaConstrained)
	}
	if hadOld {
		if err := db.store(metaLayer).DropIndex(fullName); err != nil && !errors.Is(err, buntdb.ErrNotFound) {
			return fmt.Errorf("writeIndexSpec %q: drop prior btree: %w", fullName, err)
		}
	}
	if err := db.store(patternLayer).CreateIndex(fullName, spec.KeyPattern, indexJSONComposite(spec.Paths...)); err != nil {
		return fmt.Errorf("writeIndexSpec %q: create btree: %w", fullName, err)
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
		return fmt.Errorf("writeIndexSpec %q: marshal: %w", fullName, err)
	}
	targetKey := naming.IdxMetaKey(fullName, versionHex)

	if err := db.store(metaLayer).Update(func(tx *buntdb.Tx) error {
		if err := deleteIdxSpecMeta(tx, fullName); err != nil {
			return err
		}
		_, _, setErr := tx.Set(targetKey, string(raw), nil)
		return setErr
	}); err != nil {
		return fmt.Errorf("writeIndexSpec %q: persist %s: %w", fullName, targetKey, err)
	}

	db.idxRegMu.Lock()
	db.idxRegSpec[fullName] = spec
	db.idxRegMu.Unlock()

	if hadOld {
		slog.Info("writeIndexSpec: index rebuilt (new fingerprint)",
			"full", fullName, "owner_ns", spec.OwnerNs, "logical", spec.Logical,
			"old_version", oldSpec.Version, "new_version", versionHex,
			"paths", spec.Paths,
			"created_at", spec.CreatedAt.Format(time.RFC3339Nano),
			"updated_at", spec.UpdatedAt.Format(time.RFC3339Nano))
	} else {
		slog.Info("writeIndexSpec: new index persisted",
			"full", fullName, "owner_ns", spec.OwnerNs, "logical", spec.Logical,
			"version", versionHex, "paths", spec.Paths,
			"created_at", spec.CreatedAt.Format(time.RFC3339Nano))
	}
	return nil
}

// ——— Registry load + register lifecycle ———

// registerIndexes converts one or more x.Index values into idxSpec and
// delegates to writeIndexSpec (see writeIndexSpec for the MD5-fingerprint +
// btree lifecycle). Used by: REGIDX cmd, CreateIndex alias and boot fixtures.
func (db *DB) registerIndexes(indexes ...x.Index) error {
	for _, idx := range indexes {
		fullName := idx.Name()
		if fullName == "" {
			return fmt.Errorf("index name is required")
		}
		ownerNs, logical, err := naming.ParseIdxFullName(fullName)
		if err != nil {
			return fmt.Errorf("index %q: parse full name: %w", fullName, err)
		}
		spec := idxSpec{
			OwnerNs:    ownerNs,
			Logical:    logical,
			KeyPattern: idx.KeyPattern(),
			Paths:      idx.Paths(),
		}
		if err := db.writeIndexSpec(spec); err != nil {
			return err
		}
	}
	return nil
}

// loadIndexes scans disk+mem layers for persisted idx meta keys and mounts
// every idxSpec found. Intentionally runs BEFORE serving begins (openDB →
// StartWith bootstrap). Caller holds idxRegMu for the entire pass.
//
// The "no multi-version" invariant applies here: writeIndexSpec always
// deletes the prior meta key before writing the new one, so AscendKeys only
// ever returns 0 or 1 matches per fullName; there is no version-priority
// dedup step because it cannot happen.
func (db *DB) loadIndexes() error {
	type kv struct {
		metaKey string
		raw     string
	}
	patterns := []struct {
		layer storageLayer
		glob  string
	}{
		{layer: storageDisk, glob: naming.IdxMetaGlob()},
		{layer: storageMem, glob: naming.IdxMetaGlob()},
	}
	db.idxRegMu.Lock()
	defer db.idxRegMu.Unlock()
	for _, p := range patterns {
		var collected []kv
		err := db.store(p.layer).View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(p.glob, func(metaKey, value string) bool {
				collected = append(collected, kv{metaKey: metaKey, raw: value})
				return true
			})
		})
		if err != nil {
			return err
		}
		for _, c := range collected {
			var spec idxSpec
			if err := json.Unmarshal([]byte(c.raw), &spec); err != nil {
				return fmt.Errorf("index %s: unmarshal idxSpec: %w", c.metaKey, err)
			}
			if spec.OwnerNs == "" || spec.Logical == "" {
				return fmt.Errorf("index %s: missing owner_ns/logical", c.metaKey)
			}
			fullName := spec.fullName()
			layer, constrained, err := resolveLayer(spec.KeyPattern)
			if err != nil {
				return fmt.Errorf("index %s: %w", fullName, err)
			}
			if !constrained {
				return fmt.Errorf("index %s: key_pattern cannot start with wildcard", fullName)
			}
			if len(spec.Paths) == 0 {
				return fmt.Errorf("index %s: at least one json path is required", fullName)
			}
			if err := db.store(layer).CreateIndex(fullName, spec.KeyPattern, indexJSONComposite(spec.Paths...)); err != nil {
				return fmt.Errorf("index %s: create btree: %w", fullName, err)
			}
			db.idxRegSpec[fullName] = spec
		}
	}
	slog.Debug("index registry loaded", "count", len(db.idxRegSpec))
	return nil
}

// loadDocSpecs mirrors loadIndexes for the doc-spec registry. Same "exactly-
// one-version-per-storageNs invariant applies; AscendKeys dedup step is unnecessary.
func (db *DB) loadDocSpecs() error {
	type kv struct {
		metaKey string
		raw     string
	}
	patterns := []struct {
		layer storageLayer
		glob  string
	}{
		{layer: storageDisk, glob: naming.DocMetaGlob()},
		{layer: storageMem, glob: naming.DocMetaGlob()},
	}
	db.docRegMu.Lock()
	defer db.docRegMu.Unlock()
	for _, p := range patterns {
		var collected []kv
		err := db.store(p.layer).View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(p.glob, func(metaKey, value string) bool {
				collected = append(collected, kv{metaKey: metaKey, raw: value})
				return true
			})
		})
		if err != nil {
			return err
		}
		for _, c := range collected {
			var spec docSpec
			if err := json.Unmarshal([]byte(c.raw), &spec); err != nil {
				return fmt.Errorf("doc %s: unmarshal docSpec: %w", c.metaKey, err)
			}
			if spec.Namespace == "" {
				return fmt.Errorf("doc %s: empty namespace", c.metaKey)
			}
			db.docRegSpec[spec.storageNs()] = spec
		}
	}
	slog.Debug("doc registry loaded", "count", len(db.docRegSpec))
	return nil
}

// registerSchemas converts x.Schema values into docSpec and delegates to
// writeDocSpec (the single persistence entry point). Used by: REGSCH cmd,
// boot fixtures. x.Schema only provides the 4 user-visible fields; the 3
// internal fields (Version/CreatedAt/UpdatedAt) are stamped inside
// writeDocSpec.
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

// ——— Registry drop ———

// dropIndexByFullName is the DROPIDX command implementation (1 arg = fullName
// or 2 args = ownerNs + logical, resolved in cmd.go dropIdxCommand). idxRegMu
// is held for the entire op (spec read → btree DropIndex → meta probe/delete
// → map delete). Ctrl ops are low frequency so coarse lock scope keeps the
// reasoning symmetric with DROPSCH.
func (db *DB) dropIndexByFullName(fullName string) error {
	if db == nil {
		return errors.New("dropIndexByFullName: db is nil")
	}
	if fullName == "" {
		return errors.New("dropIndexByFullName: full_name is required")
	}
	ownerNs, _, err := naming.ParseIdxFullName(fullName)
	if err != nil {
		return fmt.Errorf("dropIndexByFullName: %w", err)
	}
	layer, constrained, lerr := resolveLayer(ownerNs)
	if lerr != nil || !constrained {
		return fmt.Errorf("dropIndexByFullName: ownerNs %q is not a concrete ns (err=%v constrained=%v)", ownerNs, lerr, constrained)
	}
	// Symmetric with dropSchemaByLogicalNs: acquire the registry write-lock up
	// front for the entire operation (spec lookup → btree DropIndex → meta
	// probe/delete → map delete). Ctrl registry ops are low frequency so the
	// coarse lock scope keeps reasoning simple and symmetric with DROPSCH.
	db.idxRegMu.Lock()
	defer db.idxRegMu.Unlock()
	spec, ok := db.idxRegSpec[fullName]
	if !ok {
		return fmt.Errorf("dropIndexByFullName: index %q not registered", fullName)
	}
	if err := db.store(layer).DropIndex(fullName); err != nil && !errors.Is(err, buntdb.ErrNotFound) {
		return fmt.Errorf("dropIndexByFullName: drop btree: %w", err)
	}
	glob := naming.IdxMetaGlobFor(fullName)
	hasAnyMeta := false
	if err := db.store(layer).View(func(tx *buntdb.Tx) error {
		_ = tx.AscendKeys(glob, func(_, _ string) bool {
			hasAnyMeta = true
			return false
		})
		return nil
	}); err != nil {
		return fmt.Errorf("dropIndexByFullName: probe %s meta: %w", fullName, err)
	}
	if hasAnyMeta {
		if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
			return deleteIdxSpecMeta(tx, fullName)
		}); err != nil {
			return fmt.Errorf("dropIndexByFullName: delete %s meta: %w", fullName, err)
		}
	}
	delete(db.idxRegSpec, fullName)
	slog.Debug("index dropped",
		"name", fullName, "owner_ns", spec.OwnerNs,
		"logical", spec.Logical, "mem", naming.HasMemPrefix(spec.OwnerNs))
	return nil
}

// dropSchemaByLogicalNs is the DROPSCH command implementation. It checks for
// attached indexes first (hard ERR with names when count>0; caller drops them
// via DROPIDX first), then deletes meta keys + docRegSpec entries on disk
// AND mem layers (both candidates probed). Coarse docRegMu lock is held for
// the candidates loop (symmetric with DROPIDX).
func (db *DB) dropSchemaByLogicalNs(logicalNs string) error {
	if db == nil {
		return errors.New("dropSchemaByLogicalNs: db is nil")
	}
	if logicalNs == "" {
		return errors.New("dropSchemaByLogicalNs: logical ns is required")
	}
	if err := naming.ValidateDocLogicalNamespace(logicalNs); err != nil {
		return fmt.Errorf("dropSchemaByLogicalNs: %w", err)
	}
	memStorageNs := naming.BuildStorageNs(logicalNs, true)
	diskStorageNs := naming.BuildStorageNs(logicalNs, false)
	db.idxRegMu.RLock()
	attachedNames := lo.FilterMap(lo.Entries(db.idxRegSpec), func(e lo.Entry[string, idxSpec], _ int) (string, bool) {
		base, _ := naming.StripMemPrefixIfMem(e.Value.OwnerNs)
		return e.Key, base == logicalNs || e.Value.OwnerNs == memStorageNs || e.Value.OwnerNs == diskStorageNs
	})
	db.idxRegMu.RUnlock()
	if len(attachedNames) > 0 {
		sort.Strings(attachedNames)
		return fmt.Errorf("doc %q still has %d attached index(es): %s — drop indexes first via DROPIDX", logicalNs, len(attachedNames), strings.Join(attachedNames, ","))
	}
	candidates := []struct {
		storageNs string
		layer     storageLayer
	}{
		{storageNs: diskStorageNs, layer: storageDisk},
		{storageNs: memStorageNs, layer: storageMem},
	}
	db.docRegMu.Lock()
	defer db.docRegMu.Unlock()
	anyFound := false
	for _, cand := range candidates {
		foundOnLayer := false
		if viewErr := db.store(cand.layer).View(func(tx *buntdb.Tx) error {
			_ = tx.AscendKeys(naming.DocMetaGlobFor(cand.storageNs), func(_, _ string) bool {
				foundOnLayer = true
				return false
			})
			return nil
		}); viewErr != nil {
			return fmt.Errorf("dropSchemaByLogicalNs: probe %s meta: %w", cand.storageNs, viewErr)
		}
		if foundOnLayer {
			anyFound = true
			if upErr := db.store(cand.layer).Update(func(tx *buntdb.Tx) error {
				return deleteDocSpecMeta(tx, cand.storageNs)
			}); upErr != nil {
				return fmt.Errorf("dropSchemaByLogicalNs: delete %s meta: %w", cand.storageNs, upErr)
			}
		}
		if _, ok := db.docRegSpec[cand.storageNs]; ok {
			anyFound = true
			delete(db.docRegSpec, cand.storageNs)
		}
	}
	if !anyFound {
		return fmt.Errorf("dropSchemaByLogicalNs: doc %q not registered (disk+mem both empty)", logicalNs)
	}
	slog.Debug("doc dropped", "logical_ns", logicalNs)
	return nil
}
