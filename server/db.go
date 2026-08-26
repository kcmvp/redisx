package server

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/samber/mo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/match"
	"github.com/tidwall/sjson"

	"github.com/kcmvp/redisx/x"
)

type storageLayer uint8

const (
	storageDisk storageLayer = iota
	storageMem
)

type docSpec struct {
	Namespace string        `json:"namespace"`
	Mem       bool          `json:"mem"`
	KeyAttrs  []string      `json:"key_attrs"`
	TTL       time.Duration `json:"ttl_ns"`

	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

func (p docSpec) StorageNs() string {
	return naming.BuildStorageNs(p.Namespace, p.Mem)
}

type idxSpec struct {
	OwnerNs    string   `json:"owner_ns"`
	Logical    string   `json:"logical"`
	KeyPattern string   `json:"key_pattern"`
	Paths      []string `json:"paths"`

	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

func (i idxSpec) FullName() string { return naming.BuildIdxFullName(i.OwnerNs, i.Logical) }

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

// ==========================================================================
// VERSIONED META HELPERS (Step 2 — canonical MD5 + latest load + stale cleanup)
// ==========================================================================

// md5VersionHex computes a 12-character truncated MD5 hex fingerprint over
// the canonical JSON encoding of v. The encoding sorts map keys alphabetically
// and omits non-semantic fields (Version, CreatedAt, UpdatedAt) so that
// fingerprints are deterministic across callers and time.
func md5VersionHex(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("md5VersionHex: marshal: %w", err)
	}
	sum := md5.Sum(raw)
	return hex.EncodeToString(sum[:])[:12], nil
}

type canonicalDoc struct {
	Namespace string        `json:"namespace"`
	Mem       bool          `json:"mem"`
	KeyAttrs  []string      `json:"key_attrs"`
	TTL       time.Duration `json:"ttl_ns"`
}

func canonicalDocMD5(spec docSpec) (string, error) {
	if len(spec.KeyAttrs) == 0 {
		spec.KeyAttrs = []string{}
	}
	return md5VersionHex(canonicalDoc{
		Namespace: spec.Namespace,
		Mem:       spec.Mem,
		KeyAttrs:  append([]string(nil), spec.KeyAttrs...),
		TTL:       spec.TTL,
	})
}

type canonicalIdx struct {
	OwnerNs    string   `json:"owner_ns"`
	Logical    string   `json:"logical"`
	KeyPattern string   `json:"key_pattern"`
	Paths      []string `json:"paths"`
}

func canonicalIdxMD5(spec idxSpec) (string, error) {
	paths := make([]string, len(spec.Paths))
	for i, p := range spec.Paths {
		paths[i] = strings.ReplaceAll(p, ".", "_")
	}
	if len(paths) == 0 {
		paths = []string{}
	}
	return md5VersionHex(canonicalIdx{
		OwnerNs:    spec.OwnerNs,
		Logical:    spec.Logical,
		KeyPattern: spec.KeyPattern,
		Paths:      paths,
	})
}

// loadDocSpec reads the single persisted doc meta key for storageNs.
// Uses the per-ns glob invariant (at most one v_* key exists after any
// write path; multiple stale ones are cleaned inside writeDocSpec Tx).
// Returns exists=false if no versioned meta is currently on disk.
func (db *DB) loadDocSpec(storageNs string) (spec docSpec, exists bool, err error) {
	if storageNs == "" {
		return spec, false, errors.New("loadDocSpec: storageNs is required")
	}
	layer := layerForStorageNs(storageNs)
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

// loadIdxSpec mirrors loadDocSpec for index metadata.
func (db *DB) loadIdxSpec(idxFullName string) (spec idxSpec, exists bool, err error) {
	if idxFullName == "" {
		return spec, false, errors.New("loadIdxSpec: idxFullName is required")
	}
	ownerNs, _, perr := naming.ParseIdxFullName(idxFullName)
	if perr != nil {
		return spec, false, fmt.Errorf("loadIdxSpec: %w", perr)
	}
	layer := layerForStorageNs(ownerNs)
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

// deleteStaleDocSpecVersions deletes all versioned doc meta keys for storageNs whose
// version fingerprint does NOT match keepVersion. Used inside writeDocSpec Tx
// to enforce the "exactly one latest version on disk" invariant.
func deleteStaleDocSpecVersions(tx *buntdb.Tx, storageNs, keepVersion string) error {
	var stale []string
	_ = tx.AscendKeys(naming.DocMetaGlobFor(storageNs), func(k, _ string) bool {
		ver := versionFromKey(k)
		if ver != keepVersion {
			stale = append(stale, k)
		}
		return true
	})
	for _, k := range stale {
		if _, e := tx.Delete(k); e != nil && !errors.Is(e, buntdb.ErrNotFound) {
			return fmt.Errorf("deleteStaleDocSpecVersions: %s: %w", k, e)
		}
	}
	return nil
}

// deleteStaleIdxSpecVersions mirrors deleteStaleDocSpecVersions for index meta keys.
func deleteStaleIdxSpecVersions(tx *buntdb.Tx, idxFullName, keepVersion string) error {
	var stale []string
	_ = tx.AscendKeys(naming.IdxMetaGlobFor(idxFullName), func(k, _ string) bool {
		ver := versionFromKey(k)
		if ver != keepVersion {
			stale = append(stale, k)
		}
		return true
	})
	for _, k := range stale {
		if _, e := tx.Delete(k); e != nil && !errors.Is(e, buntdb.ErrNotFound) {
			return fmt.Errorf("deleteStaleIdxSpecVersions: %s: %w", k, e)
		}
	}
	return nil
}

// versionFromKey extracts the 12-hex md5 fingerprint from the LAST segment
// of a versioned meta key (e.g. "_doc_:user:v_0123456789ab" → "0123456789ab").
// Returns "" if the key cannot be parsed.
func versionFromKey(metaKey string) string {
	last := strings.LastIndex(metaKey, ":")
	if last < 0 || last+1 >= len(metaKey) {
		return ""
	}
	tail := metaKey[last+1:]
	if !strings.HasPrefix(tail, "v_") || len(tail) != 2+12 {
		return ""
	}
	return tail[2:]
}

// ============================================================
// STEP 2 CORE WRITERS — writeDocSpec / writeIndexSpec
// ============================================================

// writeDocSpec is the UNIFIED entry point for ALL document spec writes
// (boot registerSchemas, user REGSCH cmd, future client RegisterSchema path2,
// fixture helpers). It never accepts a version as an input parameter:
// the caller supplies only the logical spec (namespace/mem/keyattrs/ttl)
// and the version fingerprint is computed & stored internally.
//
// Behaviour:
//   - Same MD5 as the latest persisted version → noop (Debug log).
//   - New / different spec → persist new v_<md5> meta key, delete ALL stale
//     v_* keys for the same storageNs, overwrite docRegSpec memory map.
//   - Lifecycle timestamps: first write sets CreatedAt=now UpdatedAt=zero;
//     subsequent writes preserve original CreatedAt and bump UpdatedAt=now.
func (db *DB) writeDocSpec(spec docSpec) error {
	if spec.Namespace == "" {
		return errors.New("writeDocSpec: namespace is required")
	}
	if err := naming.ValidateDocLogicalNamespace(spec.Namespace); err != nil {
		return fmt.Errorf("writeDocSpec: %w", err)
	}
	for i, p := range spec.KeyAttrs {
		if p == "" {
			return fmt.Errorf("writeDocSpec: key_attrs[%d] is empty", i)
		}
	}
	storageNs := spec.StorageNs()
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
	layer := layerForStorageNs(storageNs)
	targetKey := naming.DocMetaKey(storageNs, versionHex)

	if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		if err := deleteStaleDocSpecVersions(tx, storageNs, versionHex); err != nil {
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

// writeIndexSpec mirrors writeDocSpec for index specs. It additionally
// drops & re-creates the buntdb native index whenever the fingerprint
// changes (paths/order/pattern drift), ensuring the query comparator stays
// in sync with the stored metadata.
func (db *DB) writeIndexSpec(spec idxSpec) error {
	if spec.OwnerNs == "" {
		return errors.New("writeIndexSpec: owner_ns is required")
	}
	if spec.Logical == "" {
		return errors.New("writeIndexSpec: logical is required")
	}
	fullName := spec.FullName()
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
	patternLayer, constrained, err := resolvePatternLayer(spec.KeyPattern)
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

	metaLayer := layerForStorageNs(spec.OwnerNs)
	if hadOld {
		if err := db.store(metaLayer).DropIndex(fullName); err != nil && !errors.Is(err, buntdb.ErrNotFound) {
			return fmt.Errorf("writeIndexSpec %q: drop stale btree: %w", fullName, err)
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
		if err := deleteStaleIdxSpecVersions(tx, fullName, versionHex); err != nil {
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

// SECTION: Open

func openDB(path string) *DB {
	if path == "" {
		slog.Error("failed to open storage", "error", "db path is required")
		return nil
	}
	if path == ":memory:" {
		slog.Error("failed to open storage", "path", path, "error", `db path must be a file path; ":memory:" is reserved for buntdb in-memory mode`)
		return nil
	}
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		slog.Error("failed to open storage", "path", path, "error", "db path must be a file path, not a directory")
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		slog.Error("failed to create db directory", "path", path, "error", err)
		return nil
	}

	disk, err := buntdb.Open(path)
	if err != nil {
		slog.Error("failed to open buntdb", "path", path, "error", err)
		return nil
	}

	mem, err := buntdb.Open(":memory:")
	if err != nil {
		slog.Error("failed to open buntdb", "path", ":memory:", "error", err)
		_ = disk.Close()
		return nil
	}

	return &DB{
		disk:       disk,
		mem:        mem,
		docRegSpec: map[string]docSpec{},
		idxRegSpec: map[string]idxSpec{},
	}
}

// DB is a lightweight two-layer BuntDB wrapper for the high-frequency
// operations exposed by redisx.
//
// It is designed for two in-process use cases:
//   - direct embedded access from the same application
//   - the backing implementation behind RESP X commands
//
// redisx always opens two underlying BuntDB instances:
//   - one primary layer from dbPath
//   - one dedicated memory-only layer for keys prefixed with "_m_"
//
// For lower-level or BuntDB-specific operations, use [DB.Raw].
type DB struct {
	disk *buntdb.DB
	mem  *buntdb.DB

	docRegMu   sync.RWMutex
	docRegSpec map[string]docSpec

	idxRegMu   sync.RWMutex
	idxRegSpec map[string]idxSpec
}

// SECTION: Core DB

// Raw exposes the primary (disk) BuntDB instance for advanced/native use.
// This is the layer opened from dbPath.
func (db *DB) Raw() *buntdb.DB {
	return db.disk
}

// RawMem exposes the in-memory storage layer used by keys with the "_m_"
// prefix.
//
// Keys routed to this layer are not persisted across restarts.
func (db *DB) RawMem() *buntdb.DB {
	return db.mem
}

// Close closes both the persistent and in-memory storage layers.
//
// The first close error is returned, if any.
func (db *DB) Close() error {
	var firstErr error
	if db.mem != nil {
		closeErr := db.mem.Close()
		if closeErr != nil {
			firstErr = closeErr
		}
	}
	if db.disk != nil {
		closeErr := db.disk.Close()
		if closeErr != nil && firstErr == nil {
			firstErr = closeErr
		}
	}
	return firstErr
}

// SECTION: Storage Routing

func isMemKey(key string) bool {
	return naming.HasMemPrefix(key)
}

func hasLeadingWildcard(keyPattern string) bool {
	return keyPattern != "" && (keyPattern[0] == '*' || keyPattern[0] == '?')
}

func layerForKey(key string) storageLayer {
	if isMemKey(key) {
		return storageMem
	}
	return storageDisk
}

// resolvePatternLayer reports whether keyPattern alone pins the target layer.
// A leading wildcard is treated as unconstrained, not an error.
func resolvePatternLayer(keyPattern string) (storageLayer, bool, error) {
	if keyPattern == "" {
		return storageDisk, false, errors.New("key pattern is required")
	}
	if hasLeadingWildcard(keyPattern) {
		return storageDisk, false, nil
	}
	return layerForKey(keyPattern), true, nil
}

func layerForStorageNs(storageNs string) storageLayer {
	if naming.HasMemPrefix(storageNs) {
		return storageMem
	}
	return storageDisk
}

// store returns the underlying storage handle for layer.
func (db *DB) store(layer storageLayer) *buntdb.DB {
	if layer == storageMem {
		return db.mem
	}
	return db.disk
}

func setOptionsForTTL(ttl time.Duration) *buntdb.SetOptions {
	if ttl <= 0 {
		return nil
	}
	return &buntdb.SetOptions{Expires: true, TTL: ttl}
}

// SECTION: Query And Update

// Update applies one or more x.Mutation values to every doc matched by kr
// and filter. Each mutation uses sjson path semantics. keyPattern must be
// one full storage-key glob (cannot start with wildcard).
func (db *DB) Update(kr x.KeyRange, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	layerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		return layerForKey(k)
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	layer, ok := layerObj.(storageLayer)
	if !ok {
		return mo.Err[[]string](fmt.Errorf("layerForKey returned non-storageLayer type %T for key range routing", layerObj))
	}
	var updatedKeys []string
	var err error
	err = db.store(layer).Update(func(tx *buntdb.Tx) error {
		var matchedKeys []string
		scanErr := applyKeyRange(tx, kr, x.RangeAsc, func(key, value string) bool {
			if filter == nil || filter.Eval(value) {
				matchedKeys = append(matchedKeys, key)
			}
			return true
		})
		if scanErr != nil {
			return scanErr
		}

		for _, key := range matchedKeys {
			val, getErr := tx.Get(key)
			if getErr != nil {
				return getErr
			}
			ttl, ttlErr := tx.TTL(key)
			if ttlErr != nil && !errors.Is(ttlErr, buntdb.ErrNotFound) {
				return ttlErr
			}

			newVal := val
			for _, vp := range values {
				newVal, err = sjson.Set(newVal, vp.Path(), vp.Value())
				if err != nil {
					err = fmt.Errorf("failed to set %s: %w", vp.Path(), err)
					slog.Error("failed to update document", "key", key, "error", err)
					return err
				}
			}

			storageNs, _, splitErr := naming.SplitStorageKey(key)
			if splitErr == nil && !naming.IsInternalStorageNs(storageNs) {
				doc, regOK := db.docRegSpec[storageNs]
				if regOK {
					oldDK, dkErr := deriveDocKey(doc, storageNs, val)
					if dkErr == nil {
						newDK, dkErr2 := deriveDocKey(doc, storageNs, newVal)
						if dkErr2 == nil && oldDK.FullStorageKey != newDK.FullStorageKey {
							return fmt.Errorf("UPDATE would change primary key of %q from %q to %q — pk mutations are not allowed", key, oldDK.FullStorageKey, newDK.FullStorageKey)
						}
					}
				}
			}

			if newVal != val {
				_, _, err = tx.Set(key, newVal, setOptionsForTTL(ttl))
				if err != nil {
					return err
				}
			}
			updatedKeys = append(updatedKeys, key)
		}

		return nil
	})
	if err != nil {
		return mo.Err[[]string](err)
	}

	sort.Strings(updatedKeys)
	return mo.Ok(updatedKeys)
}

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

func (db *DB) CreateIndex(idx x.Index) error {
	return db.registerIndexes(idx)
}

func (db *DB) UpsertDoc(s x.Schema) error {
	return db.registerSchemas(s)
}

func (db *DB) UpsertIndex(i x.Index) error {
	return db.registerIndexes(i)
}

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
			fullName := spec.FullName()
			layer, constrained, err := resolvePatternLayer(spec.KeyPattern)
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
			db.docRegSpec[spec.StorageNs()] = spec
		}
	}
	slog.Debug("doc registry loaded", "count", len(db.docRegSpec))
	return nil
}

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

// SearchIndex scans one registered index on its bound storage layer,
// narrows by kr and optional filter, and returns matched JSON documents in
// index order. The kr KeyRange cannot start with wildcard and must target
// the same layer as the index.
func (db *DB) SearchIndex(indexName string, kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if indexName == "" {
		return mo.Err[[]string](errors.New("index name is required"))
	}
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	idxSpec, ok := db.idxRegSpec[indexName]
	if !ok {
		return mo.Err[[]string](fmt.Errorf("index not found: %s", indexName))
	}
	layer := storageDisk
	if naming.HasMemPrefix(idxSpec.OwnerNs) {
		layer = storageMem
	}
	krLayerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		return layerForKey(k)
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	krLayer, ok := krLayerObj.(storageLayer)
	if !ok {
		return mo.Err[[]string](fmt.Errorf("layerForKey returned non-storageLayer type %T for key range routing", krLayerObj))
	}
	if krLayer != layer {
		return mo.Err[[]string](fmt.Errorf("key range targets a different storage layer than index %s", indexName))
	}

	idxKeySep := "\x00"
	limit := kr.GetLimit()

	var results []string
	err := db.store(layer).View(func(tx *buntdb.Tx) error {
		iter := tx.Ascend
		if desc {
			iter = tx.Descend
		}
		return iter(indexName, func(ik, value string) bool {
			sepIdx := strings.Index(ik, idxKeySep)
			var storageKey string
			if sepIdx >= 0 {
				storageKey = ik[sepIdx+1:]
			} else {
				storageKey = ik
			}
			if !x.MatchesStorageKey(kr, storageKey) {
				return true
			}
			if !gjson.Valid(value) {
				return true
			}
			if filter == nil || filter.Eval(value) {
				results = append(results, value)
				if limit > 0 && len(results) == limit {
					return false
				}
			}
			return true
		})
	})

	if err != nil {
		if errors.Is(err, buntdb.ErrNotFound) {
			return mo.Err[[]string](fmt.Errorf("index not found: %s", indexName))
		}
		return mo.Err[[]string](err)
	}
	return mo.Ok(results)
}

// SearchKey resolves one sealed x.KeyRange to a single storage layer, walks
// the matching default storage-index key range using native BTree iterators,
// applies the optional filter, and returns matched JSON documents in ASC or
// DESC key order. The KeyRange must pin exactly one storage layer.
func (db *DB) SearchKey(kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	layerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		return layerForKey(k)
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	layer, ok := layerObj.(storageLayer)
	if !ok {
		return mo.Err[[]string](fmt.Errorf("layerForKey returned non-storageLayer type %T for key range routing", layerObj))
	}

	dir := x.RangeAsc
	if desc {
		dir = x.RangeDesc
	}
	var results []string
	err := db.store(layer).View(func(tx *buntdb.Tx) error {
		return applyKeyRange(tx, kr, dir, func(_, value string) bool {
			if !gjson.Valid(value) {
				return true
			}
			if filter == nil || filter.Eval(value) {
				results = append(results, value)
			}
			return true
		})
	})
	if err != nil {
		return mo.Err[[]string](err)
	}
	return mo.Ok(results)
}

// SECTION: Key Value

// Set stores one string value under key.
//
// Routing rules:
//   - keys prefixed with "_m_" are stored in the in-memory layer
//   - all other keys are stored in the persistent layer
//
// key must be the full storage key.
//
// RESP equivalent:
//
//	SET <key> <value>
func (db *DB) Set(key string, value string) error {
	return db.store(layerForKey(key)).Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

// SetWithTtl stores one string value under key with an optional TTL.
//
// A positive ttl makes the key expire automatically. A zero or negative ttl
// behaves the same as [DB.Set].
//
// key must be the full storage key.
//
// RESP equivalents:
//
//	SET <key> <value> EX <seconds>
//	SETEX <key> <seconds> <value>
func (db *DB) SetWithTtl(key string, value string, ttl time.Duration) error {
	ttl = db.autoTTLFromKey(key, ttl)
	opt := setOptionsForTTL(ttl)
	if opt == nil {
		return db.Set(key, value)
	}
	return db.store(layerForKey(key)).Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, opt)
		return err
	})
}

// SetNX stores value only when key does not already exist.
//
// The returned boolean reports whether the write happened.
//
// key must be the full storage key.
//
// RESP equivalent:
//
//	SETNX <key> <value>
func (db *DB) SetNX(key string, value string) (bool, error) {
	return db.SetNXWithTtl(key, value, 0)
}

// SetNXWithTtl stores value only when key does not already exist, and applies
// the provided TTL when it is positive.
//
// The returned boolean reports whether the write happened.
//
// key must be the full storage key.
func (db *DB) SetNXWithTtl(key string, value string, ttl time.Duration) (bool, error) {
	ttl = db.autoTTLFromKey(key, ttl)
	var set bool
	opt := setOptionsForTTL(ttl)
	err := db.store(layerForKey(key)).Update(func(tx *buntdb.Tx) error {
		_, err := tx.Get(key)
		if err == buntdb.ErrNotFound {
			_, _, err = tx.Set(key, value, opt)
			if err == nil {
				set = true
			}
			return err
		}
		return err
	})
	if err != nil {
		return false, err
	}
	return set, nil
}

// Get returns the string value stored under key.
//
// A missing key is returned as an error result.
//
// key must be the full storage key.
//
// RESP equivalent:
//
//	GET <key>
func (db *DB) Get(key string) mo.Result[string] {
	var val string
	err := db.store(layerForKey(key)).View(func(tx *buntdb.Tx) error {
		var innerErr error
		val, innerErr = tx.Get(key)
		return innerErr
	})

	if err != nil {
		return mo.Err[string](err)
	}
	return mo.Ok(val)
}

// Delete removes key from its routed storage layer.
//
// The returned boolean reports whether the key existed.
//
// key must be the full storage key.
//
// RESP equivalent:
//
//	DEL <key>
func (db *DB) Delete(key string) (bool, error) {
	var val string
	err := db.store(layerForKey(key)).Update(func(tx *buntdb.Tx) error {
		var innerErr error
		val, innerErr = tx.Delete(key)
		if innerErr == buntdb.ErrNotFound {
			return nil
		}
		return innerErr
	})
	if err != nil {
		return false, err
	}
	return len(val) > 0, nil
}

// Keys resolves one full storage-key pattern to one storage layer first, then
// returns all matching keys from that layer.
//
// keyPattern uses key glob matching such as "*" and "?".
//
// RESP equivalent:
//
//	KEYS <key_pattern>
func (db *DB) Keys(keyPattern string) mo.Result[[]string] {
	layer, constrained, err := resolvePatternLayer(keyPattern)
	if err != nil {
		return mo.Err[[]string](err)
	}
	if !constrained {
		return mo.Err[[]string](errors.New("key pattern cannot start with wildcard"))
	}
	var keys []string
	err = db.store(layer).View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(keyPattern, func(key, value string) bool {
			keys = append(keys, key)
			return true
		})
	})
	if err != nil {
		return mo.Err[[]string](err)
	}
	sort.Strings(keys)
	return mo.Ok(keys)
}

func withLimit(limit int, fn func(key, value string) bool) func(key, value string) bool {
	if limit <= 0 {
		return fn
	}
	consumed := 0
	return func(key, value string) bool {
		if !fn(key, value) {
			return false
		}
		consumed++
		return consumed < limit
	}
}

func nsGuard(pivot string, fn func(k, v string) bool) func(k, v string) bool {
	storageNs, _, err := naming.SplitStorageKey(pivot)
	if err != nil || storageNs == "" {
		return fn
	}
	scope := naming.StorageNsScope(storageNs)
	n := len(scope)
	return func(k, v string) bool {
		if len(k) <= n || k[:n] != scope {
			return true
		}
		return fn(k, v)
	}
}

func matchFilter(predicate func(key string) bool, fn func(key, value string) bool) func(key, value string) bool {
	return func(key, value string) bool {
		if !predicate(key) {
			return true
		}
		return fn(key, value)
	}
}

func upperBoundCutoff(hi string, fn func(key, value string) bool) func(key, value string) bool {
	if hi == "" {
		return fn
	}
	return func(key, value string) bool {
		if key >= hi {
			return false
		}
		return fn(key, value)
	}
}

func lowerBoundCutoff(lo string, fn func(key, value string) bool) func(key, value string) bool {
	if lo == "" {
		return fn
	}
	return func(key, value string) bool {
		if key < lo {
			return false
		}
		return fn(key, value)
	}
}

func applyBtRange(tx *buntdb.Tx, ge, lt string, limit int, dir x.RangeDirection, fn func(key, value string) bool) error {
	cb := withLimit(limit, fn)
	if x.IsLiteral(ge) && x.IsLiteral(lt) {
		cb = nsGuard(ge, cb)
		if dir == x.RangeAsc {
			return tx.AscendRange("", ge, lt, cb)
		}
		return tx.DescendLessOrEqual("", x.NextLex(lt), func(k, v string) bool {
			if k >= lt {
				return true
			}
			if k < ge {
				return false
			}
			return cb(k, v)
		})
	}
	loGe, _ := match.Allowable(ge)
	_, hiLt := match.Allowable(lt)
	pred := func(k string) bool {
		geOK := k >= ge || !x.IsLiteral(ge) && match.Match(k, ge)
		ltOK := k < lt || !x.IsLiteral(lt) && match.Match(k, lt)
		return geOK && ltOK
	}
	cb = matchFilter(pred, cb)
	if dir == x.RangeAsc {
		return tx.AscendGreaterOrEqual("", loGe, upperBoundCutoff(hiLt, cb))
	}
	return tx.DescendLessOrEqual("", hiLt, lowerBoundCutoff(loGe, cb))
}

func applySingleBoundaryLiteralASC(tx *buntdb.Tx, op string, pivot string, cb func(k, v string) bool) error {
	cb = nsGuard(pivot, cb)
	switch op {
	case "gt":
		return tx.AscendGreaterOrEqual("", x.NextLex(pivot), cb)
	case "gte":
		return tx.AscendGreaterOrEqual("", pivot, cb)
	case "lt":
		return tx.AscendLessThan("", pivot, cb)
	case "lte":
		return tx.AscendLessThan("", x.NextLex(pivot), cb)
	}
	panic("applySingleBoundaryLiteralASC: unknown op " + op)
}

func applySingleBoundaryLiteralDESC(tx *buntdb.Tx, op string, pivot string, cb func(k, v string) bool) error {
	cb = nsGuard(pivot, cb)
	switch op {
	case "gt":
		return tx.DescendRange("", "\xFF\xFF\xFF\xFF", pivot, cb)
	case "gte":
		return tx.DescendLessOrEqual("", "\xFF\xFF\xFF\xFF",
			lowerBoundCutoff(pivot, cb))
	case "lt":
		return tx.DescendLessOrEqual("", x.NextLex(pivot), func(k, v string) bool {
			if k >= pivot {
				return true
			}
			return cb(k, v)
		})
	case "lte":
		return tx.DescendLessOrEqual("", pivot, cb)
	}
	panic("applySingleBoundaryLiteralDESC: unknown op " + op)
}

func applySingleBoundaryPattern(tx *buntdb.Tx, dir x.RangeDirection, op string, pivot string, limit int, fn func(k, v string) bool) error {
	lo, hi := match.Allowable(pivot)
	var pred func(k string) bool
	switch op {
	case "gt":
		pred = func(k string) bool { return k > pivot && match.Match(k, pivot) }
	case "gte":
		pred = func(k string) bool { return k >= pivot && match.Match(k, pivot) }
	case "lt":
		pred = func(k string) bool { return k < pivot && match.Match(k, pivot) }
	case "lte":
		pred = func(k string) bool { return k <= pivot && match.Match(k, pivot) }
	default:
		panic("applySingleBoundaryPattern: unknown op " + op)
	}
	if dir == x.RangeAsc {
		return tx.AscendGreaterOrEqual("", lo,
			upperBoundCutoff(hi, matchFilter(pred, withLimit(limit, fn))))
	}
	return tx.DescendLessOrEqual("", hi,
		lowerBoundCutoff(lo, matchFilter(pred, withLimit(limit, fn))))
}

func applySingleBoundary(tx *buntdb.Tx, op string, pivot string, limit int, dir x.RangeDirection, fn func(key, value string) bool) error {
	if x.IsLiteral(pivot) {
		cb := withLimit(limit, fn)
		if dir == x.RangeAsc {
			return applySingleBoundaryLiteralASC(tx, op, pivot, cb)
		}
		return applySingleBoundaryLiteralDESC(tx, op, pivot, cb)
	}
	return applySingleBoundaryPattern(tx, dir, op, pivot, limit, fn)
}

func applyPatternRange(tx *buntdb.Tx, p string, limit int, dir x.RangeDirection, fn func(key, value string) bool) error {
	if dir == x.RangeAsc {
		if p == "" {
			return nil
		}
		if p[0] == '*' {
			if p == "*" {
				return tx.Ascend("", withLimit(limit, fn))
			}
			cb := matchFilter(func(k string) bool { return match.Match(k, p) }, withLimit(limit, fn))
			return tx.Ascend("", cb)
		}
		min, max := match.Allowable(p)
		cb := upperBoundCutoff(max,
			matchFilter(func(k string) bool { return match.Match(k, p) },
				withLimit(limit, fn)))
		return tx.AscendGreaterOrEqual("", min, cb)
	}
	if p == "" {
		return nil
	}
	if p[0] == '*' {
		if p == "*" {
			return tx.Descend("", withLimit(limit, fn))
		}
		cb := matchFilter(func(k string) bool { return match.Match(k, p) }, withLimit(limit, fn))
		return tx.Descend("", cb)
	}
	min, max := match.Allowable(p)
	cb := lowerBoundCutoff(min,
		matchFilter(func(k string) bool { return match.Match(k, p) },
			withLimit(limit, fn)))
	return tx.DescendLessOrEqual("", max, cb)
}

func applyKeyRange(tx *buntdb.Tx, kr x.KeyRange, dir x.RangeDirection, fn func(key, value string) bool) error {
	kind, pa, pb, limit := x.InspectKeyRange(kr)
	switch kind {
	case x.KeyRangeBt:
		return applyBtRange(tx, pa, pb, limit, dir, fn)
	case x.KeyRangeGt:
		return applySingleBoundary(tx, "gt", pa, limit, dir, fn)
	case x.KeyRangeGte:
		return applySingleBoundary(tx, "gte", pa, limit, dir, fn)
	case x.KeyRangeLt:
		return applySingleBoundary(tx, "lt", pa, limit, dir, fn)
	case x.KeyRangeLte:
		return applySingleBoundary(tx, "lte", pa, limit, dir, fn)
	case x.KeyRangePattern:
		return applyPatternRange(tx, pa, limit, dir, fn)
	}
	panic("applyKeyRange: unhandled KeyRangeKind " + kind.String())
}

// ============================================================
// Internal helpers for atomic batched writes + index drop.
// Batch atomicity enforces single storage layer (disk XOR mem).
// ============================================================

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
	layer := layerForStorageNs(ownerNs)
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
	if err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		var keys []string
		if err := tx.AscendKeys(glob, func(k, _ string) bool {
			keys = append(keys, k)
			return true
		}); err != nil {
			return err
		}
		for _, k := range keys {
			if _, delErr := tx.Delete(k); delErr != nil && !errors.Is(delErr, buntdb.ErrNotFound) {
				return delErr
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("dropIndexByFullName: delete %s meta: %w", fullName, err)
	}
	delete(db.idxRegSpec, fullName)
	slog.Debug("index dropped",
		"name", fullName, "owner_ns", spec.OwnerNs,
		"logical", spec.Logical, "mem", naming.HasMemPrefix(spec.OwnerNs))
	return nil
}

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
	attached := 0
	var attachedNames []string
	for full, spec := range db.idxRegSpec {
		base, _ := naming.StripMemPrefixIfMem(spec.OwnerNs)
		if base == logicalNs || spec.OwnerNs == memStorageNs || spec.OwnerNs == diskStorageNs {
			attached++
			attachedNames = append(attachedNames, full)
		}
	}
	db.idxRegMu.RUnlock()
	if attached > 0 {
		sort.Strings(attachedNames)
		return fmt.Errorf("doc %q still has %d attached index(es): %s — drop indexes first via DROPIDX", logicalNs, attached, strings.Join(attachedNames, ","))
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
		glob := naming.DocMetaGlobFor(cand.storageNs)
		var metaKeys []string
		if viewErr := db.store(cand.layer).View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(glob, func(k, _ string) bool {
				metaKeys = append(metaKeys, k)
				return true
			})
		}); viewErr != nil {
			return fmt.Errorf("dropSchemaByLogicalNs: scan %s meta: %w", cand.storageNs, viewErr)
		}
		if len(metaKeys) > 0 {
			anyFound = true
			if upErr := db.store(cand.layer).Update(func(tx *buntdb.Tx) error {
				for _, k := range metaKeys {
					if _, delErr := tx.Delete(k); delErr != nil && !errors.Is(delErr, buntdb.ErrNotFound) {
						return delErr
					}
				}
				return nil
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

type batchedWrite struct {
	Key   string
	Value string
	TTL   time.Duration
}

var errNxPreconditionFailed = errors.New("setBatchAtomic: nx precondition failed — one or more keys already exist")

func (db *DB) setBatchAtomic(batch []batchedWrite, nxMode bool) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	layers := make(map[storageLayer][]batchedWrite, 2)
	for _, w := range batch {
		l := layerForKey(w.Key)
		layers[l] = append(layers[l], w)
	}
	if len(layers) != 1 {
		return 0, errors.New("atomic batch writes cannot span storage layers; pick consistent Mem flag across payloads")
	}
	var layer storageLayer
	var writes []batchedWrite
	for l, ws := range layers {
		layer = l
		writes = ws
	}
	applied := 0
	err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		seen := make(map[string]struct{}, len(writes))
		for _, w := range writes {
			if w.Key == "" {
				return errors.New("batch: empty key")
			}
			if _, dup := seen[w.Key]; dup {
				return fmt.Errorf("batch: duplicate key %q in single SET", w.Key)
			}
			seen[w.Key] = struct{}{}
		}
		if nxMode {
			for _, w := range writes {
				_, err := tx.Get(w.Key)
				if err == nil {
					return errNxPreconditionFailed
				}
				if err != buntdb.ErrNotFound {
					return err
				}
			}
		}
		for _, w := range writes {
			opt := setOptionsForTTL(w.TTL)
			_, _, err := tx.Set(w.Key, w.Value, opt)
			if err != nil {
				return err
			}
			applied++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return applied, nil
}

func (db *DB) deleteBatchAtomic(keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	layers := make(map[storageLayer][]string, 2)
	for _, k := range keys {
		l := layerForKey(k)
		layers[l] = append(layers[l], k)
	}
	if len(layers) != 1 {
		return 0, errors.New("atomic batch deletes cannot span storage layers")
	}
	var layer storageLayer
	var ks []string
	for l, list := range layers {
		layer = l
		ks = list
	}
	seen := make(map[string]struct{}, len(ks))
	for _, k := range ks {
		if _, dup := seen[k]; dup {
			return 0, fmt.Errorf("batch: duplicate key %q in single DEL", k)
		}
		seen[k] = struct{}{}
	}
	deleted := 0
	err := db.store(layer).Update(func(tx *buntdb.Tx) error {
		for _, k := range ks {
			val, err := tx.Delete(k)
			if err != nil && err != buntdb.ErrNotFound {
				return err
			}
			if err == nil && len(val) > 0 {
				deleted++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}
