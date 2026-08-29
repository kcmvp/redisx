package server

import (
	"crypto/rand"
	"encoding/hex"
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
	"github.com/tidwall/sjson"

	"github.com/kcmvp/redisx/x"
)

// ——— Type / Struct definitions ———

type storageLayer uint8

const (
	storageDisk storageLayer = iota
	storageMem
)

const (
	installedAppAuth  = "app_0"
	installedCtrlAuth = "ctrl_0"
)

// DB is a lightweight two-layer BuntDB wrapper (disk + memory). Memory layer
// keys are routed via the "_m_:" storageNs prefix. All typed-doc registry
// logic (docRegSpec / idxRegSpec maps) live in-process alongside storage so
// registry lookups are zero I/O.
type DB struct {
	disk *buntdb.DB
	mem  *buntdb.DB

	docRegMu   sync.RWMutex
	docRegSpec map[string]docSpec

	idxRegMu   sync.RWMutex
	idxRegSpec map[string]idxSpec
}

// ——— Public (exported) DB methods ———

// ——— DB raw access + lifecycle ———

// Raw exposes the primary (disk) BuntDB instance. For "_m_:" keys use RawMem.
func (db *DB) raw() *buntdb.DB {
	return db.disk
}

// RawMem exposes the in-memory-only storage layer. Keys here are NOT persisted
// across server restarts.
func (db *DB) rawMem() *buntdb.DB {
	return db.mem
}

// EffectiveAuthKeys returns the two effective port-role-bound auth keys that
// are persisted on disk under `_auth_:app_0` and `_auth_:ctrl_0`. These are
// the keys printed on the bootstrap banner whenever any slot was generated.
// Empty strings are returned for slots that have not been written yet
// (should never happen after a successful StartWithConfig bootstrap).
// SSoT = direct DB read from `_auth_` namespace, no in-memory cache.
func (db *DB) EffectiveAuthKeys() (app_0, ctrl_0 string) {
	if db == nil || db.disk == nil {
		return "", ""
	}
	_ = db.disk.View(func(tx *buntdb.Tx) error {
		if v, err := tx.Get(naming.AuthStorageKey(installedAppAuth)); err == nil {
			app_0 = v
		}
		if v, err := tx.Get(naming.AuthStorageKey(installedCtrlAuth)); err == nil {
			ctrl_0 = v
		}
		return nil
	})
	return app_0, ctrl_0
}

// Close closes both storage layers and returns the first error, if any.
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

// bootstrapAuth resolves the dual-port AUTH pair using a strict per-slot
// priority cascade:  cfg seed  →  DB stored value  →  crypto/rand 128-bit hex.
//
// Rules (simplified SSoT):
//
//  1. If cfg.App.Auth (seedApp) is non-empty → it wins the app slot unconditio-
//     nally. The ctrl slot resolves similarly from seedCtrl.
//  2. For whichever slot the seed is empty, fall back to whatever is stored on
//     disk under `_auth_:app` / `_auth_:ctrl`; if that slot is also missing,
//     generate a fresh 128-bit random hex for just that one missing slot.
//  3. The resulting resolved values are written back to disk (in a single
//     Update tx) iff the slot's value differs from what was on disk (no-op
//     reads are never persisted).
//  4. `anyGenerated` is true if at least one slot had to be randomly produced
//     this call; callers (srv.StartWithConfig) use this flag to decide whether
//     to print the credentials banner to stdout.
//
// No dual-port distinctness invariant is enforced here; equality between the
// two slots is allowed and the caller does not error on it. No "partial pair
// corrupted" failure mode: partial storage (one slot on disk, one missing) is
// silently repaired by generating only the missing half.
func (db *DB) bootstrapAuth(seedApp, seedCtrl string) (finalApp, finalCtrl string, anyGenerated bool, err error) {
	if db == nil || db.disk == nil {
		return "", "", false, fmt.Errorf("bootstrapAuth: storage not initialised")
	}
	appKey := naming.AuthStorageKey(installedAppAuth)
	ctrlKey := naming.AuthStorageKey(installedCtrlAuth)

	var storedApp, storedCtrl string
	err = db.disk.View(func(tx *buntdb.Tx) error {
		if av, aerr := tx.Get(appKey); aerr == nil {
			storedApp = av
		} else if aerr != buntdb.ErrNotFound {
			return fmt.Errorf("bootstrapAuth: read %s: %w", appKey, aerr)
		}
		if cv, cerr := tx.Get(ctrlKey); cerr == nil {
			storedCtrl = cv
		} else if cerr != buntdb.ErrNotFound {
			return fmt.Errorf("bootstrapAuth: read %s: %w", ctrlKey, cerr)
		}
		return nil
	})
	if err != nil {
		return "", "", false, err
	}

	finalApp = storedApp
	finalCtrl = storedCtrl
	writeApp := false
	writeCtrl := false
	if seedApp != "" {
		if finalApp != seedApp {
			finalApp = seedApp
			writeApp = true
		}
	} else if finalApp == "" {
		gapp, _, gerr := generateBootstrapAuth()
		if gerr != nil {
			return "", "", false, fmt.Errorf("bootstrapAuth: gen app slot: %w", gerr)
		}
		finalApp = gapp
		writeApp = true
		anyGenerated = true
	}
	if seedCtrl != "" {
		if finalCtrl != seedCtrl {
			finalCtrl = seedCtrl
			writeCtrl = true
		}
	} else if finalCtrl == "" {
		_, gctrl, gerr := generateBootstrapAuth()
		if gerr != nil {
			return "", "", false, fmt.Errorf("bootstrapAuth: gen ctrl slot: %w", gerr)
		}
		finalCtrl = gctrl
		writeCtrl = true
		anyGenerated = true
	}
	if !writeApp && !writeCtrl {
		return finalApp, finalCtrl, anyGenerated, nil
	}
	err = db.disk.Update(func(tx *buntdb.Tx) error {
		if writeApp {
			if _, _, serr := tx.Set(appKey, finalApp, nil); serr != nil {
				return fmt.Errorf("bootstrapAuth: persist app slot: %w", serr)
			}
		}
		if writeCtrl {
			if _, _, serr := tx.Set(ctrlKey, finalCtrl, nil); serr != nil {
				return fmt.Errorf("bootstrapAuth: persist ctrl slot: %w", serr)
			}
		}
		return nil
	})
	if err != nil {
		return "", "", false, err
	}
	if writeApp && seedApp != "" {
		slog.Info("bootstrapAuth: app slot overwritten by cfg.App.Auth")
	}
	if writeCtrl && seedCtrl != "" {
		slog.Info("bootstrapAuth: ctrl slot overwritten by cfg.Ctrl.Auth")
	}
	return finalApp, finalCtrl, anyGenerated, nil
}

// CreateIndex is a small alias over registerIndexes; see registerIndexes for
// the actual x.Index → idxSpec conversion + writeIndexSpec persistence flow.
func (db *DB) createIndex(idx x.Index) error {
	return db.registerIndexes(idx)
}

// ——— Typed-doc query & update API ———

// Update is TYPED-DOC ONLY (there is no KV-style UPDATE bypass per the colon-
// routing SSOT; the cmd.go UPDATE handler already resolved the keyrange anchor
// to a single storageNs, verified it is registered (non-internal) and rejected
// KV-shaped bare patterns). Caller provides the key-range + filter that pins
// to exactly one storage layer; mutations are sjson path writes executed
// inside a single BuntDB Update tx so either all matched rows change or none
// do. Primary keys derived from KeyAttrs are re-derived post-mutation; if any
// pk would drift the whole tx is aborted with zero writes.
func (db *DB) Update(kr x.KeyRange, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	layerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		l, ok, lerr := resolveLayer(k)
		if lerr != nil || !ok {
			return fmt.Errorf("Update: key %q is not a concrete key (err=%v constrained=%v)", k, lerr, ok)
		}
		return l
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	layer, ok := layerObj.(storageLayer)
	if !ok {
		if lerr, isErr := layerObj.(error); isErr {
			return mo.Err[[]string](lerr)
		}
		return mo.Err[[]string](fmt.Errorf("resolveLayer returned non-storageLayer type %T for key range routing", layerObj))
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

// SearchIndex scans via a buntdb-native secondary index handle. The index
// name must already be registered (REGIDX / boot registerSchemas seed or
// runtime REGIDX); an unregistered name returns Err "index not found". Key-
// range layer and index ownerNs layer must match (cross-layer searches are
// parameter-ERR). Matched rows are optionally narrowed by filter.Eval; the
// SEARCHINDEX command also respects colon-routing (cmd.go searchIndexCommand
// — index name is always logical, KV-passthrough route is SEARCHKEY only),
// per the typed-doc-SSOT for registry-bound queries.
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
	layer, constrained, lerr := resolveLayer(idxSpec.OwnerNs)
	if lerr != nil || !constrained {
		return mo.Err[[]string](fmt.Errorf("SearchIndex: owner_ns %q is not a concrete ns (err=%v constrained=%v)", idxSpec.OwnerNs, lerr, constrained))
	}
	krLayerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		kl, kok, klerr := resolveLayer(k)
		if klerr != nil || !kok {
			return fmt.Errorf("SearchIndex: key %q is not a concrete key (err=%v constrained=%v)", k, klerr, kok)
		}
		return kl
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	krLayer, ok := krLayerObj.(storageLayer)
	if !ok {
		if klerr, isErr := krLayerObj.(error); isErr {
			return mo.Err[[]string](klerr)
		}
		return mo.Err[[]string](fmt.Errorf("resolveLayer returned non-storageLayer type %T for key range routing", krLayerObj))
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

// SearchKey walks the raw storage-key order (no secondary index) for a key-
// range; used both for typed-doc queries (colon-routing doc-path, KeyRange
// anchor resolves to storageNs) AND for generic KV-pattern passthrough (colon-
// routing KV-path, caller supplies a wildcard pattern on keys that contain
// ':'). The cmd.go searchKeyCommand already handled the 4-branch KEYS-style
// colon-routing before this method is invoked; this method does NOT consult
// the typed-doc registry for KV-pattern forms.
func (db *DB) SearchKey(kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	layerObj, constrained := x.LayerRoutingConstrained(kr, func(k string) any {
		l, ok, lerr := resolveLayer(k)
		if lerr != nil || !ok {
			return fmt.Errorf("SearchKey: key %q is not a concrete key (err=%v constrained=%v)", k, lerr, ok)
		}
		return l
	})
	if !constrained {
		return mo.Err[[]string](errors.New("key range cannot start with wildcard"))
	}
	layer, ok := layerObj.(storageLayer)
	if !ok {
		if lerr, isErr := layerObj.(error); isErr {
			return mo.Err[[]string](lerr)
		}
		return mo.Err[[]string](fmt.Errorf("resolveLayer returned non-storageLayer type %T for key range routing", layerObj))
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

// ——— Raw KV passthrough (Set/SetNX/Get/Delete/Keys) ———
//
// These methods are the thin storage-layer counterparts to the RESP KV
// commands. They implement 0 registry gates: if the caller already routed a
// key with ':' (colon-routing KV-path) it reaches here as a full storage
// key; the typed-doc registry / KeyAttrs / JSON schema checks and the
// internal-namespace write-guard (_doc_/_idx_/_auth_ via validateKVMutationKey)
// are all performed ABOVE this layer (cmd.go handlers + checker.go).

// Set stores value under key (unconditional overwrite). "_m_:" prefix routes
// to the in-memory layer. RESP equivalent: SET.
func (db *DB) Set(key string, value string) error {
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return err
	}
	if !constrained {
		return fmt.Errorf("Set: key %q is a pattern, not a concrete key", key)
	}
	return db.store(layer).Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
}

// SetWithTtl sets key→value with optional positive TTL (zero/negative = no
// TTL, same as Set). autoTTLFromKey injects the doc TTL when the caller
// comes from a typed-doc SET/SETEX/SETNX handler. RESP: SET EX / SETEX.
func (db *DB) SetWithTtl(key string, value string, ttl time.Duration) error {
	ttl = db.autoTTLFromKey(key, ttl)
	opt := setOptionsForTTL(ttl)
	if opt == nil {
		return db.Set(key, value)
	}
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return err
	}
	if !constrained {
		return fmt.Errorf("SetWithTtl: key %q is a pattern, not a concrete key", key)
	}
	return db.store(layer).Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, opt)
		return err
	})
}

// SetNX sets key→value only when key does NOT already exist (Redis SETNX).
// Returns true when the write actually happened.
func (db *DB) SetNX(key string, value string) (bool, error) {
	return db.SetNXWithTtl(key, value, 0)
}

// SetNXWithTtl is SetNX with a positive TTL applied to newly-created keys.
func (db *DB) SetNXWithTtl(key string, value string, ttl time.Duration) (bool, error) {
	ttl = db.autoTTLFromKey(key, ttl)
	var set bool
	opt := setOptionsForTTL(ttl)
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return false, err
	}
	if !constrained {
		return false, fmt.Errorf("SetNXWithTtl: key %q is a pattern, not a concrete key", key)
	}
	err = db.store(layer).Update(func(tx *buntdb.Tx) error {
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

// Get returns the value stored under key. Missing key = Err result. RESP: GET.
func (db *DB) Get(key string) mo.Result[string] {
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return mo.Err[string](err)
	}
	if !constrained {
		return mo.Err[string](fmt.Errorf("Get: key %q is a pattern, not a concrete key", key))
	}
	var val string
	err = db.store(layer).View(func(tx *buntdb.Tx) error {
		var innerErr error
		val, innerErr = tx.Get(key)
		return innerErr
	})

	if err != nil {
		return mo.Err[string](err)
	}
	return mo.Ok(val)
}

// Delete removes key from its routed layer. Returns true if the key existed.
// RESP equivalent: DEL single-key form. Multi-key DEL, doc-path DEL ns
// <pk1> [pk2…] and the "multi-KV DEL not allowed" guard are all enforced
// above this layer in cmd.go delCommand.
func (db *DB) Delete(key string) (bool, error) {
	layer, constrained, err := resolveLayer(key)
	if err != nil {
		return false, err
	}
	if !constrained {
		return false, fmt.Errorf("Delete: key %q is a pattern, not a concrete key", key)
	}
	var val string
	err = db.store(layer).Update(func(tx *buntdb.Tx) error {
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

// Keys returns storage keys matching a single-layer pinned pattern. Pattern
// cannot start with wildcard (resolveLayer enforces single layer).
// The cmd.go keysCommand 4-branch colon-routing logic sits above this call:
// ':' in pattern → KV-glob straight through; no ':' + bare registered ns →
// auto-scoped `<ns>:*`; no ':' + wildcard → ERR. RESP: KEYS.
func (db *DB) Keys(keyPattern string) mo.Result[[]string] {
	layer, constrained, err := resolveLayer(keyPattern)
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

// ——— Private (unexported) helpers ———

func generateBootstrapAuth() (appAuth, ctrlAuth string, err error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", "", fmt.Errorf("generateBootstrapAuth: crypto/rand: %w", err)
	}
	appAuth = hex.EncodeToString(raw)
	if _, err := rand.Read(raw); err != nil {
		return "", "", fmt.Errorf("generateBootstrapAuth: crypto/rand ctrl: %w", err)
	}
	ctrlAuth = hex.EncodeToString(raw)
	// Guarantee distinctness (crypto-lottery collision probability is ~2^-64
	// per run; enforce to satisfy the dual-port "different passwords" hard
	// gate in Config.validate, which runs BEFORE BootstrapAuth writes back).
	if appAuth == ctrlAuth {
		// Flip the first byte of the ctrl copy.
		if raw[0] == 0xFF {
			raw[0] = 0x00
		} else {
			raw[0]++
		}
		ctrlAuth = hex.EncodeToString(raw)
	}
	return appAuth, ctrlAuth, nil
}

// ——— Storage routing helpers ———

// hasLeadingWildcard reports a pattern is unbounded (starts with * or ?).
// Unconstrained patterns are treated as "unpinned to any single layer".
func hasLeadingWildcard(keyPattern string) bool {
	return keyPattern != "" && (keyPattern[0] == '*' || keyPattern[0] == '?')
}

// resolveLayer is the single source of truth for storage-layer routing.
// It unifies the previous layerForKey / layerForStorageNs / resolvePatternLayer
// / isMemKey helpers. Callers ALWAYS handle all three returns explicitly:
//   - s == ""                 → (disk, false, err)
//   - s starts with '*'/'?'   → (disk, false, nil)    // pattern is unconstrained
//   - otherwise ("_m_:" pref) → (mem,  true,  nil)
//   - otherwise (flat ns/key) → (disk, true,  nil)
func resolveLayer(s string) (storageLayer, bool, error) {
	if s == "" {
		return storageDisk, false, errors.New("storage key or pattern is required")
	}
	if hasLeadingWildcard(s) {
		return storageDisk, false, nil
	}
	if naming.HasMemPrefix(s) {
		return storageMem, true, nil
	}
	return storageDisk, true, nil
}

// store returns the BuntDB handle for the given layer (disk XOR mem).
func (db *DB) store(layer storageLayer) *buntdb.DB {
	if layer == storageMem {
		return db.mem
	}
	return db.disk
}

// setOptionsForTTL returns nil for TTL<=0 (persist forever) or a buntdb
// SetOptions instance that expires after the provided TTL.
func setOptionsForTTL(ttl time.Duration) *buntdb.SetOptions {
	if ttl <= 0 {
		return nil
	}
	return &buntdb.SetOptions{Expires: true, TTL: ttl}
}

// openDB constructs a two-layer DB instance (disk file + volatile in-memory
// layer for "_m_:" prefixed keys). Note that the "_m_:<ns>" prefix itself is
// reserved and rejected by HasUnderscorePrefix in the checker layer (above the
// storage layer) — the store simply routes; the semantic gate lives in
// checker.go / cmd.go handlers.
//
// AUTH lifecycle is owned 1:1 by the pair of physical keys `_auth_:app` and
// `_auth_:ctrl` (SSoT lives in the storage layer itself, NOT in-process).
// First-read of the pair via `BootstrapAuth(seedApp, seedCtrl)` generates and
// SETNX-atomic-writes both keys iff both slots are empty; there is no
// per-process duplicate state on *DB and no file-creation-time dependence on
// this constructor.
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
