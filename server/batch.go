package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/buntdb"
)

// ——— Atomic batched writes ———
//
// Batch atomicity invariants (setBatchAtomic / deleteBatchAtomic):
//   - All keys in one batch target the SAME storage layer (disk XOR mem),
//     i.e. the caller already ensured a consistent Mem flag across payloads.
//   - The whole batch runs inside a single BuntDB Update tx; any sub-error
//     aborts the tx so no half-written state survives.

// batchedWrite is the per-item tuple used by setBatchAtomic (the underlying
// implementation for SET/SETEX/SETNX when multiple JSON payloads are sent via
// doc-path colon-routing form). TTL=0 means persist forever.
type batchedWrite struct {
	Key   string
	Value string
	TTL   time.Duration
}

var errNxPreconditionFailed = errors.New("setBatchAtomic: nx precondition failed — one or more keys already exist")

// setBatchAtomic writes many (Key, Value, TTL) tuples inside a SINGLE BuntDB
// Update tx so either all tuples commit or none do. Used by cmd.go
// setCommand/setExCommand/setNxCommand for doc-path multi-JSON mode.
//
// Enforced invariants:
//   - One storage layer only (disk XOR mem) — if the batch mixes "_m_:" keys
//     with regular keys the caller gets a "cannot span storage layers" ERR.
//   - No duplicate keys within a single batch (pk collision on doc-path same
//     namespace).
//   - nxMode=true (SETNX): ALL keys must be not-found before this call; any
//     single existing key → errNxPreconditionFailed (SETNX all-or-nothing
//     semantics, cmd.go returns integer count of actually-set keys = 0).
func (db *DB) setBatchAtomic(batch []batchedWrite, nxMode bool) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	layers := make(map[storageLayer][]batchedWrite, 2)
	for _, w := range batch {
		l, constrained, lerr := resolveLayer(w.Key)
		if lerr != nil || !constrained {
			return 0, fmt.Errorf("setBatchAtomic: key %q is not a concrete key (err=%v constrained=%v)", w.Key, lerr, constrained)
		}
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
		if lo.ContainsBy(writes, func(w batchedWrite) bool { return w.Key == "" }) {
			return errors.New("batch: empty key")
		}
		for _, w := range writes {
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

// deleteBatchAtomic deletes many keys inside a single BuntDB Update tx
// (all-or-nothing). Used by cmd.go delCommand for doc-path multi-pk mode:
// `DEL <registered-ns> <pk1> [pk2 …]`.
//
// Same invariants as setBatchAtomic: single storage layer, no duplicate keys
// within the batch. The "KV-path multi-DEL forbidden" guard (argc≥3 +
// classifyArg1(arg1) has ':') is enforced above this in cmd.go delCommand.
func (db *DB) deleteBatchAtomic(keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	layers := make(map[storageLayer][]string, 2)
	for _, k := range keys {
		l, constrained, lerr := resolveLayer(k)
		if lerr != nil || !constrained {
			return 0, fmt.Errorf("deleteBatchAtomic: key %q is not a concrete key (err=%v constrained=%v)", k, lerr, constrained)
		}
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
