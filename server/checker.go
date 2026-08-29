package server

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/tidwall/gjson"
)

type argShape uint8

const (
	argShapeKV argShape = iota
	argShapeDoc
)

type lookedUpDoc struct {
	Spec      docSpec
	StorageNs string
}

type derivedKey struct {
	FullStorageKey string
	PKSuffix       string
	DocJSON        string
}

func classifyArg1(arg1 string) argShape {
	if strings.Contains(arg1, naming.StorageKeySeparator()) {
		return argShapeKV
	}
	return argShapeDoc
}

func deriveDocKey(spec docSpec, storageNs string, docJSON string) (derivedKey, error) {
	var zero derivedKey
	if !gjson.Valid(docJSON) {
		return zero, errors.New("invalid document json format")
	}
	root := gjson.Parse(docJSON)
	if !root.IsObject() {
		return zero, errors.New("document must be a json object")
	}
	attrs := spec.KeyAttrs
	if len(attrs) == 0 {
		return derivedKey{
			FullStorageKey: naming.BuildStorageKey(storageNs, ""),
			PKSuffix:       "",
			DocJSON:        docJSON,
		}, nil
	}
	parts := make([]string, 0, len(attrs))
	for _, p := range attrs {
		r := gjson.Get(docJSON, p)
		if !r.Exists() {
			return zero, fmt.Errorf("missing key attr: %s", p)
		}
		parts = append(parts, keyAttrString(r))
	}
	pkSuffix := naming.JoinPKAttrValues(parts)
	if strings.Contains(pkSuffix, naming.StorageKeySeparator()) {
		return zero, fmt.Errorf("pk suffix must not contain %q after join; got %q", naming.StorageKeySeparator(), pkSuffix)
	}
	return derivedKey{
		FullStorageKey: naming.BuildStorageKey(storageNs, pkSuffix),
		PKSuffix:       pkSuffix,
		DocJSON:        docJSON,
	}, nil
}

func validatePKSuffixNoColon(suffix string) error {
	if strings.Contains(suffix, naming.StorageKeySeparator()) {
		return fmt.Errorf("pk suffix must not contain namespace separator %q", naming.StorageKeySeparator())
	}
	return nil
}

func extractStorageNsHead(full string) (string, error) {
	sep := naming.StorageKeySeparator()[0]
	first := strings.IndexByte(full, sep)
	if first < 0 {
		return "", fmt.Errorf("missing namespace separator %q in %q", naming.StorageKeySeparator(), full)
	}
	if full[:first] != naming.MemNsPrefix() {
		return full[:first], nil
	}
	second := strings.IndexByte(full[first+1:], sep)
	if second < 0 {
		return "", fmt.Errorf("key %q starts with %q but missing second namespace separator", full, naming.MemNsPrefix())
	}
	return full[:first+1+second], nil
}

func validateKVFullKey(fullKey string) error {
	if !strings.Contains(fullKey, naming.StorageKeySeparator()) {
		return fmt.Errorf("kv-pattern key must contain namespace separator %q", naming.StorageKeySeparator())
	}
	if naming.HasUnderscorePrefix(fullKey) {
		head, err := extractStorageNsHead(fullKey)
		if err != nil {
			return err
		}
		if !naming.IsInternalStorageNs(head) {
			return fmt.Errorf("kv-pattern key must not start with reserved internal prefix %q", head)
		}
	}
	return nil
}

func validateKVMutationKey(fullKey string) error {
	if err := validateKVFullKey(fullKey); err != nil {
		return err
	}
	head, err := extractStorageNsHead(fullKey)
	if err != nil {
		return err
	}
	if naming.IsInternalStorageNs(head) {
		return fmt.Errorf("internal storage namespace %q is managed exclusively via dedicated registry commands (REGSCH/DROPSCH/REGIDX/DROPIDX/AUTH); direct KV mutation via SET/SETEX/SETNX/DEL is forbidden", head)
	}
	return nil
}

func storageNsFromKRAnchor(anchor string) (string, error) {
	if anchor == "" {
		return "", errors.New("key range anchor is empty — cannot resolve namespace")
	}
	if hasLeadingWildcard(anchor) {
		return "", errors.New("key range anchor cannot start with wildcard")
	}
	head := anchor
	if p := strings.IndexByte(anchor, '|'); p >= 0 {
		head = anchor[:p]
	}
	ns, err := extractStorageNsHead(head)
	if err != nil {
		return "", fmt.Errorf("key range anchor %q — %w", anchor, err)
	}
	if naming.IsInternalStorageNs(ns) {
		return "", nil
	}
	return ns, nil
}

func keyAttrString(r gjson.Result) string {
	switch r.Type {
	case gjson.True:
		return "1"
	case gjson.False:
		return "0"
	}
	return r.String()
}

func (db *DB) lookupDocByLogicalOrStorageNs(keyOrNs string) (lookedUpDoc, bool) {
	db.docRegMu.Lock()
	defer db.docRegMu.Unlock()
	if strings.Contains(keyOrNs, naming.StorageKeySeparator()) {
		ns, _, err := naming.SplitStorageKey(keyOrNs)
		if err != nil {
			ns = keyOrNs
		}
		spec, ok := db.docRegSpec[ns]
		if !ok {
			return lookedUpDoc{}, false
		}
		return lookedUpDoc{Spec: spec, StorageNs: ns}, true
	}
	for storageNs, spec := range db.docRegSpec {
		if spec.Namespace == keyOrNs {
			return lookedUpDoc{Spec: spec, StorageNs: storageNs}, true
		}
		mem, okMem := naming.StripMemPrefixIfMem(storageNs)
		if okMem && mem == keyOrNs {
			return lookedUpDoc{Spec: spec, StorageNs: storageNs}, true
		}
	}
	return lookedUpDoc{}, false
}

func (db *DB) lookupDocByStorageNs(storageNs string) (lookedUpDoc, bool) {
	db.docRegMu.Lock()
	defer db.docRegMu.Unlock()
	spec, ok := db.docRegSpec[storageNs]
	if !ok {
		return lookedUpDoc{}, false
	}
	return lookedUpDoc{Spec: spec, StorageNs: storageNs}, true
}

func (db *DB) autoTTLFromKey(fullStorageKey string, explicit time.Duration) time.Duration {
	if explicit > 0 {
		return explicit
	}
	ns, _, err := naming.SplitStorageKey(fullStorageKey)
	if err != nil {
		return 0
	}
	if naming.IsInternalStorageNs(ns) {
		return 0
	}
	doc, ok := db.lookupDocByStorageNs(ns)
	if !ok {
		return 0
	}
	return doc.Spec.TTL
}
