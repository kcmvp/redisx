package internal

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/kcmvp/redisx/x"
	"github.com/kcmvp/redisx/x/contract"
)

var (
	authKeyOnce sync.Once
	authKey     string
)

func AuthKey() string {
	authKeyOnce.Do(func() {
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		authKey = hex.EncodeToString(b)
	})

	return authKey
}

// RawDocument is the shared typed-document constraint used by internal helpers.
// It stays inside the module to avoid leaking helper-level generic constraints
// into the public x contract surface.
type RawDocument interface {
	x.Document
	~string
}

// ValidateKeyPattern resolves one document-scoped key pattern into its full
// storage pattern. It rejects already-prefixed storage patterns because the
// document type itself already determines the namespace prefix.
func ValidateKeyPattern[D RawDocument](keyPattern string) (string, error) {
	fullNamespace := x.StorageKeyValue[D]("")
	fullPrefix := fullNamespace + contract.StorageKeySeparator
	if keyPattern == fullNamespace || strings.HasPrefix(keyPattern, fullPrefix) {
		return "", fmt.Errorf("key pattern must be document-scoped, got storage pattern: %s", keyPattern)
	}
	return x.StorageKeyValue[D](keyPattern), nil
}

// ValidateIdxName resolves one logical document index name into its full
// runtime name. It rejects already-prefixed index names because the document
// type itself already determines the namespace prefix.
func ValidateIdxName[D RawDocument](idxName string) (string, error) {
	if idxName == "" {
		return "", fmt.Errorf("index name is required")
	}

	fullNamespace := strings.ToLower(x.StorageKeyValue[D](""))
	lowerName := strings.ToLower(idxName)
	if strings.HasPrefix(lowerName, fullNamespace+"_") {
		return "", fmt.Errorf("idx name must be logical, got fully-qualified index name: %s", idxName)
	}
	return x.IdxFullName[D](idxName), nil
}
