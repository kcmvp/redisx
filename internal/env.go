package internal

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
)

const MemKeyPrefix = "_m_"

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

func MemKey(key string) string {
	if strings.HasPrefix(key, MemKeyPrefix) {
		return key
	}
	return MemKeyPrefix + key
}
