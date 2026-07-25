package internal

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
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
