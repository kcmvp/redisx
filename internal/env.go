package internal

import (
	"crypto/rand"
	"encoding/hex"
)

const (
	RespxAuthKeyEnv = "RESPX_AUTH_KEY"
	XCmdQuery       = "query"
)

func AuthKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
