package internal

import "crypto/rand"

var (
	internalAuthKey = rand.Text()
)

const (
	RespxAuthKeyEnv = "respx.auth_key"
)

func AuthKey() string {
	return internalAuthKey
}
