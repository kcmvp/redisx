package internal

import "crypto/rand"

var (
	internalAuthKey = rand.Text()
)

func AuthKey() string {
	return internalAuthKey
}
