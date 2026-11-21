package client

import (
	"errors"

	clienterrs "github.com/kcmvp/redisx/client/internal/errs"
)

var (
	ErrNotFound               = clienterrs.ErrNotFound
	ErrNotConnected           = clienterrs.ErrNotConnected
	ErrAuthFailed             = clienterrs.ErrAuthFailed
	ErrNoAuth                 = clienterrs.ErrNoAuth
	ErrNoPrivilege            = clienterrs.ErrNoPrivilege
	ErrNamespaceNotRegistered = clienterrs.ErrNamespaceNotRegistered
	ErrIndexNotFound          = clienterrs.ErrIndexNotFound
	ErrAuthKeyEmpty           = clienterrs.ErrAuthKeyEmpty
	ErrKeyRangeRequired       = clienterrs.ErrKeyRangeRequired
	ErrIndexNameRequired      = clienterrs.ErrIndexNameRequired
	ErrNoUpdateValues         = clienterrs.ErrNoUpdateValues
	ErrTopicEmpty             = clienterrs.ErrTopicEmpty
	ErrDuplicatedHandler      = clienterrs.ErrDuplicatedHandler
	ErrSubscriptionStopped    = clienterrs.ErrSubscriptionStopped
	ErrSubscribeAckTimeout    = clienterrs.ErrSubscribeAckTimeout
)

func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func IsNotConnected(err error) bool {
	return errors.Is(err, ErrNotConnected)
}

func IsAuthFailed(err error) bool {
	return errors.Is(err, ErrAuthFailed)
}

func IsNoAuth(err error) bool {
	return errors.Is(err, ErrNoAuth)
}

func IsNoPrivilege(err error) bool {
	return errors.Is(err, ErrNoPrivilege)
}
