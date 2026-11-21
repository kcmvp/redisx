package errs

import (
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

var (
	ErrNotFound               = errors.New("redisx client: not found")
	ErrNotConnected           = errors.New("redisx client: not connected")
	ErrAuthFailed             = errors.New("redisx client: authentication failed")
	ErrNoAuth                 = errors.New("redisx client: authentication required")
	ErrNoPrivilege            = errors.New("redisx client: no privilege")
	ErrNamespaceNotRegistered = errors.New("redisx client: namespace not registered")
	ErrIndexNotFound          = errors.New("redisx client: index not found")

	ErrAuthKeyEmpty        = errors.New("redisx client: auth key is empty")
	ErrKeyRangeRequired    = errors.New("redisx client: key range is required")
	ErrIndexNameRequired   = errors.New("redisx client: index name is required")
	ErrNoUpdateValues      = errors.New("redisx client: no update values provided")
	ErrTopicEmpty          = errors.New("redisx client: topic is empty")
	ErrDuplicatedHandler   = errors.New("redisx client: duplicated handler")
	ErrSubscriptionStopped = errors.New("redisx client: subscription stopped before acknowledgement")
	ErrSubscribeAckTimeout = errors.New("redisx client: subscription acknowledgement timeout")
)

type classifiedError struct {
	kind  error
	cause error
}

func (e *classifiedError) Error() string {
	return e.cause.Error()
}

func (e *classifiedError) Unwrap() error {
	return e.cause
}

func (e *classifiedError) Is(target error) bool {
	return target == e.kind || errors.Is(e.cause, target)
}

func New(kind error, message string, args ...any) error {
	return &classifiedError{
		kind:  kind,
		cause: fmt.Errorf(message, args...),
	}
}

func Wrap(kind error, cause error) error {
	if cause == nil {
		return nil
	}
	if errors.Is(cause, kind) {
		return cause
	}
	return &classifiedError{
		kind:  kind,
		cause: cause,
	}
}

func Normalize(err error) error {
	if err == nil {
		return nil
	}
	for _, kind := range []error{
		ErrNotFound,
		ErrNotConnected,
		ErrAuthFailed,
		ErrNoAuth,
		ErrNoPrivilege,
		ErrNamespaceNotRegistered,
		ErrIndexNotFound,
		ErrAuthKeyEmpty,
		ErrKeyRangeRequired,
		ErrIndexNameRequired,
		ErrNoUpdateValues,
		ErrTopicEmpty,
		ErrDuplicatedHandler,
		ErrSubscriptionStopped,
		ErrSubscribeAckTimeout,
	} {
		if errors.Is(err, kind) {
			return err
		}
	}

	msg := err.Error()
	switch {
	case errors.Is(err, redis.Nil):
		return Wrap(ErrNotFound, err)
	case errors.Is(err, redis.ErrClosed), msg == "resp client is not connected":
		return Wrap(ErrNotConnected, err)
	case strings.Contains(msg, "NOAUTH"):
		return Wrap(ErrNoAuth, err)
	case strings.Contains(msg, "WRONGPASS"), strings.Contains(msg, "ERR authentication failed"):
		return Wrap(ErrAuthFailed, err)
	case strings.Contains(msg, "No Privilege"):
		return Wrap(ErrNoPrivilege, err)
	case strings.Contains(msg, "index not found:"):
		return Wrap(ErrIndexNotFound, err)
	case strings.Contains(msg, "not registered"):
		return Wrap(ErrNamespaceNotRegistered, err)
	default:
		return err
	}
}
