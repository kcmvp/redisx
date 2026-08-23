package session

import (
	"context"
	"errors"
	"time"

	"github.com/kcmvp/redisx/internal/respconn"
	"github.com/redis/go-redis/v9"
)

type Capabilities = respconn.Capabilities

type Options struct {
	Host      string
	Port      int
	AdminAuth string
	TimeoutMs int
}

type Session struct {
	client       *redis.Client
	ctx          context.Context
	timeout      time.Duration
	capabilities Capabilities
}

type Cache struct {
	DocNs     []string
	IdxByDoc  map[string][]string
	FetchedAt time.Time
}

func NewCache() *Cache {
	return &Cache{IdxByDoc: map[string][]string{}}
}

func (c *Cache) Invalidate() {
	c.DocNs = nil
	c.IdxByDoc = map[string][]string{}
	c.FetchedAt = time.Time{}
}

var ErrAdminAuthRequired = errors.New("server admin-port requires AUTH (admin-auth key is set on the server side)")

func WrapAdminErr(raw error, authProvided bool) error {
	return respconn.WrapAdminErr(raw, authProvided)
}

var newInternal = func(opts Options) (*Session, error) {
	res, err := respconn.DialAndHandshake(respconn.Options{
		Host:         opts.Host,
		Port:         opts.Port,
		Auth:         opts.AdminAuth,
		TimeoutMs:    opts.TimeoutMs,
		AuthOptional: true,
	})
	if err != nil {
		return nil, err
	}
	return &Session{
		client:       res.Client,
		ctx:          context.Background(),
		timeout:      res.Timeout,
		capabilities: res.Capabilities,
	}, nil
}

func New(opts Options) (*Session, error) {
	return newInternal(opts)
}

func (s *Session) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *Session) Client() *redis.Client  { return s.client }
func (s *Session) Timeout() time.Duration { return s.timeout }
func (s *Session) Ctx() context.Context   { return s.ctx }
func (s *Session) Capabilities() Capabilities {
	if s == nil {
		return Capabilities{}
	}
	return s.capabilities
}

func (s *Session) SetCapabilitiesForTest(c Capabilities) {
	s.capabilities = c
}

func SetNewForTest(fn func(Options) (*Session, error)) func(Options) (*Session, error) {
	prev := newInternal
	if fn != nil {
		newInternal = fn
	} else {
		newInternal = prev
	}
	return prev
}

func (s *Session) RawDo(args []any) *redis.Cmd {
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	to := s.timeout
	if to <= 0 {
		to = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	if s.client == nil {
		return &redis.Cmd{}
	}
	return s.client.Do(ctx, args...)
}

func (s *Session) RefreshCapabilities() (previous Capabilities, now Capabilities, ok bool) {
	if s == nil || s.client == nil {
		return Capabilities{}, Capabilities{}, false
	}
	previous = s.capabilities
	c := respconn.ProbeCapabilitiesWithRetry(s.ctx, s.client, s.timeout)
	if !c.IsRedisx {
		return previous, previous, false
	}
	s.capabilities = c
	return previous, c, true
}
