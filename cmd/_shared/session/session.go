package session

import (
	"context"
	"time"

	"github.com/kcmvp/redisx/internal/respconn"
	"github.com/redis/go-redis/v9"
)

type Options struct {
	Host      string
	Port      int
	Auth      string
	TimeoutMs int
}

type Session struct {
	client  *redis.Client
	ctx     context.Context
	timeout time.Duration
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

var newInternal = func(opts Options) (*Session, error) {
	res, err := respconn.DialAndHandshake(respconn.Options{
		Host:         opts.Host,
		Port:         opts.Port,
		Auth:         opts.Auth,
		TimeoutMs:    opts.TimeoutMs,
		AuthOptional: true,
	})
	if err != nil {
		return nil, err
	}
	return &Session{
		client:  res.Client,
		ctx:     context.Background(),
		timeout: res.Timeout,
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
