package respconn

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Options struct {
	Host         string
	Port         int
	Auth         string
	TimeoutMs    int
	AuthOptional bool
	ReadTimeout  time.Duration
	OnConnect    func(ctx context.Context, cn *redis.Conn) error
}

type DialResult struct {
	Client  *redis.Client
	Timeout time.Duration
}

func Defaults(o Options) Options {
	if o.Host == "" {
		o.Host = "127.0.0.1"
	}
	if o.Port == 0 {
		o.Port = 7381
	}
	if o.TimeoutMs <= 0 {
		o.TimeoutMs = 3000
	}
	return o
}

// respProtocol pins go-redis to RESP2.
//
// RedisX's encoder (tidwall/redcon) can only emit RESP2 replies — it has no
// RESP3 types (%, ~, |, >). Leaving Protocol unset would let go-redis normalise
// 0 -> 3 and negotiate RESP3 via `HELLO 3`, after which it would wait for reply
// and push frame shapes the server cannot produce. (go-redis does fall back to
// RESP2 when HELLO fails, which is why this stayed hidden for so long.)
const respProtocol = 2

func DialAndHandshake(o Options) (*DialResult, error) {
	o = Defaults(o)
	to := time.Duration(o.TimeoutMs) * time.Millisecond
	readTimeout := o.ReadTimeout
	if readTimeout == 0 {
		readTimeout = to
	}
	// Credentials are handed to go-redis itself (Password, no Username) rather
	// than being sent from an OnConnect hook. initConn then runs
	// `HELLO 2 AUTH default <key>` as the connection's very first frame, so the
	// connection is authenticated before anything else is written. The previous
	// OnConnect form put AUTH AFTER go-redis' own unauthenticated HELLO, which a
	// RedisX server answered with NOAUTH — one warning per newly pooled
	// connection, plus helloOK=false.
	//
	// With Username empty, the legacy fallback (used only against servers that
	// reject HELLO) stays the single-argument `AUTH <key>`, so old RedisX
	// servers remain reachable.
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", o.Host, o.Port),
		DB:           0,
		DialTimeout:  to,
		ReadTimeout:  readTimeout,
		WriteTimeout: to,
		Password:     o.Auth,
		Protocol:     respProtocol,
		OnConnect:    o.OnConnect,
	})
	// Ping doubles as the reachability probe: it runs through initConn (hence
	// HELLO ... AUTH + CLIENT SETINFO) and is answered by every RedisX port.
	probeCtx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	if err := client.Ping(probeCtx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &DialResult{
		Client:  client,
		Timeout: to,
	}, nil
}
