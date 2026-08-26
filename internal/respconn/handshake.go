package respconn

import (
	"context"
	"fmt"
	"strings"
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
	Protocol     int
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

func DialAndHandshake(o Options) (*DialResult, error) {
	o = Defaults(o)
	to := time.Duration(o.TimeoutMs) * time.Millisecond
	readTimeout := o.ReadTimeout
	if readTimeout == 0 {
		readTimeout = to
	}
	onConnect := o.OnConnect
	if o.Auth != "" {
		inner := onConnect
		onConnect = func(ctx context.Context, cn *redis.Conn) error {
			if err := cn.Do(ctx, "AUTH", o.Auth).Err(); err != nil {
				return err
			}
			if inner != nil {
				return inner(ctx, cn)
			}
			return nil
		}
	}
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", o.Host, o.Port),
		DB:           0,
		DialTimeout:  to,
		ReadTimeout:  readTimeout,
		WriteTimeout: to,
		Protocol:     o.Protocol,
		OnConnect:    onConnect,
	})
	probeCtx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	raw := client.Do(probeCtx, "HELLO", 3)
	if raw.Err() != nil {
		helloErr := raw.Err()
		if !strings.Contains(helloErr.Error(), "NOAUTH") {
			pingCtx, cancelPing := context.WithTimeout(context.Background(), to)
			defer cancelPing()
			if pingErr := client.Ping(pingCtx).Err(); pingErr != nil {
				_ = client.Close()
				return nil, pingErr
			}
		} else {
			_ = client.Close()
			return nil, helloErr
		}
	}
	return &DialResult{
		Client:  client,
		Timeout: to,
	}, nil
}
