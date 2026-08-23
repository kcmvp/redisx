package respconn

import (
	"context"
	"errors"
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
	Client       *redis.Client
	Timeout      time.Duration
	Capabilities Capabilities
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

func WrapAdminErr(raw error, authProvided bool) error {
	if raw == nil {
		return nil
	}
	msg := raw.Error()
	switch {
	case strings.Contains(msg, "NOAUTH"):
		if authProvided {
			return fmt.Errorf("admin-port still returned NOAUTH after AUTH attempt: server has rotated / changed admin-auth key? %w", raw)
		}
		return errors.New("connect redisx admin-port failed: server admin-port requires AUTH. Pass the admin-auth key via `-a <ADMIN_AUTH_KEY>` (long form: `--admin-auth <ADMIN_AUTH_KEY>`). If the server was started WITHOUT admin-auth (dev-only), no `-a` flag is needed.")
	case strings.Contains(msg, "WRONGPASS"):
		return fmt.Errorf("connect redisx admin-port failed: AUTH key rejected (WRONGPASS). The key passed via `-a / --admin-auth` does not match the server's `--admin-auth` key; check for trailing whitespace. %w", raw)
	case strings.Contains(msg, "ERR authentication failed"):
		return fmt.Errorf("connect redisx admin-port failed: AUTH failed (server ERR authentication failed). Double-check `-a / --admin-auth` matches the server's `--admin-auth` startup value. %w", raw)
	}
	return fmt.Errorf("connect redisx admin-port failed: %w", raw)
}

func DialAndHandshake(o Options) (*DialResult, error) {
	o = Defaults(o)
	to := time.Duration(o.TimeoutMs) * time.Millisecond
	readTimeout := o.ReadTimeout
	if readTimeout == 0 {
		readTimeout = to
	}
	authProvided := o.Auth != ""
	onConnect := o.OnConnect
	if authProvided {
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
	var firstCaps Capabilities
	if raw.Err() == nil {
		if v, err := raw.Result(); err == nil {
			firstCaps = ParseHelloCapabilities(v)
		}
	} else {
		pingCtx, cancelPing := context.WithTimeout(context.Background(), to)
		defer cancelPing()
		if pingErr := client.Ping(pingCtx).Err(); pingErr != nil {
			_ = client.Close()
			return nil, WrapAdminErr(pingErr, authProvided)
		}
	}
	caps := ProbeCapabilities(context.Background(), client, to)
	if caps.IsRedisx {
		return &DialResult{
			Client:       client,
			Timeout:      to,
			Capabilities: caps,
		}, nil
	}
	if !firstCaps.IsRedisx && firstCaps.ServerVer != "" {
		caps = firstCaps
	}
	return &DialResult{
		Client:       client,
		Timeout:      to,
		Capabilities: caps,
	}, nil
}
