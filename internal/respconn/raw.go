package respconn

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func RawDo(ctx context.Context, client *redis.Client, timeout time.Duration, args []any) *redis.Cmd {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return client.Do(callCtx, args...)
}
