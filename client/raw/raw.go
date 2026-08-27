package raw

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/kcmvp/redisx/client/internal/conn"
	"github.com/kcmvp/redisx/client/internal/hook"
	"github.com/kcmvp/redisx/internal/proto"
	"github.com/kcmvp/redisx/x"
	"github.com/samber/mo"
)

const dialTimeout = conn.DialTimeout

func Get(key string) (string, error) {
	if key == "" {
		return "", nil
	}
	client := conn.GetSharedClient()
	if client == nil {
		return "", errors.New("resp client is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	val, err := client.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return val, nil
}

func SetWithTTL(key, value string, ttl time.Duration) error {
	if key == "" {
		return nil
	}
	client := conn.GetSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}
	reg := hook.Snapshot()
	finalVal := value
	var valBytes []byte
	if reg != nil {
		valBytes = []byte(value)
		transformed, herr := hook.RunBefore(reg, key, valBytes)
		if herr != nil {
			return herr
		}
		finalVal = string(transformed)
		valBytes = transformed
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	werr := client.Set(ctx, key, finalVal, ttl).Err()
	committed := werr == nil
	if reg != nil {
		hook.RunAfter(reg, key, valBytes, committed, werr)
	}
	return werr
}

func Set(key, value string) error {
	return SetWithTTL(key, value, 0)
}

func SetNX(key, value string) (bool, error) {
	if key == "" {
		return false, nil
	}
	client := conn.GetSharedClient()
	if client == nil {
		return false, errors.New("resp client is not connected")
	}
	reg := hook.Snapshot()
	finalVal := value
	var valBytes []byte
	if reg != nil {
		valBytes = []byte(value)
		transformed, herr := hook.RunBefore(reg, key, valBytes)
		if herr != nil {
			return false, herr
		}
		finalVal = string(transformed)
		valBytes = transformed
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	res, werr := client.Do(ctx, "SETNX", key, finalVal).Int()
	ok := res == 1
	committed := ok && werr == nil
	if reg != nil {
		hook.RunAfter(reg, key, valBytes, committed, werr)
	}
	return ok, werr
}

func SetNXWithTTL(key, value string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return SetNX(key, value)
	}
	if key == "" {
		return false, nil
	}
	client := conn.GetSharedClient()
	if client == nil {
		return false, errors.New("resp client is not connected")
	}
	reg := hook.Snapshot()
	finalVal := value
	var valBytes []byte
	if reg != nil {
		valBytes = []byte(value)
		transformed, herr := hook.RunBefore(reg, key, valBytes)
		if herr != nil {
			return false, herr
		}
		finalVal = string(transformed)
		valBytes = transformed
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	ok, werr := client.SetNX(ctx, key, finalVal, ttl).Result()
	committed := ok && werr == nil
	if reg != nil {
		hook.RunAfter(reg, key, valBytes, committed, werr)
	}
	return ok, werr
}

func Delete(key string) (bool, error) {
	if key == "" {
		return false, nil
	}
	client := conn.GetSharedClient()
	if client == nil {
		return false, errors.New("resp client is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	res, err := client.Del(ctx, key).Result()
	return res > 0, err
}

func Keys(keyPattern string) mo.Result[[]string] {
	if keyPattern == "" {
		return mo.Ok([]string{})
	}
	client := conn.GetSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	return mo.TupleToResult(client.Keys(ctx, keyPattern).Result())
}

func SearchIndex(indexName string, kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if indexName == "" {
		return mo.Err[[]string](errors.New("index name is required"))
	}
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	client := conn.GetSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	krBytes, krErr := kr.MarshalJSON()
	if krErr != nil {
		return mo.Err[[]string](fmt.Errorf("failed to serialize key range: %w", krErr))
	}
	var filterJSON string
	if filter != nil {
		b, err := filter.MarshalJSON()
		if err != nil {
			return mo.Err[[]string](fmt.Errorf("failed to serialize filter: %w", err))
		}
		filterJSON = string(b)
	} else {
		filterJSON = "{}"
	}
	args := make([]interface{}, 0, 7)
	args = append(args, proto.CmdSearchIndex, indexName, string(krBytes), filterJSON)
	if desc {
		args = append(args, "DESC")
	} else {
		args = append(args, "ASC")
	}
	if lim := kr.GetLimit(); lim != -1 {
		args = append(args, "LIMIT", strconv.Itoa(lim))
	}
	cmd := client.Do(ctx, args...)
	res, err := cmd.StringSlice()
	if err != nil {
		return mo.Err[[]string](err)
	}
	return mo.Ok(res)
}

func SearchKey(kr x.KeyRange, filter x.Filter, desc bool) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	client := conn.GetSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	krBytes, krErr := kr.MarshalJSON()
	if krErr != nil {
		return mo.Err[[]string](fmt.Errorf("failed to serialize key range: %w", krErr))
	}
	var filterJSON string
	if filter != nil {
		b, err := filter.MarshalJSON()
		if err != nil {
			return mo.Err[[]string](fmt.Errorf("failed to serialize filter: %w", err))
		}
		filterJSON = string(b)
	} else {
		filterJSON = "{}"
	}
	args := make([]interface{}, 0, 6)
	args = append(args, proto.CmdSearchKey, string(krBytes), filterJSON)
	if desc {
		args = append(args, "DESC")
	} else {
		args = append(args, "ASC")
	}
	if lim := kr.GetLimit(); lim != -1 {
		args = append(args, "LIMIT", strconv.Itoa(lim))
	}
	cmd := client.Do(ctx, args...)
	res, err := cmd.StringSlice()
	if err != nil {
		return mo.Err[[]string](err)
	}
	return mo.Ok(res)
}

func Update(kr x.KeyRange, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	if kr == nil {
		return mo.Err[[]string](errors.New("key range is required"))
	}
	if len(values) == 0 {
		return mo.Err[[]string](errors.New("no update values provided"))
	}
	client := conn.GetSharedClient()
	if client == nil {
		return mo.Err[[]string](errors.New("resp client is not connected"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	krBytes, krErr := kr.MarshalJSON()
	if krErr != nil {
		return mo.Err[[]string](fmt.Errorf("failed to serialize key range: %w", krErr))
	}
	filterJSON := "{}"
	if filter != nil {
		b, err := filter.MarshalJSON()
		if err != nil {
			return mo.Err[[]string](fmt.Errorf("failed to serialize filter: %w", err))
		}
		filterJSON = string(b)
	}
	updateJSON, err := x.MarshalUpdate(values...)
	if err != nil {
		return mo.Err[[]string](fmt.Errorf("failed to serialize updates: %w", err))
	}
	args := make([]interface{}, 0, 6)
	args = append(args, proto.CmdUpdate, string(krBytes), filterJSON, string(updateJSON))
	if lim := kr.GetLimit(); lim != -1 {
		args = append(args, "LIMIT", strconv.Itoa(lim))
	}
	cmd := client.Do(ctx, args...)
	res, err := cmd.StringSlice()
	if err != nil {
		return mo.Err[[]string](err)
	}
	return mo.Ok(res)
}

func DropSchema(logicalNs string) error {
	if logicalNs == "" {
		return errors.New("logical ns is empty")
	}
	client := conn.GetSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	return client.Do(ctx, proto.CmdDropSchema, logicalNs).Err()
}

func DropIndex(ownerNsOrFull string, logical ...string) error {
	if ownerNsOrFull == "" {
		return errors.New("owner ns or full name is required")
	}
	client := conn.GetSharedClient()
	if client == nil {
		return errors.New("resp client is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	switch len(logical) {
	case 0:
		return client.Do(ctx, proto.CmdDropIndex, ownerNsOrFull).Err()
	case 1:
		return client.Do(ctx, proto.CmdDropIndex, ownerNsOrFull, logical[0]).Err()
	default:
		return errors.New("DropIndex takes at most 2 args: (fullName) or (ownerNs, logical)")
	}
}
