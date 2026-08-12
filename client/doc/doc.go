package doc

import (
	"time"

	"github.com/kcmvp/redisx/client"
	"github.com/kcmvp/redisx/internal"
	"github.com/kcmvp/redisx/x"
	"github.com/samber/mo"
)

func AddAbortHook(h func(key string, valueJSON []byte) error) client.HookID {
	return client.AddAbortHook(client.AbortHook(h))
}

func AddTransformHook(h func(key string, valueJSON []byte) ([]byte, error)) client.HookID {
	return client.AddTransformHook(client.TransformHook(h))
}

func AddObserverHook(h func(key string, valueJSON []byte)) client.HookID {
	return client.AddObserverHook(client.ObserverHook(h))
}

func AddObserverAfterHook(h func(key string, valueJSON []byte, writeErr error)) client.HookID {
	return client.AddObserverAfterHook(client.ObserverAfterHook(h))
}

func RemoveHook(id client.HookID) {
	client.RemoveHook(id)
}

func SetHookTimeout(d time.Duration) {
	client.SetHookTimeout(d)
}

// Get retrieves one document by its document-level key value.
//
// The input key is the resolved JSON-layer key value, not the final storage
// key. The storage namespace is derived from D automatically.
func Get[D x.Document](key string) (D, error) {
	var zero D
	raw, err := client.Get(x.StorageKeyValue[D](key))
	if err != nil {
		return zero, err
	}
	return D(raw), nil
}

// Set stores the document raw JSON using the key resolved from the document
// itself, with the document TTL.
func Set[D x.Document](d D) error {
	key, err := x.StorageKey(d)
	if err != nil {
		return err
	}
	return client.SetWithTTL(key, d.RawJSON(), d.TTL())
}

// SetNX stores the document raw JSON only when its resolved storage key does
// not already exist, with the document TTL.
func SetNX[D x.Document](d D) (bool, error) {
	key, err := x.StorageKey(d)
	if err != nil {
		return false, err
	}
	return client.SetNXWithTTL(key, d.RawJSON(), d.TTL())
}

// Delete removes the document resolved from its key attributes.
func Delete[D x.Document](d D) (bool, error) {
	key, err := x.StorageKey(d)
	if err != nil {
		return false, err
	}
	return client.Delete(key)
}

// Keys returns all keys matching the document type prefix and sub-pattern.
func Keys[D x.Document](keyPattern string) mo.Result[[]string] {
	fullKeyPattern, err := internal.ValidateKeyPattern[D](keyPattern)
	if err != nil {
		return mo.Err[[]string](err)
	}
	return client.Keys(fullKeyPattern)
}

// SearchIndex executes SEARCHINDEX using one logical document index name,
// one document-scoped key pattern, and one optional JSON filter.
//
// In effect, the result set is narrowed in two dimensions:
//   - keyPattern limits which document keys are considered
//   - filter limits which JSON documents match within that key range
//
// The logical idxName is resolved into the internal full index name from D,
// while keyPattern is automatically prefixed with the document storage
// namespace derived from D. idxName must therefore be one logical index name,
// not an already-prefixed full index name such as "user_age". keyPattern must
// likewise be a document-scoped sub-pattern, not an already-prefixed storage
// pattern such as "user:*".
func SearchIndex[D x.Document](idxName string, keyPattern string, filter x.Filter, desc bool) mo.Result[[]D] {
	fullIdxName, err := internal.ValidateIdxName[D](idxName)
	if err != nil {
		return mo.Err[[]D](err)
	}

	fullKeyPattern, err := internal.ValidateKeyPattern[D](keyPattern)
	if err != nil {
		return mo.Err[[]D](err)
	}

	res := client.SearchIndex(fullIdxName, fullKeyPattern, filter, desc)
	if res.IsError() {
		return mo.Err[[]D](res.Error())
	}

	raws := res.MustGet()
	out := make([]D, 0, len(raws))
	for _, raw := range raws {
		out = append(out, D(raw))
	}
	return mo.Ok(out)
}

// SearchKey executes SEARCHKEY using the document type prefix and sub-pattern.
func SearchKey[D x.Document](keyPattern string, filter x.Filter, desc bool) mo.Result[[]D] {
	fullKeyPattern, err := internal.ValidateKeyPattern[D](keyPattern)
	if err != nil {
		return mo.Err[[]D](err)
	}

	res := client.SearchKey(fullKeyPattern, filter, desc)
	if res.IsError() {
		return mo.Err[[]D](res.Error())
	}

	raws := res.MustGet()
	out := make([]D, 0, len(raws))
	for _, raw := range raws {
		out = append(out, D(raw))
	}
	return mo.Ok(out)
}

// Update executes UPDATE using the document type prefix and sub-pattern.
func Update[D x.Document](keyPattern string, filter x.Filter, values ...x.Mutation) mo.Result[[]string] {
	fullKeyPattern, err := internal.ValidateKeyPattern[D](keyPattern)
	if err != nil {
		return mo.Err[[]string](err)
	}
	return client.Update(fullKeyPattern, filter, values...)
}
