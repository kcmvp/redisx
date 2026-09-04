# Typed Document Helpers

> ⬅️ [Back to README](../README.md)
> 📖 [Docs index](index.md)
> 🧱 [Architecture & KeyRange convention](architecture.md)
> 🧭 [How-to & examples](howto.md)
> 🪝 [Write Hook Subsystem](write-hooks.md)
> 🔌 [Stream ingest](stream.md)

`redisx` provides a small document contract (`x.Document`) plus typed
helpers on both the client and the embedded server side.

- **Remote mode (RESP)**: generic functions in the `client` package —
  `client.Set[D]`, `client.Get[D]`, `client.SearchKey[D]`, …
- **Embedded mode (in-process)**: methods on `*server.DB` —
  `db.Set`, `db.Get`, `db.SearchKey`, …

This layer exists so callers can work with document-scoped inputs
instead of manually composing storage keys, key patterns, and index
names.

For runnable examples, see [howto.md](howto.md).

## Document contract

A document type is a JSON string alias that describes how one logical
document maps onto storage:

- `Namespace()`: the document namespace, such as `user`
- `KeyAttrs()`: JSON paths used to derive the storage key, such as `id`
- `TTL()`: the default TTL used by typed writes
- `Mem()`: whether the document belongs to the memory-only layer
- `RawJSON()`: the raw JSON payload

For example, if `Namespace() == "user"` and `KeyAttrs() == []string{"id"}`,
then `{"id":"200"}` resolves to the storage key `user:200`.

## Mental model

Typed helpers accept document-scoped values. `redisx` derives
storage-scoped names from `D` for you.

### Single-document APIs

- `client.Get[D]("200")` accepts the document-level key value, not the
  storage key `user:200`
- `client.Set[D](d)` and `client.SetNX[D](d)` derive the storage key
  from `d.RawJSON()` and `d.KeyAttrs()`
- `client.Delete[D](d)` also derives the storage key from the document
  itself; when `KeyAttrs() == []string{"id"}`, a payload like
  `{"id":"200"}` is enough

### Pattern-based APIs

For methods that accept a `KeyRange` (`SearchKey`, `Update`), the range
is one document-scoped sub-range:

- `client.Keys[D]("*")` means `user:*`
- `client.SearchKey[D](x.KeysPattern("*"), filter, false)` searches over
  `user:*`
- `client.Update[D](x.KeysPattern("*"), filter, ...)` updates `user:*`

Typed helpers reject already-prefixed storage patterns such as
`user:*`, because the namespace is already derived from `D`.

### Ordering and the `desc` flag

`SearchKey`, `SearchIndex`, and `All` all accept a final (or first, for
`All`) boolean `desc` parameter controlling sort direction:

| Function | Ordered by | `desc=false` | `desc=true` |
|---|---|---|---|
| `client.SearchKey[D](kr, filter, desc)` | **Primary storage key** (`<ns>:<pk>`) in lexicographic order | Ascending | Descending |
| `client.SearchIndex[D](idx, kr, filter, desc)` | **Indexed field value** (ascending); ties within one value broken by primary storage key lexicographically | Ascending | Descending |
| `client.All[D](desc, filters…)` | **Primary storage key** — `All` is pure sugar over `SearchKey` with `kr = x.KeysPattern("*")` | Ascending | Descending |

Example:

```go
// Every user, primary-key ascending (same as SearchKey(kr, nil, false))
users := client.All[UserDoc](false).MustGet()

// Every user, primary-key descending — NEW with the v2 All signature
usersDesc := client.All[UserDoc](true).MustGet()
```

### Indexed search

`client.SearchIndex[D]` applies the same document-scoped rule to both
inputs:

- `idxName` is one logical name such as `age`, not one runtime name
  such as `user:age`
- `keyRange` is one document-scoped range such as
  `x.KeysPattern("*")`, not one full storage pattern such as
  `x.KeysPattern("user:*")`

### TTL behavior

- `client.Set[D](d)` and `client.SetNX[D](d)` write with `d.TTL()`
- `client.Update[D](...)` preserves an existing key TTL

## Embedded mode

When you have a `*server.DB` (returned by `server.Start`), you can call
its methods directly:

```go
db := server.Start(UserDoc(""))

_ = db.Set("user:200", `{"id":"200","name":"Test","age":30}`)
got := db.Get("user:200").MustGet()
idx := db.SearchIndex("user:age", x.KeysPattern("user:*"), x.Gte("age", 18), false)
```

The embedded `*server.DB` methods use full storage keys and full index
names — they do **not** auto-scope like the `client` generic functions.
Use the `client` package when you want document-scoped inputs; use
`*server.DB` when you already have full keys.
