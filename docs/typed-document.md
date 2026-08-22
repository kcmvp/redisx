# Typed Document Helpers

> ⬅️ [Back to README](../README.md)
> 📖 [Docs index](index.md)
> 🧱 [Architecture & KeyRange convention](architecture.md)
> 🧭 [How-to & examples](howto.md)
> 🪝 [Write Hook Subsystem](write-hooks.md)
> 🔌 [Stream ingest](stream.md)

`redisx` provides a small document contract (`x.Document`) plus typed helpers
on both the client and the embedded server side.

- Remote mode (RESP): use `client/doc`
- Embedded mode (in-process DB): use `server.DBX` via `server.As[D]`

This layer exists so callers can work with document-scoped inputs instead of
manually composing storage keys, key patterns, and index names.

The client-side package path is `client/doc`; examples typically import it as
`doc`.

For runnable examples, see [howto.md](howto.md).

## Document contract

A document type is a JSON string alias that describes how one logical document
maps onto storage:

- `Namespace()`: the document namespace, such as `user`
- `KeyAttrs()`: JSON paths used to derive the storage key, such as `id`
- `TTL()`: the default TTL used by typed writes
- `Mem()`: whether the document belongs to the memory-only layer
- `RawJSON()`: the raw JSON payload

For example, if `Namespace() == "user"` and `KeyAttrs() == []string{"id"}`,
then `{"id":"200"}` resolves to the storage key `user:200`.

## Mental model

Typed helpers accept document-scoped values. `redisx` derives storage-scoped
names from `D` for you.

### Single-document APIs

- `Get("200")` accepts the document-level key value, not the storage key
  `user:200`
- `Set(d)` and `SetNX(d)` derive the storage key from `d.RawJSON()` and
  `d.KeyAttrs()`
- `Delete(d)` also derives the storage key from the document itself; when
  `KeyAttrs() == []string{"id"}`, a payload like `{"id":"200"}` is enough

### Pattern-based APIs

For methods that accept a `keyPattern` (`Keys`, `SearchKey`, `Update`), the
pattern is one document-scoped sub-pattern:

- `Keys("*")` means `user:*`
- `SearchKey("*", filter, false)` searches over `user:*`
- `Update("*", filter, ...)` updates `user:*`

Typed helpers reject already-prefixed storage patterns such as `user:*`,
because the namespace is already derived from `D`.

### Indexed search

`SearchIndex` applies the same document-scoped rule to both inputs:

- `idxName` is one logical name such as `age`, not one runtime name such as
  `user_age`
- `keyPattern` is one document-scoped sub-pattern such as `*`, not one full
  storage pattern such as `user:*`

### TTL behavior

- `Set(d)` and `SetNX(d)` write with `d.TTL()`
- `Update(...)` preserves an existing key TTL

## Embedded mode

Use `server.As[D]` to bind one document type to an existing `*server.DB`.

`As[D]` only changes the static pointer type (`*DB` -> `*DBX[D]`). It does not
allocate a wrapper and does not copy `DB`.

`D` must remain a `string` alias so `Get(...)` can safely return loaded raw
JSON as `D`.
