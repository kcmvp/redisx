# Architecture

> ⬅️ [Back to README](../README.md)
> 📖 [Docs index](index.md)
> 🧭 [How-to & examples](howto.md)
> 🏷️ [Typed document helpers](typed-document.md)
> 🪝 [Write Hook Subsystem](write-hooks.md)
> 🔌 [Stream ingest](stream.md)

This document explains how redisx is structured: dual storage layer,
server startup, the AUTH model, and the `x.KeyRange` algebra + namespace
convention that powers the three JSON commands
(`SEARCHINDEX`, `SEARCHKEY`, `UPDATE`).

For copy-paste examples, see [howto.md](howto.md).

For typed document-scoped helpers, see [typed-document.md](typed-document.md).

---

## Dual storage layer

`redisx` always opens two storage layers in the same process. Routing is
purely key-prefix based and deterministic — there is no runtime flag to
flip.

| Layer | Prefix | Lifecycle | `dbPath` influence |
|---|---|---|---|
| **Primary (disk-backed)** | any key **not** starting with `_m_` | Persisted to `dbPath` via BuntDB | `dbPath` stores exactly this layer |
| **Memory-only (volatile)** | keys starting with `_m_` | Lost on process exit | Not touched |

Key points:

- The two layers share a single BuntDB `*DB` handle internally; the
  memory layer uses an in-memory BuntDB opened alongside it.
- Pattern scans (`KEYS`, `SEARCHKEY`, `UPDATE`) resolve exactly **one**
  concrete layer first. Patterns that start with a wildcard (`*`, `?`)
  are **rejected** for these commands — layer is ambiguous.
- `SEARCHINDEX` always routes by index first, then runs the key-pattern
  inside that same layer. Indexes have a 1-to-1 mapping to the layer of
  the document that registered them.

Memory key examples:

```text
_m_user:session1   → memory layer
_m_cache:hits      → memory layer
user:profile:200   → primary disk layer
```

See the `KeyRangeFixtureMem()` toggle in the test suite for a concrete
proof that the exact same KeyRange expression resolves both layers.

---

## Server startup

Start the embedded RESP server with:

```go
import (
    "path/filepath"
    "time"

    "github.com/kcmvp/redisx/server"
    "github.com/kcmvp/redisx/x"
)

type UserDoc string

func (UserDoc) Namespace() string  { return "user" }
func (UserDoc) Mem() bool          { return false }
func (UserDoc) KeyAttrs() []string { return []string{"id"} }
func (u UserDoc) RawJSON() string  { return string(u) }
func (UserDoc) TTL() time.Duration { return 0 }

dbPath := filepath.Join("/var/lib/redisx", "app.db")

db := server.Start(
    "127.0.0.1:6380",
    dbPath,
    x.Idx[UserDoc]("age",   "*", "age"),
    x.Idx[UserDoc]("email", "*", "email"),
)
```

Rules for `Start(addr, dbPath, indexes...)`:

- `addr` is passed directly to `net.Listen("tcp", addr)`.
- `dbPath` must be a **file path**, not `":memory:"` — the memory layer
  is already `_m_`, so `:memory:` as a primary layer would be
  ambiguous.
- `dbPath` is forwarded to `buntdb.Open(dbPath)`. Missing parent dirs
  are created; the file is created empty on first start.
- `~` paths are **not** expanded — build an explicit path yourself.
- `x.Idx[D](...)` registers one JSON secondary index for document `D`.
  `SEARCHINDEX` can only use indexes that were registered at startup.
- `Start` returns `*server.DB` for direct in-process access (no RESP
  round-trip). Use `server.As[D](db)` to get a typed document view of
  it — see [typed-document.md](typed-document.md).

---

## AUTH model

All external (non-internal-auth) connections must present an auth key
that already exists in storage. Configs live under the reserved prefix
`_auth_:`.

```text
SET _auth_:<auth_key> <max_connections>
```

Examples:

```text
SET _auth_:demo-key    2
SET _auth_:batch-job  50
```

Auth rules:

- The **value** is the max number of **concurrent connections** allowed
  for that key.
- Connection count is re-read from storage on every `AUTH`, so changes
  take effect for new AUTHs without a restart.
- If the stored `_auth_:<key>` record is expired, missing, or out of
  range, the key is treated as disabled.
- Every process also has one per-process `internalAuthKey` that is
  **not** stored in the DB and is always unlimited. It is used by the
  in-process `client` package when connecting to a loopback server.

The minimal setup sequence is:

1. `server.Start(...)`
2. Use the returned `*server.DB` to `Set("_auth_:mykey", "4")`
3. Connect from any external client with `AUTH mykey`.

---

## KeyRange & namespace convention

The three extended JSON commands (`SEARCHINDEX`, `SEARCHKEY`, `UPDATE`)
are all built on top of one sealed expression algebra:
`github.com/kcmvp/redisx/x.KeyRange`. The same `KeyRange` value passes
through **all four layers** of the stack with identical meaning:

```
typed doc API  →  untyped client API  →  RESP wire  →  server engine
(doc.ScopeKeyRange)   (client.SearchKey/Update)     (server.db.ApplyKeyRange)
```

This section describes both the public constructors and the namespace
convention that makes cross-namespace scans safe even when multiple
consumers share one server.

### The `:` namespace convention (locked public spec)

**Storage key format:**

```text
disk layer   →  <namespace>:<id>            ← one colon, always
memory layer →  _m_<namespace>:<id>         ← one colon after the _m_ layer prefix
```

The **first `:` character** (counting from the left) separates the
scope prefix from the id part. Examples:

| Storage key | Scope prefix | Id | Layer |
|---|---|---|---|
| `user:200` | `user:` | `200` | disk |
| `_m_user:200` | `_m_user:` | `200` | memory |
| `trade:btcusdt@kline_1m` | `trade:` | `btcusdt@kline_1m` | disk |
| `just-a-flat-key` | *(none)* | `just-a-flat-key` | disk *(bare key — not in any namespace)* |

Consequences:

1. **Layer** can always be derived before inspecting the namespace —
   `strings.HasPrefix(key, "_m_")` is layer-complete.
2. **The first `:`** is the only delimiter that matters for scoping.
   There is intentionally **no** multi-colon hierarchy: the id part is
   free-form and can contain colons itself.
3. Pattern-based scans on the server side enforce this through
   `scopeGuard` / `scopeGuard2` helpers in
   `server/key_range.go`: for literal-range constructors (`KeysGte`,
   `KeysBt`, etc.) every B-tree callback first checks "does this key's
   first-colon scope prefix match the scan's scope prefix". This
   prevents a `KeysGte(_m_nsA:p050)` from leaking into the larger
   `_m_nsB:p000…p099` namespace just because of dictionary order.

### Six sealed constructors + Limit

There are exactly **6** ways to build a `KeyRange`. They are sealed via
the unexported `keyRange` return type: add via helper only.

| Constructor | Semantics | Accepts |
|---|---|---|
| `x.KeysPattern(pattern)` | Glob match; `*` / `?` are allowed | Pure glob, no prefix-only constraints for wildcard locations |
| `x.KeysEq(key)` | Exactly one key = | Full storage key or (typed) doc-scoped id |
| `x.KeysGt(pivot)` | Strictly greater than pivot, ASC default | Literal pivot (no wildcard) |
| `x.KeysGte(pivot)` | Greater-or-equal | Literal pivot |
| `x.KeysLt(pivot)` | Strictly less than | Literal pivot |
| `x.KeysLte(pivot)` | Less-or-equal | Literal pivot |
| `x.KeysBt(a, b)` | Strictly within the interval `(a, b)` — bounds excluded on both sides | Two literal pivots |

Then one modifier:

- `kr.Limit(count)` — with `count > 0` only. Callbacks are short-circuited
  *during* the B-tree walk (proven by the `TestLimit7PrefixEqualFullSet`
  suites: `sort(full)[:7]` byte-equals `Limit(7)` result).

### Four-layer signature parity

For each of the three commands, every layer accepts the **same**
`x.KeyRange` (RESP takes it as a single JSON object):

```text
Server engine — server/db.ApplySearchKey / ApplyUpdate / ApplySearchIndex
    ↑ RESP argv[1] = one JSON object matching the constructor shape
Client API  — client.SearchKey / Update / SearchIndex(kr, …)
    ↑ typed client/doc — doc.SearchKey[D](docScopedKR, …)  first runs
                         internal.ScopeKeyRange[D](docScopedKR) to force
                         D's fullNamespace+layer prefix onto every
                         pivot/pattern — so typed APIs never hit the
                         "different storage layer" server-side error.
```

### Input examples

```go
x.KeysPattern("user:*")                                 // every user
x.KeysGte("user:200")                                   // users id ≥ 200
x.KeysBt("user:200", "user:300").Limit(10)              // users (200,300) first 10 ASC
x.KeysPattern("*")                                      // ← rejected for SK/UPDATE — ambiguous layer
```

For typed helpers, the equivalent scoped inputs would be:

```go
doc.SearchKey[UserDoc](x.KeysPattern("*"), nil, false)  // resolves to "user:*" or "_m_user:*" automatically
doc.Update[UserDoc](x.KeysGte("200").Limit(10), …)      // resolves to ≥ "user:200" / "_m_user:200"
```
