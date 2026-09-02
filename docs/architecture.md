# Architecture

> ⬅️ [Back to README](../README.md)
> 📖 [Docs index](index.md)
> 🧭 [How-to & examples](howto.md)
> 🏷️ [Typed document helpers](typed-document.md)
> 🪝 [Write Hook Subsystem](write-hooks.md)
> 🔌 [Stream ingest](stream.md)

This document explains how redisx is structured: dual-port startup,
dual storage layer, the AUTH model, and the `x.KeyRange` algebra +
namespace convention that powers the three JSON commands
(`SEARCHINDEX`, `SEARCHKEY`, `UPDATE`).

For copy-paste examples, see [howto.md](howto.md).

---

## Dual-port startup

redisx always opens **two** TCP listeners:

| Port | Role | Purpose |
|---|---|---|
| **App port** (default 7379) | Client-facing | RESP commands: SET, GET, SEARCHINDEX, pub/sub, … |
| **Ctrl port** (default 7381) | Admin-facing | Schema/index registry (REGSCH, REGIDX, DROPSCH, DROPIDX), internal client bridge |

The ctrl port is bound to `127.0.0.1` by default and is not meant for
external traffic. The embedded Go client (`client.ConnectEmbedded`) uses
it automatically via a shared per-process auth key.

### Starting the server

```go
import (
    "github.com/kcmvp/redisx/server"
    "github.com/kcmvp/redisx/x"
)

type UserDoc string

func (UserDoc) Namespace() string  { return "user" }
func (UserDoc) Mem() bool          { return false }
func (UserDoc) KeyAttrs() []string { return []string{"id"} }
func (u UserDoc) RawJSON() string  { return string(u) }
func (UserDoc) TTL() time.Duration { return 0 }

// Zero-config: reads redisx.yaml from cwd (or applies defaults)
db := server.Start(UserDoc(""))
defer server.Stop()
```

`server.Start` accepts variadic `x.Schema` values. It loads configuration
from `redisx.yaml` in the current working directory; when the file is
missing, sensible defaults kick in (app 7379 on auto-selected RFC1918
bind, ctrl 7381 on 127.0.0.1, database at `~/.redisx/redisx.db`).

### Explicit configuration

```go
cfg := &server.Config{
    App:      server.AppConfig{Bind: "127.0.0.1", Port: 7379},
    Ctrl:     server.CtrlConfig{Bind: "127.0.0.1", Port: 7381},
    DataPath: "/var/lib/redisx/app.db",
}
db := server.StartWith(cfg, UserDoc(""))
```

Rules:

- `DataPath` must be a **file path**, not `":memory:"` — the memory layer
  is already `_m_:`, so `:memory:` as a primary layer would be ambiguous.
- Missing parent dirs are created automatically.
- App and ctrl ports must be distinct and in the range 7001–65535.
- `server.Start` returns `*server.DB` for direct in-process access (no
  RESP round-trip).

### Registering indexes

Indexes are **not** passed as Go parameters to `Start`. They are created
at runtime via the ctrl port:

```text
REGIDX user age user:* age
```

Or from Go, using the typed client:

```go
_ = client.RegisterIndex[UserDoc]("age", "*", "age")
```

`SEARCHINDEX` can only use indexes that have been registered. Indexes
persist to the matching storage layer and are auto-recovered on restart.

---

## Dual storage layer

`redisx` always opens two storage layers in the same process. Routing is
purely key-prefix based and deterministic — there is no runtime flag to
flip.

| Layer | Prefix | Lifecycle | `DataPath` influence |
|---|---|---|---|
| **Primary (disk-backed)** | any key **not** starting with `_m_:` | Persisted to `DataPath` via BuntDB | `DataPath` stores exactly this layer |
| **Memory-only (volatile)** | keys starting with `_m_:` | Lost on process exit | Not touched |

Key points:

- The two layers use separate BuntDB handles: one disk-backed, one
  in-memory (`buntdb.Open(":memory:")`).
- Pattern scans (`KEYS`, `SEARCHKEY`, `UPDATE`) resolve exactly **one**
  concrete layer first. Patterns that start with a wildcard (`*`, `?`)
  are **rejected** for these commands — layer is ambiguous.
- `SEARCHINDEX` always routes by index first, then runs the key-pattern
  inside that same layer. Indexes have a 1-to-1 mapping to the layer of
  the document that registered them.

Memory key examples:

```text
_m_:user:session1   → memory layer
_m_:cache:hits      → memory layer
user:profile:200    → primary disk layer
```

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
  in-process `client` package when connecting via `ConnectEmbedded`.

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
typed client API  →  raw client API  →  RESP wire  →  server engine
(client.SearchKey)   (raw.SearchKey)    (JSON arg1)   (db.SearchKey)
```

### The `:` namespace convention (locked public spec)

**Storage key format:**

```text
disk layer   →  <namespace>:<id>            ← one colon, always
memory layer →  _m_:<namespace>:<id>        ← _m_: layer prefix, then one colon
```

The **first `:` character** (counting from the left) separates the
scope prefix from the id part. Examples:

| Storage key | Scope prefix | Id | Layer |
|---|---|---|---|
| `user:200` | `user:` | `200` | disk |
| `_m_:user:200` | `_m_:user:` | `200` | memory |
| `trade:btcusdt@kline_1m` | `trade:` | `btcusdt@kline_1m` | disk |
| `just-a-flat-key` | *(none)* | `just-a-flat-key` | disk *(bare key — not in any namespace)* |

Consequences:

1. **Layer** can always be derived before inspecting the namespace —
   `strings.HasPrefix(key, "_m_:")` is layer-complete.
2. **The first `:`** is the only delimiter that matters for scoping.
   There is intentionally **no** multi-colon hierarchy: the id part is
   free-form and can contain colons itself.
3. Pattern-based scans on the server side enforce this through
   `scopeGuard` helpers in `server/key_range.go`: for literal-range
   constructors (`KeysGte`, `KeysBt`, etc.) every B-tree callback first
   checks "does this key's first-colon scope prefix match the scan's
   scope prefix". This prevents a `KeysGte(_m_:nsA:p050)` from leaking
   into the larger `_m_:nsB:p000…p099` namespace just because of
   dictionary order.

### Six sealed constructors + Limit

There are exactly **6** ways to build a `KeyRange`. They are sealed via
the unexported `keyRange` return type: add via helper only.

| Constructor | Semantics | Accepts |
|---|---|---|
| `x.KeysPattern(pattern)` | Glob match; `*` / `?` are allowed | Pure glob, no prefix-only constraints for wildcard location |
| `x.KeysEq(key)` | Exactly one key = | Full storage key or (typed) doc-scoped id |
| `x.KeysGt(pivot)` | Strictly greater than pivot, ASC default | Literal pivot (no wildcard) |
| `x.KeysGte(pivot)` | Greater-or-equal | Literal pivot |
| `x.KeysLt(pivot)` | Strictly less than | Literal pivot |
| `x.KeysLte(pivot)` | Less-or-equal | Literal pivot |
| `x.KeysBt(a, b)` | Half-open `[a, b)` — ge included, lt excluded | Two literal pivots |

Then one modifier:

- `kr.Limit(count)` — with `count > 0` only. Callbacks are short-circuited
  *during* the B-tree walk.

### Four-layer signature parity

For each of the three commands, every layer accepts the **same**
`x.KeyRange` (RESP takes it as a single JSON object):

```text
Server engine — db.SearchKey / db.Update / db.SearchIndex
    ↑ RESP argv[1] = one JSON object matching the constructor shape
Client API  — client.SearchKey / client.Update / client.SearchIndex
    ↑ typed client — client.SearchKey[D](docScopedKR, …) first runs
                     x.ScopeKeyRange[D](docScopedKR) to force D's
                     fullNamespace+layer prefix onto every pivot/pattern
```

### Input examples

```go
x.KeysPattern("user:*")                                 // every user
x.KeysGte("user:200")                                   // users id ≥ 200
x.KeysBt("user:200", "user:300").Limit(10)              // users [200,300) first 10 ASC
x.KeysPattern("*")                                      // ← rejected for SK/UPDATE — ambiguous layer
```

For typed helpers, the equivalent scoped inputs would be:

```go
client.SearchKey[UserDoc](x.KeysPattern("*"), nil, false)  // resolves to "user:*" or "_m_:user:*" automatically
client.Update[UserDoc](x.KeysGte("200").Limit(10), …)      // resolves to ≥ "user:200" / "_m_:user:200"
```
