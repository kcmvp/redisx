<p align="center">
  Redis compatible embedded JSON document store
  <br/>
  <br/>
  <a href="https://github.com/kcmvp/redisx/blob/main/LICENSE">
    <img alt="GitHub" src="https://img.shields.io/github/license/kcmvp/redisx"/>
  </a>
  <a href="https://pkg.go.dev/github.com/kcmvp/redisx">
    <img src="https://pkg.go.dev/badge/github.com/kcmvp/redisx.svg" alt="Go Reference"/>
  </a>
  <a href="https://github.com/kcmvp/redisx/blob/main/.github/workflows/ci.yml" rel="nofollow">
     <img src="https://img.shields.io/github/actions/workflow/status/kcmvp/redisx/ci.yml?branch=main" alt="Build" />
  </a>
  <a href="https://app.codecov.io/gh/kcmvp/redisx" ref="nofollow">
    <img src ="https://img.shields.io/codecov/c/github/kcmvp/redisx" alt="coverage"/>
  </a>

</p>

## Features

**redisx** is an embedded, high-performance JSON document store with a Redis-compatible API. It blends standard Redis key-value operations with JSON-aware query and patch commands for JSON documents.

### 1. Native Redis Commands

`redisx` natively supports a subset of standard Redis commands, allowing you to drop it into existing ecosystems with minimal friction:

- **Connection Management:** `AUTH`, `HELLO`, `PING`, `QUIT`, `CLIENT`
- **Key-Value Operations:** `SET`, `SETEX`, `SETNX`, `GET`, `DEL`, `KEYS`
- **Pub/Sub:** `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`

### 2. Extended (X) Commands

The true power of `redisx` lies in its extended document commands. Stored strings can be treated as JSON documents and operated on directly. The current design is schema-less: queries and updates work on key patterns and JSON attributes without predefined schemas.

You can use these commands in two ways:

- With a Redis-compatible client, call `SEARCHINDEX`, `SEARCHKEY`, and `UPDATE` directly and pass JSON strings yourself.
- With the `redisx` Go API, build the same queries and updates with `x.Filter` and `x.Set(...)`.

In the raw RESP API, command arguments always use full storage keys, full key
patterns, and full index names. The typed helper API later in this README uses
document-scoped names and sub-patterns instead.

Go examples below assume:

```go
import (
    "github.com/kcmvp/redisx/client"
    "github.com/kcmvp/redisx/x"
)
```

### 3. Typed JSON Document API (`x.Document`)

Beyond raw key/value commands, `redisx` also provides a typed JSON document layer based on `x.Document`.

You define a document type once, then work with document-level keys instead of manually composing storage keys such as `"user:200"`. `x.Document` is a JSON string alias contract, so a document type is typically defined as `type UserDoc string`.

- **Client side:** use `client/doc` over the shared RESP connection
- **Server side:** use `server.As[D]` to get `*server.DBX[D]` on top of an embedded `*server.DB`

This keeps the low-level key/value API available, while giving higher-level code a cleaner JSON document entry point.

In Go code, the client-side package path is `client/doc`; examples typically
import it as `doc`.

Core document commands:

- `SEARCHINDEX`: query through one registered JSON index
- `SEARCHKEY`: scan one full storage-key pattern and filter JSON payloads
- `UPDATE`: patch matched JSON documents in place
- `GET`, `SET`, `SETNX`, `DEL`, and `KEYS`: also have typed document helpers

Parameter semantics differ by API surface:

- raw RESP commands use full storage keys, full key patterns, and full index names
- typed `x.Document` entry points use logical index names and document-scoped sub-patterns

Practical notes for typed helpers:

- `Get("200")` accepts the document-level key value, not the full storage key `"user:200"`
- `Keys("*")`, `SearchKey("*", ...)`, and `Update("*", ...)` accept one document-scoped sub-pattern, which is automatically prefixed to `user:*`
- `SearchIndex("age", "*", ...)` accepts the logical index name `age`, not the full runtime index name `user_age`
- typed helpers reject already-prefixed storage patterns such as `user:*`, because the namespace is already derived from `D`

For the full typed API contract, see [docs/typed-document.md](docs/typed-document.md).
For detailed command examples, see [docs/howto.md](docs/howto.md).

### 4. Stream Ingest (`ingest/stream`)

`redisx` also includes an optional websocket ingestion extension in
`ingest/stream`.

It is designed for one focused job: consume external websocket streams and
forward payloads into `x.Document` workflows.

The package provides:

- `stream.Start[D](...)`: for endpoints whose full subscription set is already encoded in the URL
- `stream.StartSubscribable[D](...)`: for protocols that add and remove subscriptions over one existing websocket connection
- automatic reconnect and subscription restore after reconnect
- optional active websocket ping via one `time.Duration` argument

This keeps the core storage API small while giving stream-driven applications a
native way to feed realtime document payloads into `redisx`.

For the full stream ingest contract and examples, see
[docs/stream.md](docs/stream.md).

### 5. Write Hook Subsystem — Register once, applied globally

`redisx` ships a first-class typed **Write Hook Subsystem** on top of every
write path (`client.Set`, `client.SetWithTTL`, `client.SetNX`,
`client.SetNXWithTTL`, and their typed document helpers in `client/doc`).

Register a hook **once** when your process boots; it applies to every future
write without touching any business call site. Four semantic hook types are
provided as distinct Go function signatures — the type system enforces the
correct error-vs-value contract for each:

| # | Type | Timing | Signature | Fail policy | Typical use |
|---|---|---|---|---|---|
| 1 | **AbortHook** | Before write | `(key, value) error` | **fail-closed** — any err/panic/timeout **aborts** the write | DLP, ACL, rate limit, quota, audit gating |
| 2 | **TransformHook** | Before write | `(key, value) (newValue, err)` | **fail-closed** on err | AES encrypt, gzip, schema prefix, payload normalization |
| 3 | **ObserverHook** (Before) | Before write, after Abort+Transform | `(key, value)` — no error return | **fail-open** — panic/timeout logged only | debug fixture capture, metrics counters, ad-hoc snapshot dumps |
| 4 | **ObserverAfterHook** (After) | After write | `(key, value, writeErr)` — no error return | **fail-open** | CDC, L1 cache invalidation, audit, dual-write migration |

Mandatory synchronous execution order (all Before-phase hooks complete their
lifecycle **before** `Set` returns, even under outer `context.WithTimeout`):

```
AbortHook → TransformHook chain → ObserverHook (sees post-transform value)
           ↓ actual Redis SET/SETNX ↓
ObserverAfterHook (sees final value + writeErr)
```

Every hook has two built-in, **always-on** safety nets:
1. **Panic isolation** — `recover()` + `slog.Error` with hook label, panic
   value, and a full stack trace. Never propagates to your main code.
2. **Per-hook execution timeout** — default 100 ms, configurable via
   `client.SetHookTimeout(d)`. Pass `d <= 0` to disable timeouts (panic
   isolation remains on).

Fail-closed vs fail-open is non-negotiable per hook type:

- **AbortHook / TransformHook** panic or timeout ⇒ write is **aborted**
  (security and data-safety default).
- **Observer*Hook** panic or timeout ⇒ **only a log is emitted**; the write and
  sibling hooks continue unaffected.

There is **zero cost by default**: the hot path does a single `atomic.Load`
when zero hooks are registered (~0.24 ns/op, 0 B/op, 0 allocs on Apple M4),
which is well under 0.01% of a real Redis SET round-trip. Hook add/remove uses
a copy-on-write slice swap, so writing can proceed concurrently with
registration changes.

#### Quick example

```go
import (
    "github.com/kcmvp/redisx/client"
    doc "github.com/kcmvp/redisx/client/doc" // typed JSON docs
)

// 1) Gate — abort writes of oversized payloads (fail-closed).
oversize := client.AddAbortHook(func(key string, value []byte) error {
    if len(value) > 1024*1024 {
        return fmt.Errorf("rejecting %s: payload %d bytes > 1MB", key, len(value))
    }
    return nil
})

// 2) Transform — wrap every JSON doc with a {"schema":"v1","doc":...} envelope.
// (Illustrative skeleton; see docs/write-hooks.md for the full copy-safe version.)
schemaTag := doc.AddTransformHook(func(key string, valueJSON []byte) ([]byte, error) {
    env := make([]byte, 0, len(`{"schema":"v1","doc":}`)+len(valueJSON))
    env = append(env, []byte(`{"schema":"v1","doc":}`)...)
    env = append(env, valueJSON...)
    env = append(env, '}')
    return env, nil
})

// 3) Observe before write — capture every user-document write to a test fixture.
var captureMu sync.Mutex
var fixtures [][]byte
capture := doc.AddObserverHook(func(key string, valueJSON []byte) {
    if strings.HasPrefix(key, "user:") {
        captureMu.Lock()
        fixtures = append(fixtures, append([]byte(nil), valueJSON...))
        captureMu.Unlock()
    }
})

// 4) Observe after write — invalidate an external L1 cache on success only.
l1 := client.AddObserverAfterHook(func(key string, value []byte, writeErr error) {
    if writeErr == nil {
        go externalL1Cache.Evict(key) // async inside Observer — you choose
    }
})

// Later: remove a specific hook, or disable timeouts globally for heavy-duty observers.
client.RemoveHook(capture)
client.SetHookTimeout(0) // timeouts off; panic recover still on.
```

For the full hook contract, safety-net composition rules, real-world patterns
per hook type, and troubleshooting, see [docs/write-hooks.md](docs/write-hooks.md).

## Storage Layers

`redisx` supports two storage layers inside the same server:

- **Primary layer:** normal keys are stored here. It is backed by the `dbPath`
  you pass to `server.Start(...)`.
- **Memory-only layer:** keys prefixed with `_m_` are stored here and are not kept after restart.

`redisx` always opens both layers at the same time.

The `dbPath` argument only configures the primary layer:

- use a real database file path such as `"/tmp/redisx.db"`
- `":memory:"` is rejected because redisx already has a dedicated memory-only layer

Routing is explicit and key-based:

- `_m_<key>` -> memory-only
- any other key -> primary layer

Pattern-based scan commands do not cross layers implicitly:

- `KEYS`, `SEARCHKEY`, and `UPDATE` must resolve one concrete layer first
- patterns that start with `*` or `?` are rejected for those commands
- `SEARCHINDEX` routes by index first, then applies `key_pattern` inside that layer

For concrete memory-key examples, see [docs/howto.md](docs/howto.md).

## Server Startup

Start the embedded RESP server with:

```go
import (
    "os"
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

home, _ := os.UserHomeDir()
dbPath := filepath.Join(home, ".redisx", "test.db")

db := server.Start(
    "127.0.0.1:6380",
    dbPath,
    x.Idx[UserDoc]("age", "*", "age"),
    x.Idx[UserDoc]("email", "*", "email"),
)
```

- The second argument is passed through to `buntdb.Open(path)`.
- `redisx` always opens a second dedicated memory-only layer for `_m_` keys.
- Use an explicit database file path such as `"/tmp/redisx.db"` for the primary layer.
- `":memory:"` is rejected as `dbPath`; the `_m_` layer is already the in-memory layer.
- Missing parent directories are created automatically, and the database file is created on first start if it does not already exist.
- `dbPath` itself must still be a file path, not a directory.
- Do not pass `"~/.redisx/test.db"` literally. `redisx` does not expand `~`; build an explicit path yourself.
- `Start` returns the local `*server.DB` handle, so the same process can also operate on the database directly.
- `SEARCHINDEX` requires its target index to be created here during startup.

For end-to-end startup, embedded access, and typed API examples, see
[docs/howto.md](docs/howto.md) and [docs/typed-document.md](docs/typed-document.md).

## Authentication

All external connections must authenticate with an auth key that already exists in storage. Authentication configuration is stored with the reserved prefix `_auth_:`.

```text
_auth_:<auth_key> -> <max_connections>
```

Examples:

```text
SET _auth_:demo-key 2
SET _auth_:batch-worker 20
```

- The value is the maximum number of concurrent connections allowed for that auth key.
- Limits are refreshed from storage during `AUTH`, so changes take effect for new authentications without restarting the server.
- If a stored auth limit is expired or unavailable, that auth key is treated as unavailable.
- `internalAuthKey` is generated per process, is not stored in the database, and is always unlimited.

## Docs

- [docs/howto.md](docs/howto.md): copy-paste oriented command examples,
  including connection/authentication flows, key-value commands, pub/sub
  commands, JSON query/update commands, and typed document end-to-end examples
- [docs/typed-document.md](docs/typed-document.md): the `x.Document` contract
  and the input semantics of typed helpers on both the client and embedded
  server side
- [docs/write-hooks.md](docs/write-hooks.md): the **Write Hook Subsystem** —
  four typed hook kinds (Abort / Transform / Observer-Before /
  Observer-After), their fail-policy matrix, safety-net composition rules,
  real-world usage patterns per hook type, hook lifecycle guidance, and
  troubleshooting
- [docs/stream.md](docs/stream.md): the websocket stream ingestion extension
  for `x.Document` workflows, including reconnect and subscription-aware
  behavior

## Installation

```bash
go get github.com/kcmvp/redisx
```
