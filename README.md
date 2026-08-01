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

Core document commands:

- `SEARCHINDEX`: query through one registered JSON index
- `SEARCHKEY`: scan one full storage-key pattern and filter JSON payloads
- `UPDATE`: patch matched JSON documents in place

Parameter semantics differ by API surface:

- raw RESP commands use full storage keys, full key patterns, and full index names
- typed `x.Document` entry points use logical index names and document-scoped sub-patterns

For the full typed API contract, see [docs/typed-document.md](docs/typed-document.md).
For detailed command examples, see [docs/howto.md](docs/howto.md).

## Usage Modes

`redisx` supports two access modes:

- **Remote access:** connect to the RESP server with the `client` package or any Redis-compatible client.
- **In-process access:** start the server and use the returned `*server.DB` directly inside the same application.

If your values are JSON documents, both modes also support the typed `x.Document` helpers:

- **Remote typed documents:** `client/doc`
- **Embedded typed documents:** `server.As[D]` -> `*server.DBX[D]`

## Storage Layers

`redisx` supports two storage layers inside the same server:

- **Primary layer:** normal keys are stored here. It is backed by the `dbPath`
  you pass to `server.Start(...)`.
- **Memory-only layer:** keys prefixed with `_m_` are stored here and are not kept after restart.

`redisx` always opens both layers at the same time.

The `dbPath` argument only configures the primary layer:

- use a real file path such as `"/tmp/redisx.db"` for a disk-backed primary layer
- use BuntDB's special path `":memory:"` for an in-memory primary layer

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

db := server.Start(
    "127.0.0.1:6380",
    ":memory:",
    x.Idx[UserDoc]("age", "*", "age"),
    x.Idx[UserDoc]("email", "*", "email"),
)
```

- The second argument is passed through to `buntdb.Open(path)`.
- `redisx` always opens a second dedicated memory-only layer for `_m_` keys.
- Use BuntDB's special path `":memory:"` when the primary layer should also remain in memory. If the server restarts, all primary-layer data is lost.
- Use an explicit file path such as `"/tmp/redisx.db"` when the primary layer should persist on disk. If the server restarts, that primary-layer data is kept.
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

## Command How-To

For detailed, copy-paste oriented examples of every supported command, see
[docs/howto.md](docs/howto.md).

It includes:

- connection and authentication flows
- key-value commands
- pub/sub commands
- JSON query and update commands
- typed JSON document API end-to-end examples

## Installation

```bash
go get github.com/kcmvp/redisx
```
