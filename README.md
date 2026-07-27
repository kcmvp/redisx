<p align="center">
  Redis compatible embedded document store
  <br/>
  <br/>
  <a href="https://github.com/kcmvp/redisx/blob/main/LICENSE">
    <img alt="GitHub" src="https://img.shields.io/github/license/kcmvp/redisx"/>
  </a>
  <a href="https://pkg.go.dev/github.com/kcmvp/redisx">
    <img src="https://pkg.go.dev/badge/github.com/kcmvp/redisx.svg" alt="Go Reference"/>
  </a>
  <a href="https://goreportcard.com/report/github.com/kcmvp/redisx">
    <img src="https://goreportcard.com/badge/github.com/kcmvp/redisx" alt="report"/>
  </a>
  <a href="https://github.com/kcmvp/redisx/blob/main/.github/workflows/ci.yml" rel="nofollow">
     <img src="https://img.shields.io/github/actions/workflow/status/kcmvp/redisx/ci.yml?branch=main" alt="Build" />
  </a>
  <a href="https://app.codecov.io/gh/kcmvp/redisx" ref="nofollow">
    <img src ="https://img.shields.io/codecov/c/github/kcmvp/redisx" alt="coverage"/>
  </a>

</p>

## Features

**redisx** is an embedded, high-performance document store with a Redis-compatible API. It blends standard Redis key-value operations with JSON-aware query and patch commands for JSON documents.

### 1. Native Redis Commands

`redisx` natively supports a subset of standard Redis commands, allowing you to drop it into existing ecosystems with minimal friction:

- **Connection Management:** `AUTH`, `HELLO`, `PING`, `QUIT`, `CLIENT`
- **Key-Value Operations:** `SET`, `SETEX`, `SETNX`, `GET`, `DEL`, `KEYS`
- **Pub/Sub:** `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`

### 2. Extend (X) Commands

The true power of `redisx` lies in its extended document commands. Stored strings can be treated as JSON documents and operated on directly. The current design is schema-less: queries and updates work on key patterns and JSON attributes without predefined schemas.

You can use these commands in two ways:

- With a native Redis client, call `SEARCHINDEX`, `SEARCHKEY`, and `UPDATE` directly and pass JSON strings yourself.
- With the `redisx` Go API, build queries and updates with `x.Filter` and `x.Set(...)`, which gives a more expressive and less error-prone way to describe intent.

Go examples below assume:

```go
import (
    "github.com/kcmvp/redisx/client"
    "github.com/kcmvp/redisx/x"
)
```

#### `SEARCHINDEX`

Performs a MongoDB-style query on a registered index.

**Syntax:**

```text
SEARCHINDEX <index_name> <json_filter> [ASC|DESC]
```

- **`index_name`**: The name of an index that must already exist.
- **`json_filter`**: A MongoDB-style JSON string defining the composite filtering conditions.
- **`[ASC|DESC]`**: Optional order direction. Default is `ASC`.

`SEARCHINDEX` only works with indexes created during server startup. A common
pattern is to declare them with `server.Idx(...)` or `server.Index(...)` when
calling `server.Start(...)`.

**Raw command examples:**

```text
SEARCHINDEX idx_email {"email": "ken@example.com"}
SEARCHINDEX idx_age {"age": {"$gt": 18}}
SEARCHINDEX idx_age {"$and": [{"age": {"$gte": 18}}, {"status": "active"}]}
```

**Go examples:**

```go
res := client.SearchIndex("idx_email", x.Eq("email", "ken@example.com"), false)

res = client.SearchIndex("idx_age", x.Gt("age", 18), false)

res = client.SearchIndex(
    "idx_age",
    x.And(
        x.Gte("age", 18),
        x.Eq("status", "active"),
    ),
    false,
)
```

#### `SEARCHKEY`

Performs a MongoDB-style query over keys matching a glob pattern.

**Syntax:**

```text
SEARCHKEY <pattern> <json_filter> [ASC|DESC]
```

- **`pattern`**: A glob pattern to match the keys (e.g., `*`, `123*`).
- **`json_filter`**: A MongoDB-style JSON string defining the composite filtering conditions.
- **`[ASC|DESC]`**: Optional order direction. Default is `ASC`.

**Raw command examples:**

```text
SEARCHKEY user:* {"region": "us"}
SEARCHKEY order:* {"total": {"$gte": 100}} DESC
```

**Go examples:**

```go
res := client.SearchKey("user:*", x.Eq("region", "us"), false)
res = client.SearchKey("order:*", x.Gte("total", 100), true)
```

#### `UPDATE`

Updates JSON documents matched by key pattern and filter.

**Syntax:**

```text
UPDATE <pattern> <json_filter> <update_json>
```

- **`pattern`**: A glob pattern to match the keys to update.
- **`json_filter`**: A MongoDB-style JSON string defining which JSON documents should be updated.
- **`update_json`**: A JSON object whose key/value pairs are applied as JSON path updates. Nested objects are supported.

**Raw command examples:**

```text
UPDATE user:* {"status": "pending"} {"status": "active"}
UPDATE user:* {"id": "1"} {"profile": {"age": 18}, "verified": true}
```

**Go examples:**

```go
res := client.Update(
    "user:*",
    x.Eq("status", "pending"),
    x.Set("status", "active"),
)

res = client.Update(
    "user:*",
    x.Eq("id", "1"),
    x.Set("profile.age", 18),
    x.Set("verified", true),
)
```

## Usage Modes

`redisx` supports two access modes:

- **Remote access:** connect to the RESP server with the `client` package or any Redis-compatible client.
- **In-process access:** start the server and use the returned `*server.DB` directly inside the same application.

## Storage Layers

`redisx` supports two storage layers inside the same server:

- **Persistent layer:** normal keys are stored here.
- **Memory-only layer:** keys prefixed with `_m_` are stored here and are not kept after restart.

Routing is explicit and key-based:

- `_m_<key>` -> memory-only
- any other key -> disk

For Go clients, use `client.MemKey(...)` to build a memory-only key without hardcoding the prefix:

```go
import "github.com/kcmvp/redisx/client"

if err := client.Connect("127.0.0.1:6380", "demo-key"); err != nil {
    panic(err)
}
defer client.Disconnect()

_ = client.Set("user:1", `{"name":"ken"}`)
_ = client.Set(client.MemKey("session:1"), `{"online":true}`)
```

## Server Startup And Auth

Start the embedded RESP server with:

```go
db := server.Start(
    "127.0.0.1:6380",
    ":memory:",
    server.Idx("user:*", "age"),
    server.Idx("user:*", "email"),
)
```

- Use `":memory:"` for an in-memory instance. If the server restarts, all data is lost.
- Use an explicit file path such as `"/tmp/redisx.db"` to persist data on disk. If the server restarts, data is kept.
- `Start` returns the local `*server.DB` handle, so the same process can also operate on the database directly.
- Remote clients must authenticate before using any command other than the initial handshake commands.
- `SEARCHINDEX` requires its target index to be created here during startup.

### Embedded DB Access

For in-process usage, `server.DB` can be used directly:

```go
import (
    "github.com/kcmvp/redisx/server"
    "github.com/kcmvp/redisx/x"
)

db := server.Start(
    "127.0.0.1:6380",
    ":memory:",
    server.Idx("user:*", "age"),
)

_ = db.Set("user:1", `{"name":"ken","age":18}`)
users := db.SearchIndex("idx_age", x.Gte("age", 18), false).MustGet()
_ = users
```

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

## Installation

```bash
go get github.com/kcmvp/redisx
```
