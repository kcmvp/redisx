# How-To

> ⬅️ [Back to README](../README.md)
> 📖 [Docs index](index.md)
> 🧱 [Architecture & KeyRange convention](architecture.md)
> 🏷️ [Typed document helpers](typed-document.md)
> 🪝 [Write Hook Subsystem](write-hooks.md)
> 🔌 [Stream ingest](stream.md)

This guide is copy-paste oriented: RESP command examples plus the
matching Go client calls. For background on dual-port startup, the
dual storage layer, AUTH model, or the `:` namespace convention, see
[architecture.md](architecture.md).

For intercepting writes (DLP gates, AES encryption, L1 cache invalidation,
CDC, audit logging) without modifying individual call sites, see
**[write-hooks.md](write-hooks.md)**.

## Setup

Assume the server is started like this:

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
func (UserDoc) TTL() time.Duration { return time.Hour }

cfg := &server.Config{
    App:  server.AppConfig{Bind: "127.0.0.1", Port: 7379},
    Ctrl: server.CtrlConfig{Bind: "127.0.0.1", Port: 7381},
}
db := server.StartWith(cfg, UserDoc(""))
defer server.Stop()

_ = db.Set("_auth_:demo-key", "2")
```

The examples below assume:

- app port: `127.0.0.1:7379`
- auth key: `demo-key`
- auth config already exists as `_auth_:demo-key -> 2`

Go client examples use the `raw` sub-package for untyped key-value
operations and the `client` package for typed document operations:

```go
import (
    "github.com/kcmvp/redisx/client"
    "github.com/kcmvp/redisx/client/raw"
    "github.com/kcmvp/redisx/x"
)

_ = client.Connect("127.0.0.1:7379", "demo-key")
```

## Connection Commands

### `AUTH`

Authenticate before using any stateful command.

```text
AUTH demo-key
```

Expected response: `OK`

Go client:

```go
_ = client.Connect("127.0.0.1:7379", "demo-key")
```

### `HELLO`

Inspect basic server metadata.

```text
HELLO
```

Example response fields:

```text
server: redisx
version: 1.0.0
proto: 2
mode: standalone
role: master
```

### `PING`

Health check for one authenticated connection.

```text
PING
```

Expected response: `PONG`

### `QUIT`

Close the current connection.

```text
QUIT
```

Expected response: `OK`

## Key-Value Commands

### `SET`

Store one string value.

```text
SET user:1 {"id":"1","name":"ken","age":18}
```

Expected response: `OK`

Go client:

```go
if err := raw.Set("user:1", `{"id":"1","name":"ken","age":18}`); err != nil {
    panic(err)
}
```

### `SET` with `EX` / `PX`

Store one value with a TTL in seconds or milliseconds.

```text
SET session:1 {"online":true} EX 60
SET session:2 {"online":true} PX 1500
```

Go client:

```go
_ = raw.SetWithTTL("session:1", `{"online":true}`, 60*time.Second)
```

### `SET` with `NX`

Only store the value when the key does not already exist.

```text
SET user:1 {"id":"1","name":"override"} NX
```

If the key already exists, the response is null.

### `SETEX`

Store one value with a TTL in seconds.

```text
SETEX cache:user:1 30 {"name":"ken"}
```

Go client:

```go
_ = raw.SetWithTTL("cache:user:1", `{"name":"ken"}`, 30*time.Second)
```

### `SETNX`

Store one value only when the key does not already exist.

```text
SETNX user:2 {"id":"2","name":"alice"}
```

Expected response: `1` when created, `0` when the key already existed.

Go client:

```go
ok, err := raw.SetNX("user:2", `{"id":"2","name":"alice"}`)
_ = ok
```

### `GET`

Read one string value by key.

```text
GET user:1
```

Expected response:

```text
{"id":"1","name":"ken","age":18}
```

Go client:

```go
val, err := raw.Get("user:1")
_ = val
```

### `DEL`

Delete one key.

```text
DEL user:2
```

Expected response: `1` when the key existed, `0` when it did not.

Go client:

```go
deleted, err := raw.Delete("user:2")
_ = deleted
```

### `KEYS`

List keys by one full storage-key pattern.

```text
KEYS user:*
```

Expected response:

```text
1) user:1
2) user:3
```

Important constraints:

- `KEYS` must resolve one concrete storage layer first
- patterns starting with `*` or `?` are rejected

Go client:

```go
res := raw.Keys("user:*")
if res.IsError() {
    panic(res.Error())
}
keys := res.MustGet()
_ = keys
```

## Pub/Sub Commands

### `PUBLISH`

Publish one payload to one topic.

```text
PUBLISH topic:user.updated {"id":"1","name":"ken"}
```

The integer response is the number of receivers that got the message.

### `SUBSCRIBE`

Subscribe to one or more exact topics.

```text
SUBSCRIBE topic:user.created topic:user.updated
```

This command enters subscription mode and streams messages on the same
connection.

Go client:

```go
ch := client.Subscribe("topic:user.updated").MustGet()
msg := <-ch
_ = msg
```

## JSON Query And Update Commands

### `SEARCHINDEX`

Query JSON documents through one registered index.

```text
SEARCHINDEX user:age {"op":"pattern","p":"user:*"} {"age":{"$gte":18}}
```

More examples:

```text
SEARCHINDEX user:age {"op":"pattern","p":"user:*"} {"email":"ken@example.com"}
SEARCHINDEX user:age {"op":"gte","pivot":"user:"} {"age":{"$gte":18}} DESC
SEARCHINDEX user:age {"op":"pattern","p":"user:*"} {"age":{"$gte":18}} LIMIT 100
SEARCHINDEX user:age {"op":"pattern","p":"user:*"} {"age":{"$gte":18}} DESC LIMIT 50
```

Important constraints:

- `index_name` is the full runtime index name, such as `user:age`
- the second wire arg is a sealed `KeyRange` JSON payload — see
  [KeyRange constructors](#keyrange-json-shape) below
- indexes must be registered before use (via `REGIDX` or
  `client.RegisterIndex`)
- the index chooses the storage layer; a `KeyRange` that resolves to a
  **different** layer is rejected

Go client:

```go
kr := x.KeysPattern("user:*").Limit(100)
res := raw.SearchIndex("user:age", kr, x.Gte("age", 18), false)
if res.IsError() {
    panic(res.Error())
}
raws := res.MustGet()
_ = raws
```

### `SEARCHKEY`

Query JSON documents by scanning one storage-key range expressed as a
sealed `KeyRange` JSON object.

#### RESP wire format

```text
SEARCHKEY <keyrange_json> <json_filter> [ASC|DESC] [LIMIT count]
```

Argument shapes:

| argc after command | meaning |
|---|---|
| 2 | `{kr}` `{filter}` — ASC, no LIMIT |
| 3 | `{kr}` `{filter}` `ASC\|DESC` — direction only |
| 4 | `{kr}` `{filter}` `LIMIT` `N` — count only, ASC default |
| 5 | `{kr}` `{filter}` `ASC\|DESC` `LIMIT` `N` |

#### KeyRange JSON shape — one-of 6 sealed constructors

| Constructor | JSON wire shape | Go expression |
|---|---|---|
| `KeysPattern(p)` | `{"op":"pattern","p":"user:*"}` | `x.KeysPattern("user:*")` |
| `KeysGte(pivot)` | `{"op":"gte","pivot":"user:100"}` | `x.KeysGte("user:100")` |
| `KeysGt(pivot)`  | `{"op":"gt","pivot":"user:100"}` | `x.KeysGt("user:100")` |
| `KeysLte(pivot)` | `{"op":"lte","pivot":"user:100"}` | `x.KeysLte("user:100")` |
| `KeysLt(pivot)`  | `{"op":"lt","pivot":"user:100"}` | `x.KeysLt("user:100")` |
| `KeysBt(ge, lt)` | `{"op":"bt","ge":"user:0100","lt":"user:0200"}` | `x.KeysBt("user:0100","user:0200")` |

All 6 accept a chained `.Limit(N)` modifier on the Go side; on the wire
`LIMIT N` is a **separate trailing two-token pair**, never inside the
JSON object. Wire wins if both are present.

KeysBt is always **half-open `[ge, lt)`** regardless of direction.

#### RESP examples

```text
SEARCHKEY {"op":"pattern","p":"user:*"} {"status":"active"}
SEARCHKEY {"op":"gte","pivot":"order:2024-01"} {"region":"us"} DESC
SEARCHKEY {"op":"bt","ge":"user:engineering:0100","lt":"user:engineering:0200"} {"total":{"$gte":100}} LIMIT 50
```

Go client:

```go
res := raw.SearchKey(
    x.KeysPattern("user:*"),
    x.Eq("status", "active"),
    false, // desc=false → ASC
)
if res.IsError() {
    panic(res.Error())
}
rawJSONs := res.MustGet()
_ = rawJSONs
```

### `UPDATE`

Patch JSON documents matched by one sealed `x.KeyRange`, one optional
JSON filter, and one JSON object of mutations.

#### RESP wire format

```text
UPDATE <keyrange_json> <filter_json> <update_json> [LIMIT count]
```

Argument shapes (UPDATE has **no** `ASC|DESC` keyword):

| argc after cmd word | shape |
|---|---|
| 3 | `{kr}` `{filter}` `{update}` — no LIMIT |
| 5 | `{kr}` `{filter}` `{update}` `LIMIT` `count` |

#### RESP examples

```text
UPDATE {"op":"pattern","p":"user:*"} {"status":"pending"} {"status":"active"}
UPDATE {"op":"gte","pivot":"order:2024-01-01"} {"region":"us"} {"verified":true}
UPDATE {"op":"bt","ge":"user:engineering:0100","lt":"user:engineering:0200"} {} {"$set":{"status":"reviewed"}} LIMIT 50
```

Go client:

```go
res := raw.Update(
    x.KeysPattern("user:*"),
    x.Eq("status", "pending"),
    x.Set("status", "active"),
    x.Set("verified", true),
)
if res.IsError() {
    panic(res.Error())
}
keys := res.MustGet()
_ = keys
```

## Memory-Only Keys

Use the reserved `_m_:` prefix for memory-only data.

```text
SET _m_:session:1 {"online":true}
GET _m_:session:1
KEYS _m_:session:*
```

Go client:

```go
key := x.MemKey("session:1")
if err := raw.Set(key, `{"online":true}`); err != nil {
    panic(err)
}
```

The full routing model lives in
**[architecture.md § Dual storage layer](architecture.md#dual-storage-layer)**.

## Typed JSON Document API

The typed helpers live in the `client` package as generic functions
parameterized by your document type `D`.

### Remote mode (RESP client)

```go
import (
    "github.com/kcmvp/redisx/client"
    "github.com/kcmvp/redisx/x"
)

_ = client.Connect("127.0.0.1:7379", "demo-key")

// Set — derives the storage key from d.RawJSON() + d.KeyAttrs()
_ = client.Set(UserDoc(`{"id":"200","name":"Test","age":30}`))

// Get — accepts the document-level key value, not the storage key
got, _ := client.Get[UserDoc]("200")

// SearchIndex — idxName is logical ("age"), not runtime ("user:age")
idx := client.SearchIndex[UserDoc]("age", x.KeysPattern("*"), x.Gte("age", 18), false)

// SearchKey
keys := client.SearchKey[UserDoc](x.KeysPattern("*"), x.Eq("status", "active"), false)

// Update
updated := client.Update[UserDoc](x.KeysPattern("*"), x.Eq("id", "200"), x.Set("name", "Updated"))

_ = got
_ = idx
_ = keys
_ = updated
```

### Embedded mode (in-process DB)

```go
// db is *server.DB returned by server.Start
_ = db.Set("user:200", `{"id":"200","name":"Test","age":30}`)

got := db.Get("user:200").MustGet()

// JSON commands work directly on *server.DB
idx := db.SearchIndex("user:age", x.KeysPattern("user:*"), x.Gte("age", 18), false)
keys := db.SearchKey(x.KeysPattern("user:*"), x.Eq("status", "active"), false)
updated := db.Update(x.KeysPattern("user:*"), x.Eq("id", "200"), x.Set("name", "Updated"))

_ = got
_ = idx.MustGet()
_ = keys.MustGet()
_ = updated.MustGet()
```

Typed API rules:

- `idxName` is logical, such as `age`, not runtime `user:age`
- `scopedKR` is document-scoped, such as `x.KeysPattern("*")`
- the namespace prefix comes from `D`
- typed helpers reject already-prefixed storage patterns

## Common Pitfalls

- raw RESP commands use full storage keys, full key patterns, and full
  index names (e.g. `user:age`, not `age`)
- typed Go helpers use logical index names and document-scoped
  sub-patterns
- `SEARCHKEY`, `UPDATE`, and `KEYS` reject leading-wildcard patterns
- `SEARCHINDEX` requires indexes declared via `REGIDX` or
  `client.RegisterIndex`
- external connections must authenticate before using stateful commands
- memory-layer keys use `_m_:` prefix (with colon): `_m_:user:200`, not
  `_m_user:200`
