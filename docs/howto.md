# How-To

This guide complements the README with copy-paste oriented examples.

It covers:

- RESP command examples for every supported command
- the corresponding Go client calls where they help
- the typed JSON document API entry points for document-centric workflows

For intercepting writes (DLP gates, AES encryption, L1 cache invalidation,
CDC, audit logging, debug-fixture capture) without modifying individual
call sites, see **[write-hooks.md](write-hooks.md)** — the Write Hook
Subsystem overview, usage patterns, and troubleshooting guide.

## Setup

Assume the server is started like this:

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
func (UserDoc) TTL() time.Duration { return time.Hour }

home, _ := os.UserHomeDir()
dbPath := filepath.Join(home, ".redisx", "howto.db")

db := server.Start(
    "127.0.0.1:6380",
    dbPath,
    x.Idx[UserDoc]("age", "*", "age"),
    x.Idx[UserDoc]("email", "*", "email"),
)

_ = db.Set("_auth_:demo-key", "2")
```

The examples below assume:

- server address: `127.0.0.1:6380`
- auth key: `demo-key`
- auth config already exists as `_auth_:demo-key -> 2`

## Connection Commands

### `AUTH`

Authenticate before using any stateful command.

```text
AUTH demo-key
```

Expected response:

```text
OK
```

Go client:

```go
if err := client.Connect("127.0.0.1:6380", "demo-key"); err != nil {
    panic(err)
}
```

### `HELLO`

Inspect basic server metadata before or after authentication.

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

### `CLIENT`

`CLIENT` currently returns a simple acknowledgement.

```text
CLIENT
```

Expected response:

```text
OK
```

### `PING`

Health check for one authenticated connection.

```text
PING
```

Expected response:

```text
PONG
```

### `QUIT`

Close the current connection.

```text
QUIT
```

Expected response:

```text
OK
```

## Key-Value Commands

### `SET`

Store one string value.

```text
SET user:1 {"id":"1","name":"ken","age":18}
```

Expected response:

```text
OK
```

Go client:

```go
if err := client.Set("user:1", `{"id":"1","name":"ken","age":18}`); err != nil {
    panic(err)
}
```

### `SET` with `EX`

Store one value with a TTL in seconds.

```text
SET session:1 {"online":true} EX 60
```

### `SET` with `PX`

Store one value with a TTL in milliseconds.

```text
SET session:2 {"online":true} PX 1500
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
if err := client.SetWithTTL("cache:user:1", `{"name":"ken"}`, 30*time.Second); err != nil {
    panic(err)
}
```

### `SETNX`

Store one value only when the key does not already exist.

```text
SETNX user:2 {"id":"2","name":"alice"}
```

Expected response:

- `1` when the key was created
- `0` when the key already existed

Go client:

```go
ok, err := client.SetNX("user:2", `{"id":"2","name":"alice"}`)
if err != nil {
    panic(err)
}
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
raw, err := client.Get("user:1")
if err != nil {
    panic(err)
}
_ = raw
```

### `DEL`

Delete one key.

```text
DEL user:2
```

Expected response:

- `1` when the key existed
- `0` when the key did not exist

Go client:

```go
deleted, err := client.Delete("user:2")
if err != nil {
    panic(err)
}
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
- patterns like `_au*` are also rejected because reserved prefixes must stay explicit

Go client:

```go
res := client.Keys("user:*")
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

This command enters subscription mode and streams messages on the same connection.

Go client:

```go
ch := client.Subscribe("topic:user.updated")
msg := <-ch
_ = msg
```

### `PSUBSCRIBE`

Subscribe by pattern.

```text
PSUBSCRIBE topic:user.*
```

Example use case:

- `topic:user.created`
- `topic:user.updated`
- `topic:user.deleted`

All of them match `topic:user.*`.

Go client:

```go
ch := client.PSubscribe("topic:user.*")
msg := <-ch
_ = msg
```

## JSON Query And Update Commands

### `SEARCHINDEX`

Query JSON documents through one registered index.

```text
SEARCHINDEX user_age {"op":"pattern","p":"user:*"} {"age":{"$gte":18}}
```

More examples:

```text
SEARCHINDEX user_email {"op":"pattern","p":"user:*"} {"email":"ken@example.com"}
SEARCHINDEX user_age {"op":"pattern","p":"user:Engineering:*"} {"$and":[{"age":{"$gte":18}},{"status":"active"}]}
SEARCHINDEX user_age {"op":"gte","pivot":"user:engineering:"} {"age":{"$gte":18}} DESC
SEARCHINDEX user_age {"op":"pattern","p":"user:*"} {"age":{"$gte":18}} LIMIT 100
SEARCHINDEX user_age {"op":"pattern","p":"user:*"} {"age":{"$gte":18}} DESC LIMIT 50
```

Important constraints:

- `index_name` is the full runtime index name, such as `user_age`
- the second wire arg is a sealed `KeyRange` JSON payload (see the
  `SEARCHKEY` section for the full shape: `pattern`, `bt`, `gte`, `lte`,
  `gt`, `lt`)
- indexes must be registered at startup
- the index chooses the storage layer first
- a `KeyRange` routing that resolves to a **different storage layer**
  than the index is rejected
- `LIMIT N` is accepted both inside the `KeyRange` payload and as a
  trailing `LIMIT N` wire token (wire override wins)

Go client:

```go
kr := x.KeysPattern("user:*").Limit(100)
res := client.SearchIndex("user_age", kr, x.Gte("age", 18), false)
if res.IsError() {
    panic(res.Error())
}
raws := res.MustGet()
_ = raws
```

Typed JSON document API:

```go
docs := doc.SearchIndex[UserDoc]("age", x.KeysPattern("*"), x.Gte("age", 18), false)
typed := dbx.SearchIndex("age", x.KeysPattern("*"), x.Gte("age", 18), false)
_ = docs
_ = typed
```

Typed API rules:

- `idxName` is logical, such as `age`
- `scopedKR` is document-scoped, such as `x.KeysPattern("*")`
- the namespace prefix comes from `D`

### `SEARCHKEY`

Query JSON documents by scanning one storage-key range expressed as a sealed
`KeyRange` JSON object (see the six constructors below). `SEARCHKEY` was
upgraded in Issue #42 (FR: SEARCHKEY KeyRange) from a legacy glob string
argument to a structured JSON shape — the first positional argument must
be a JSON object, never a raw `"user:*"` string.

#### RESP wire format

```text
SEARCHKEY <keyrange_json> <json_filter> [ASC|DESC] [LIMIT count]
```

Argument shapes (strict — the server rejects any count of positional args
outside 2 / 3 / 4 / 5):

| argc after command | meaning |
|---|---|
| 2 | `{kr}` `{filter}` — ASC, no LIMIT |
| 3 | `{kr}` `{filter}` `ASC\|DESC` — direction only, default LIMIT=∞ |
| 4 | `{kr}` `{filter}` `LIMIT` `N` — count only, direction defaults to ASC |
| 5 | `{kr}` `{filter}` `ASC\|DESC` `LIMIT` `N` |

#### KeyRange JSON shape — 6 sealed constructors (one-of)

```json
{"op":"pattern","p":"user:*"}                          // glob: KeysPattern(p)
{"op":"gte",    "pivot":"user:100"}                    // [pivot, +∞)  KeysGte
{"op":"gt",     "pivot":"user:100"}                    // (pivot, +∞)  KeysGt
{"op":"lte",    "pivot":"user:100"}                    // (-∞, pivot]  KeysLte
{"op":"lt",     "pivot":"user:100"}                    // (-∞, pivot)  KeysLt
{"op":"bt",     "ge":"user:0100","lt":"user:0200"}     // [ge, lt) half-open KeysBt
```

Any `pivot` / `ge` / `lt` / `p` may be a literal key **or** a glob pattern
containing `* ? [`. Semantic rules you must be aware of:

- `KeysBt` is **half-open** `[ge, lt)` regardless of direction.
- Single-boundary ctors with a **glob pivot** (`KeysGte("order:p05*")` etc.)
  apply the **AND** of the dictionary boundary predicate and glob match, so
  e.g. `KeysLte("order:p04*")` is always empty (lex ≤ and glob match are
  disjoint sets).
- Glob patterns starting with a leading wildcard (`*...`, `?...`) are rejected
  by layer routing (the key range cannot pin a single storage layer).

`LIMIT` is accepted **both** as a trailing positional `LIMIT N` wire arg and
as an intrinsic field carried inside the sealed `KeyRange` object (Go API:
`kr.Limit(50)`). When both appear, the wire `LIMIT N` overrides (it re-applies
`kr.Limit(N)` after unmarshaling).

#### RESP examples

```text
SEARCHKEY {"op":"pattern","p":"user:*"} {"status":"active"}
SEARCHKEY {"op":"gte","pivot":"order:2024-01"} {"region":"us"} DESC
SEARCHKEY {"op":"bt","ge":"user:engineering:0100","lt":"user:engineering:0200"} {"total":{"$gte":100}} LIMIT 50
SEARCHKEY {"op":"pattern","p":"product:*"} {"$and":[{"category":"book"},{"price":{"$lte":20}}]} DESC LIMIT 10
```

Important constraints:

- `KeyRange` must resolve one concrete storage layer before iteration begins
  (handled by `x.LayerRoutingConstrained` — uses `Bounds()` / `Pattern()` to
  derive the leading anchor and then routes to either memory or disk layer).
- Patterns whose routing anchor starts with `*` or `?` are rejected (the
  caller must narrow the range so it pins a layer).

#### Go client (non-generic `[]string` raw-JSON API)

```go
// x.KeyRange sealed ctors + optional chained Limit()
kr := x.KeysPattern("user:*")
kr = x.KeysBt("user:engineering:0100", "user:engineering:0200").Limit(50)
kr = x.KeysGte("order:2024-01").Limit(200)

// Signature: SearchKey(kr x.KeyRange, filter x.Filter, desc bool)
res := client.SearchKey(
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

#### Typed JSON document API

```go
// doc.SearchKey[D] applies D's namespace + mem flag to build a KeyRange
// prefix automatically, then runs SEARCHKEY.
docs := doc.SearchKey[UserDoc](x.KeysPattern("*"), x.Eq("status", "active"), false)

// dbx typed helper mirrors the non-doc signature with a D-scoped prefix.
typed := dbx.SearchKey[UserDoc](x.KeysPattern("*"), x.Eq("status", "active"), false)
_ = docs
_ = typed
```

> Note — the `UPDATE` command still accepts the legacy `keyPattern string`
> form as an intentional write-path exception.



### `UPDATE`

Patch JSON documents matched by one full storage-key pattern and one filter.

```text
UPDATE user:* {"status":"pending"} {"status":"active"}
```

More examples:

```text
UPDATE user:* {"id":"1"} {"profile":{"age":18},"verified":true}
UPDATE user:* {"region":"us"} {"status":"reviewed"}
```

Important constraints:

- `key_pattern` must resolve one concrete storage layer
- patterns starting with `*` or `?` are rejected
- nested objects in `update_json` are supported

Go client:

```go
res := client.Update(
    "user:*",
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

Typed JSON document API:

```go
docs := doc.Update[UserDoc]("*", x.Eq("status", "pending"), x.Set("status", "active"))
typed := dbx.Update("*", x.Eq("status", "pending"), x.Set("status", "active"))
_ = docs
_ = typed
```

## Memory-Only Keys

Use the reserved `_m_` prefix for memory-only data:

```text
SET _m_session:1 {"online":true}
GET _m_session:1
KEYS _m_session:*
```

Go client:

```go
key := x.MemKey("session:1")
if err := client.Set(key, `{"online":true}`); err != nil {
    panic(err)
}
```

## Typed JSON Document API End-To-End

The client-side typed helper package lives at `client/doc`; examples import it
as `doc`.

Client mode:

```go
import (
    "github.com/kcmvp/redisx/client"
    doc "github.com/kcmvp/redisx/client/doc"
    "github.com/kcmvp/redisx/x"
)

if err := client.Connect("127.0.0.1:6380", "demo-key"); err != nil {
    panic(err)
}

_ = doc.Set(UserDoc(`{"id":"200","name":"Test","age":30}`))

got, _ := doc.Get[UserDoc]("200")
idx := doc.SearchIndex[UserDoc]("age", "*", x.Gte("age", 18), false)
keys := doc.Update[UserDoc]("*", x.Eq("id", "200"), x.Set("name", "Updated"))

_ = got
_ = idx
_ = keys
```

Embedded mode:

```go
dbx := server.As[UserDoc](db)

_ = dbx.Set(UserDoc(`{"id":"200","name":"Test","age":30}`))

got, _ := dbx.Get("200")
idx := dbx.SearchIndex("age", "*", x.Gte("age", 18), false)
keys := dbx.Update("*", x.Eq("id", "200"), x.Set("name", "Updated"))

_ = got
_ = idx
_ = keys
```

## Common Pitfalls

- raw RESP commands use full storage keys, full key patterns, and full index names
- typed JSON document API calls use logical index names and document-scoped sub-patterns
- `SEARCHKEY`, `UPDATE`, and `KEYS` reject leading-wildcard patterns
- `SEARCHINDEX` requires indexes declared during startup
- external connections must authenticate before using stateful commands
