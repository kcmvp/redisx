# How-To

This guide complements the README with copy-paste oriented examples.

It covers:

- RESP command examples for every supported command
- the corresponding Go client calls where they help
- the typed JSON document API entry points for document-centric workflows

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
defer client.Disconnect()
```

### `HELLO`

Inspect basic server metadata before or after authentication.

```text
HELLO
```

Example response fields:

```text
server: mresp
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
SEARCHINDEX user_age user:* {"age":{"$gte":18}}
```

More examples:

```text
SEARCHINDEX user_email user:* {"email":"ken@example.com"}
SEARCHINDEX user_age user:Engineering:* {"$and":[{"age":{"$gte":18}},{"status":"active"}]}
SEARCHINDEX user_age user:* {"age":{"$gte":18}} DESC
```

Important constraints:

- `index_name` is the full runtime index name, such as `user_age`
- `key_pattern` is one full storage-key pattern, such as `user:*`
- indexes must be registered at startup
- the index chooses the storage layer first
- a conflicting `key_pattern` is rejected

Go client:

```go
res := client.SearchIndex("user_age", "user:*", x.Gte("age", 18), false)
if res.IsError() {
    panic(res.Error())
}
raws := res.MustGet()
_ = raws
```

Typed JSON document API:

```go
docs := doc.SearchIndex[UserDoc]("age", "*", x.Gte("age", 18), false)
typed := dbx.SearchIndex("age", "*", x.Gte("age", 18), false)
_ = docs
_ = typed
```

Typed API rules:

- `idxName` is logical, such as `age`
- `keyPattern` is document-scoped, such as `*`
- the namespace prefix comes from `D`

### `SEARCHKEY`

Query JSON documents by scanning one full storage-key pattern.

```text
SEARCHKEY user:* {"status":"active"}
```

More examples:

```text
SEARCHKEY user:* {"region":"us"}
SEARCHKEY order:* {"total":{"$gte":100}} DESC
```

Important constraints:

- `key_pattern` must resolve one concrete storage layer
- patterns starting with `*` or `?` are rejected

Go client:

```go
res := client.SearchKey("user:*", x.Eq("status", "active"), false)
if res.IsError() {
    panic(res.Error())
}
raws := res.MustGet()
_ = raws
```

Typed JSON document API:

```go
docs := doc.SearchKey[UserDoc]("*", x.Eq("status", "active"), false)
typed := dbx.SearchKey("*", x.Eq("status", "active"), false)
_ = docs
_ = typed
```

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
defer client.Disconnect()

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
