<p align="center">
  <strong>Query JSON like MongoDB but with Typesafe. Talk RESP like Redis. Embed as a Go library.</strong>
  <br/>
  <br/>
  <a href="https://github.com/kcmvp/redisx/blob/main/LICENSE">
    <img alt="GitHub" src="https://img.shields.io/github/license/kcmvp/redisx"/>
  </a>
  <a href="https://pkg.go.dev/github.com/kcmvp/redisx">
    <img src="https://pkg.go.dev/badge/github.com/kcmvp/redisx.svg" alt="Go Reference"/>
  </a>
  <a href="https://github.com/kcmvp/redisx/actions/workflows/ci.yml">
     <img src="https://img.shields.io/github/actions/workflow/status/kcmvp/redisx/ci.yml?branch=main" alt="CI" />
  </a>
  <a href="https://app.codecov.io/gh/kcmvp/redisx">
    <img src="https://img.shields.io/codecov/c/github/kcmvp/redisx" alt="coverage"/>
  </a>
</p>

## The missing piece

There are lots of databases, but none of them give you everything at once:
fast embedded storage, JSON queries, and the Redis protocol you already
know. You've always had to pick:

- **Redis** — great protocol, but it stores strings. No JSON queries.
  And it's another process to run, monitor, and pay latency tax on.
- **MongoDB** — rich queries, but heavy. Overkill for an embedded need.
- **BoltDB / Badger** — fast KV, but you're back to manual indexing
  and stringly-typed everything.

**redisx** fills that gap — a single Go module you `go get` and forget
about.

## How does it compare?

| | Redis | MongoDB | redisx |
|---|---|---|---|
| Embed in your Go binary | ❌ | ❌ | ✅ |
| Query JSON by field values | ❌ | ✅ | ✅ |
| Use any Redis client | ✅ | ❌ | ✅ |
| Secondary indexes | ❌ | ✅ | ✅ |
| Type-safe Go API | ❌ | ❌ | ✅ |
| Write hooks (DLP, CDC, …) | ❌ | ❌ | ✅ |
| Runs without a separate server | ✅ | ✅ | ✅ |

## Start it in 30 seconds

Unstructured KV first — no schema needed:

```go
db := server.Start()
defer server.Stop()

db.Set("user:1", `{"id":"1","name":"ken","age":18}`)
db.Set("user:2", `{"id":"2","name":"alice","age":25}`)

db.Get("user:1").MustGet()
// → {"id":"1","name":"ken","age":18}
```

One function call. Data on disk. Done. Now connect any Redis client
and keep going:

```
$ redis-cli -p 7379
> AUTH <your-key>
> GET user:1
{"id":"1","name":"ken","age":18}
> KEYS user:*
1) user:1
2) user:2
```

## Typed documents in 30 seconds

For real apps, define a schema **once** with `x.RegisterSchema`, then
derive a 5-method `Document` type. `redisx` auto-derives storage keys,
writes with the correct default TTL, and routes mem vs disk based on the
`mem` flag you set at registration time.

```go
package main

import (
    "time"

    "github.com/kcmvp/redisx/client"
    "github.com/kcmvp/redisx/server"
    "github.com/kcmvp/redisx/x"
)

// 1. Register the schema (call once at init or boot time).
//    x.RegisterSchema[T](namespace, mem, ttl, keyAttrs...)
//    - namespace = "agg"   → storage key prefix "agg:"
//    - mem       = true    → layer is "_m_:agg:" (hot path in RAM)
//    - ttl       = 910s    → every write auto-expires in ~15 min
//    - keyAttrs  = symbol, tradeTime, aggId  → PK = agg:<symbol>:<tradeTime>:<aggId>
var AggTradeSchema = x.RegisterSchema[x.Document](
    "agg",
    true,
    910*time.Second,
    "symbol", "tradeTime", "aggId",
)

// 2. One type alias + 5 lines implements x.Document.
//    RawJSON() simply IS the string (zero-copy). The other 4 methods
//    delegate to the already-registered schema — no duplication.
type AggTrade string

func (a AggTrade) KeyAttrs() []string    { return AggTradeSchema.KeyAttrs() }
func (a AggTrade) Mem() bool             { return AggTradeSchema.Mem() }
func (a AggTrade) Namespace() string     { return AggTradeSchema.Namespace() }
func (a AggTrade) RawJSON() string       { return string(a) }
func (a AggTrade) TTL() time.Duration    { return AggTradeSchema.TTL() }

func main() {
    // 3. Start the server. Pass schemas directly to Start — they are
    //    registered server-side before the listener opens, so client
    //    writes always land on a known namespace. Start() reads
    //    redisx.yaml from CWD if present, or uses system defaults
    //    (ports 7379 app / 7381 ctrl, loopback bind, data at ~/.redisx).
    db := server.Start(AggTradeSchema)
    defer server.Stop()
    if db == nil {
        panic("redisx failed to start")
    }

    // 4. Bridge the typed client to the in-process server — no ports,
    //    no auth keys, no yaml required. ConnectEmbedded() auto-picks
    //    the ctrl listener that Start just opened and authenticates
    //    with the shared per-process internal key. For cross-process
    //    remote clients, use client.Connect(addr, auth) instead.
    if err := client.ConnectEmbedded(); err != nil {
        panic(err)
    }

    // 5. Writes — storage key auto-built from JSON attrs + schema keyAttrs.
    //    TTL auto-applied from the schema (910 s).
    _ = client.Set(AggTrade(`{"symbol":"BTCUSDT","tradeTime":1710000000000,"aggId":1,"price":68000}`))
    _ = client.Set(AggTrade(`{"symbol":"BTCUSDT","tradeTime":1710000000001,"aggId":2,"price":68001}`))

    // 6. Queries — typed reads return []AggTrade (not strings).
    //    Ordering is by primary storage key; pass true for DESC.
    recent := client.All[AggTrade](true).MustGet()   // DESC by key → newest first
    older  := client.All[AggTrade](false).MustGet()  // ASC  by key → oldest first
    _ = recent
    _ = older
}
```

That's it. One registration, one 5-liner type, and every `Set` /
`Get` / `All` / `SearchKey` / `SearchIndex` call is fully typed,
routed to the right layer, and auto-TTL'd.


## What else can it do?

- **JSON queries** — store JSON, query by field values, filter with
  conditions like `age >= 18`. No manual indexing, no string parsing.
- **Typed documents** — define a document type once. The library
  auto-derives storage keys, index names, and routing. Works with both
  the embedded DB and the remote Go client.
- **Write hooks** — intercept every write for data-loss prevention,
  encryption, change capture, or cache invalidation. Register once,
  applies everywhere. Zero call-site changes.
- **Pub/Sub** — publish and subscribe to topics, with pattern matching.
- **Memory + disk** — hot data in memory, durable data on disk. Routing
  is automatic, based on a simple key prefix.
- **Stream ingestion** — pipe websocket feeds straight into your
  document store, with auto-reconnect.

## Install

```bash
go get github.com/kcmvp/redisx
```

## Next steps

| | |
|---|---|
| [How-to](docs/howto.md) | Copy-paste examples for every command |
| [Architecture](docs/architecture.md) | How the server and storage layers work |
| [Typed documents](docs/typed-document.md) | Document contract and generic helpers |
| [Write hooks](docs/write-hooks.md) | Intercept writes for DLP, encryption, CDC |
| [Stream ingest](docs/stream.md) | Websocket ingestion with auto-reconnect |

## License

MIT
