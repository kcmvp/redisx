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
| Embed in your Go binary | ✗ | ✗ | ✓ |
| Query JSON by field values | ✗ | ✓ | ✓ |
| Use any Redis client | ✓ | ✗ | ✓ |
| Secondary indexes | ✗ | ✓ | ✓ |
| Type-safe Go API | ✗ | ✗ | ✓ |
| Write hooks (DLP, CDC, …) | ✗ | ✗ | ✓ |
| Runs without a separate server | ✓ | ✓ | ✓ |

## Start it in 30 seconds

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
