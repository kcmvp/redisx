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

## What

**redisx** is an embedded, high-performance JSON document store with a
Redis-compatible wire protocol. Stored strings are JSON payloads that you can
query and patch directly — through standard RESP commands **or** a typed Go
document layer.

## Features

- **RESP server & client** — drop-in Redis-compatible subset (AUTH / HELLO /
  PING / SET / GET / DEL / KEYS / PUBLISH / SUBSCRIBE / PSUBSCRIBE).
  [Quick examples](docs/howto.md).
- **JSON document commands** — `SEARCHINDEX`, `SEARCHKEY`, `UPDATE` work on
  storage keys, JSON filters, and in-place JSON patches. Powered by a sealed
  `x.KeyRange` algebra (6 constructors, Limit, mem/disk routing) and a
  strict `:`-based namespace convention.
  [Command reference & JSON examples](docs/howto.md#searchindex)
  · [KeyRange & namespace convention](docs/architecture.md#keyrange-namespace-convention)
- **Typed document API (`x.Document`)** — define a doc type once
  (`type UserDoc string`), then write/logical-key/pattern/index inputs are
  scoped automatically.
  [Contract & API semantics](docs/typed-document.md).
- **Write Hook Subsystem** — register once at boot, apply across every
  future `Set` / `SetNX` (and their typed helpers): four typed hook kinds
  (Abort / Transform / Observer-Before / Observer-After) with distinct
  fail-policy contracts, panic isolation, and per-hook timeout safety net.
  [Full contract & real-world patterns](docs/write-hooks.md).
- **Dual storage layer** — primary disk-backed layer (`dbPath`) plus a
  dedicated in-memory layer for `_m_`-prefixed keys; routing is key-prefix
  based and scans never cross layers.
  [Storage architecture & startup](docs/architecture.md#dual-storage-layer)
- **Stream ingestion (`ingest/stream`)** — optional reconnecting websocket
  ingestion extension that pipes external payloads straight into
  `x.Document` workflows, with add/remove subscription support.
  [Stream ingest guide](docs/stream.md).

## Install

```bash
go get github.com/kcmvp/redisx
```

## Docs

All how-tos, architecture, and API contracts live under `docs/`:

| File | What it covers |
|---|---|
| [docs/index.md](docs/index.md) | Docs landing page — pick the right guide for your task. |
| [docs/howto.md](docs/howto.md) | Copy-paste examples for every RESP command, Go client call, and typed document end-to-end flow. |
| [docs/architecture.md](docs/architecture.md) | Dual storage layer, server startup, AUTH model, the `x.KeyRange` algebra, and `:`-namespace convention. |
| [docs/typed-document.md](docs/typed-document.md) | The `x.Document` contract and the input semantics of typed helpers on both the RESP client and embedded-server sides. |
| [docs/write-hooks.md](docs/write-hooks.md) | Write Hook Subsystem — hook kinds, fail-policy matrix, safety-net composition, real-world usage patterns, and troubleshooting. |
| [docs/stream.md](docs/stream.md) | WebSocket stream ingestion for `x.Document` workflows — reconnect and subscription-aware behavior. |

## License

MIT
