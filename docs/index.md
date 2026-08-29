# redisx docs

Pick the guide that matches what you want to do:

| If you want to… | Read… |
|---|---|
| Copy-paste working examples for every RESP command and Go client call | [howto.md](howto.md) |
| Understand dual-port startup, dual storage, AUTH, and the `x.KeyRange` algebra | [architecture.md](architecture.md) |
| Build a document-centric app with typed `x.Document` helpers | [typed-document.md](typed-document.md) |
| Add cross-cutting write concerns (DLP, encryption, CDC, cache invalidation) without touching call sites | [write-hooks.md](write-hooks.md) |
| Feed realtime websocket payloads into `x.Document` flows | [stream.md](stream.md) |

## Conventions used across the docs

These are snapshot reminders; the full specification lives in
[architecture.md](architecture.md).

- **Raw RESP wire commands** use full storage keys (e.g. `user:200`,
  or `_m_:user:200` for the memory layer).
  [Dual storage layer](architecture.md#dual-storage-layer).
- **Typed helpers** (`client.Set[D]`, `client.Get[D]`, …) take
  document-scoped inputs (e.g. `"200"` or `"*"`), never full storage keys.
  [typed-document.md](typed-document.md).
- **Storage routing** is deterministic: `_m_:<ns>:<id>` → memory layer;
  `<ns>:<id>` → disk layer.
  [Dual storage layer](architecture.md#dual-storage-layer).
- **Namespace convention** — the first `:` in a key separates `<namespace>`
  (including any `_m_:` prefix) from `<id>`. Pattern-based scans never
  cross namespaces by construction.
  [KeyRange & namespace convention](architecture.md#keyrange--namespace-convention).
