# redisx docs

Pick the guide that matches what you want to do:

| If you want to… | Read… |
|---|---|
| Copy-paste working command examples for RESP (`SET`, `GET`, `SEARCHINDEX`, `SEARCHKEY`, `UPDATE`, pub/sub, …) and their matching Go client + typed document equivalents | [howto.md](howto.md) |
| Understand how the system is put together: dual storage layer, how to start a server, how AUTH works, and the `x.KeyRange` algebra + `:` namespace convention that drives JSON commands | [architecture.md](architecture.md) |
| Build a document-centric app with the typed `x.Document` helpers (`client/doc` for remote / `server.As[D]` for embedded) | [typed-document.md](typed-document.md) |
| Add cross-cutting write concerns — DLP / ACL / rate gates, encryption-at-rest, debug-fixture capture, L1 cache invalidation, CDC — without modifying individual `Set` call sites | [write-hooks.md](write-hooks.md) |
| Feed realtime websocket payloads into `x.Document` flows, with reconnect and add/remove subscription semantics | [stream.md](stream.md) |

## Conventions used across the docs

These are **snapshot reminders**; the full locked specification for each
item lives under the linked section of
[architecture.md](architecture.md).

- **Raw RESP wire commands** always use full storage keys (e.g. `user:200`,
  or memory-layer `_m_user:200`).
  [Dual storage layer](architecture.md#dual-storage-layer).
- **Typed document helpers** always take document-scoped inputs (e.g.
  `"200"` or `"*"`), never full storage keys. See
  [typed-document.md](typed-document.md).
- **Storage-layer routing** is deterministic: `_m_<ns>:<id>` → memory
  layer; `<ns>:<id>` → primary disk layer. See
  [Dual storage layer](architecture.md#dual-storage-layer).
- **Namespace convention for JSON commands** — the first `:` in a key
  separates `<namespace>` (including any `_m_` prefix) from `<id>`.
  Pattern-based scans never cross namespaces by construction; typed helpers
  scope into your document's namespace so you never need to think about it.
  See
  [KeyRange & namespace convention](architecture.md#keyrange-namespace-convention).
