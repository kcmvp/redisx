# Write Hook Subsystem

> ⬅️ [Back to README](../README.md)
> 📖 [Docs index](index.md)
> 🧱 [Architecture & KeyRange convention](architecture.md)
> 🧭 [How-to & examples](howto.md)
> 🏷️ [Typed document helpers](typed-document.md)
> 🔌 [Stream ingest](stream.md)

The Write Hook Subsystem lets you intercept every write through
`client.Set` / `raw.Set` **without modifying individual call sites**.
Register a hook once at process boot; it applies globally to every
subsequent write.

> **API location:** the public entry points are facade functions on the
> `client` package: `client.AddAbortHook`, `client.AddTransformHook`,
> `client.AddObserverHook`, `client.AddObserverAfterHook`,
> `client.RemoveHook`, `client.SetHookTimeout`. The hook engine lives
> at `client/internal/hook` — shared plumbing between `client` and
> `client/raw`, not importable outside the `client/` subtree.

Four semantic hook types are provided as distinct Go function signatures.
The type system enforces correct contracts: an `Abort` hook *must* return
an error, while an `Observer` hook *cannot* — this prevents accidental
"I'll just return nil here even though I'm an observer" bugs.

- Entry points
  - `client.AddAbortHook(h)` — fail-closed veto
  - `client.AddTransformHook(h)` — fail-closed rewrite
  - `client.AddObserverHook(h)` — fail-open before-write watch
  - `client.AddObserverAfterHook(h)` — fail-open post-write reaction
  - `client.RemoveHook(id)` — multi-removal of the same id is a safe no-op
  - `client.SetHookTimeout(d)` — `d > 0` sets the per-hook wall-clock
    timeout; `d <= 0` disables timeouts (panic isolation remains on)
  - Hook ids are process-local `uint64`; `0` means "invalid / no-op for Remove"

## Motivation

Without a framework-level primitive, every consumer project ends up stitching
the same cross-cutting concerns onto every call site:

- Pre-write policy gates (DLP, ACL, rate limits) are easy to forget at one
  call site → production data leaks.
- Post-write side effects (L1 invalidation, CDC, audit) are often omitted for
  one-off writes → cache dirty reads and audit blind spots.
- Observability debug fixtures (write capture for regression tests) rely on
  ad-hoc spliced logs, toggled with feature flags.
- Each team independently re-implements panic handling and slow-path timeouts
  for their hooks. One buggy hook hangs or aborts writes in production.

The Write Hook Subsystem centralises these once: register once, globally
effective, zero business call-site changes, uniform safety net across every
hook.

## Execution order

All Before-phase hooks complete their synchronous lifecycle **before** `Set`
returns — even if the caller uses an outer `context.WithTimeout`. This is a
guarantee for capture-style Observer hooks that must not truncate.

The five synchronous phases, in order:

| Phase | Type | Value passed | On error/panic/timeout |
|---|---|---|---|
| 1 | **Abort** (all registered, in registration order) | original `value` bytes | **ABORT** the write immediately. Subsequent phases run only if every Abort returned `nil`. |
| 2 | **Transform** (all registered, in registration order) | output of previous Transform; original value for the first one | **ABORT** on any error; otherwise each Transform feeds its `newValue` into the next. |
| 3 | **Observer** — the "Before" observer | **post-Transform** value (i.e. what will actually be written) | Only a `slog` warning/error is produced; the write and sibling observer hooks continue unaffected (fail-open). |
| 4 | Actual Redis `SET` / `SETNX` / `SETNX + TTL` | final value from phase 2 | Propagated to the caller as the `Set` return value; also passed to phase 5. |
| 5 | **ObserverAfter** (all registered, in registration order) | final written value + `committed bool` + `writeErr` (phase 4 result: `committed=true` iff actual data change occurred) | Only a `slog` warning/error is produced on panic/timeout (fail-open). |

Important invariants derived from the order:

- If **any Abort aborts**, phases 2–5 do **not** run. No Redis key is
  touched. No ObserverAfter fires.
- If **any Transform returns an error**, phases 3–5 do **not** run. No
  Redis key is touched. No ObserverAfter fires.
- Observer (Before) always sees the **transformed** value, not the
  original. This is intentional: "what will actually be written" is the value
  that capture fixtures and metrics must record. If you need the pre-transform
  value for policy, that logic belongs in Abort or Transform.

### Fail-policy matrix at a glance

| Hook type | Error | Panic | Timeout |
|---|---|---|---|
| **Abort** | ❌ abort write | ❌ abort write | ❌ abort write |
| **Transform** | ❌ abort write | ❌ abort write | ❌ abort write |
| **Observer** (Before) | N/A (no error return) | ✅ log only | ✅ log only |
| **ObserverAfter** | N/A (no error return) | ✅ log only | ✅ log only |

This is **non-negotiable per hook type**. If you find yourself wanting
"fail-closed observers" you should instead model the logic as an Abort or
a Transform — the type signatures are the tool that helps you notice the
semantic mismatch.

## Safety-net composition rules

The implementation enforces two always-on safety nets **per hook invocation**.

### Panic recovery

Each call to a user-supplied hook body runs inside an inner `defer recover()`
that translates a panic into a returned error (for Abort/Transform) or into a
logged-and-ignored event (for Observer*). A panic inside your hook body can
**never** propagate out of the hook infrastructure. Your `Set(...)` call will
at worst get a wrapped `fmt.Errorf("hook Abort#0 panic: boom")` error. The
stack trace is always written to `slog.Error` at key `"stack"`.

### Timeout

Each hook runs inside a one-shot goroutine. The dispatcher then does:

```
select {
case err := <-done:   return err          // hook body completed
case <-timer.C:       return timeout err  // wall-clock d elapsed
}
```

`d` is `getHookTimeout()` (default 100 ms, or whatever you set via
`SetHookTimeout`).

Rule of thumb for production: set `SetHookTimeout(T)` to **50–80% of your
tightest outer `Set` deadline** — that way the hook layer always attributes
slow hooks before the outer timeout blames "the whole call".

### Abort and Transform timeout/panic = fail-closed (ABORT)

Non-negotiable security default. A policy-gate hook that panics or hangs is
functionally equivalent to "we can't verify the policy"; the write is
rejected until the hook is fixed.

If you really want "best-effort gating that never blocks", put that logic
inside an **Observer** (which never aborts), not inside an Abort.

### Observer Hook timeout/panic = fail-open (LOG ONLY)

Observers are side effects. They must never impact the write. A crash or
hang in an audit hook or an L1 invalidation hook:

- logs once via `slog.Error` (panic) or `slog.Warn` (timeout)
- never reaches the `Set` return value
- subsequent sibling observer hooks still run

## Hook-by-hook usage patterns

### Abort — "reject bad writes"

Use for policy gates that must run before the key touches storage.

Signature: `func(key string, value []byte) error`
Returning `nil` means "allow"; any non-`nil` error **stops everything**.

**Payload-size DLP gate**

```go
id := client.AddAbortHook(func(key string, value []byte) error {
    if len(value) > 8*1024*1024 {
        return fmt.Errorf("redisx: reject key %s, size %d > 8 MiB limit",
            key, len(value))
    }
    return nil
})
```

**ACL key-prefix gate**

```go
client.AddAbortHook(func(key string, value []byte) error {
    if strings.HasPrefix(key, "billing:") {
        if !securityCtx.AllowedByCurrentCaller("billing-write") {
            return securityCtx.ErrDenied
        }
    }
    return nil
})
```

**Rate limit (token bucket)**

```go
client.AddAbortHook(func(key string, value []byte) error {
    if !rateBucket.TryTake() {
        return errors.New("redisx: per-process write rate exhausted")
    }
    return nil
})
```

### Transform — "rewrite what gets written"

Use for AES encryption, gzip/deflate, schema-version prefixing, payload
normalization, or any other "fresh-copy → store" step.

Signature: `func(key string, value []byte) (newValue []byte, err error)`

The contract:

- **Return a fresh byte slice.** Never mutate `value` in-place.
- Chains in registration order. Hook 1's `newValue` is Hook 2's `value`.
- Returning `err != nil` aborts the write.

**Append a JSON "v1" schema-version field**

```go
client.AddTransformHook(func(key string, value []byte) ([]byte, error) {
    if len(value) == 0 || value[0] != '{' {
        return nil, fmt.Errorf(
            "redisx: transform: expected JSON object, got %q",
            value[:min(16, len(value))])
    }
    out := make([]byte, 0, len(`{"schema":"v1","doc":}`)+len(value))
    out = append(out, []byte(`{"schema":"v1","doc":`)...)
    out = append(out, value...)
    out = append(out, '}')
    return out, nil
})
```

**gzip with a size threshold**

```go
client.AddTransformHook(func(key string, value []byte) ([]byte, error) {
    if len(value) < 512 {
        return value, nil
    }
    var buf bytes.Buffer
    w := gzip.NewWriter(&buf)
    if _, err := w.Write(value); err != nil {
        _ = w.Close()
        return nil, err
    }
    if err := w.Close(); err != nil {
        return nil, err
    }
    return buf.Bytes(), nil
})
```

### Observer (Before) — "watch what will be written"

Use for debug capture, metrics counters, test fixtures, and anything that
needs the *final* bytes before storage but must never block a write.

Signature: `func(key string, value []byte)` — no error return.

**Capture user-doc writes to regression fixtures**

```go
var mu sync.Mutex
var caps [][]byte
captureID := client.AddObserverHook(func(key string, value []byte) {
    if strings.HasPrefix(key, "user:") {
        mu.Lock()
        caps = append(caps, append([]byte(nil), value...))
        mu.Unlock()
    }
})
```

**Sampled debug log**

```go
client.AddObserverHook(func(key string, value []byte) {
    if debugSample.Allow() {
        slog.Debug("redisx write sample", "key", key,
            "size", len(value), "prefix", firstBytes(value, 16))
    }
})
```

### ObserverAfter — "react to a completed write"

Use for CDC, L1 cache invalidation, audit logs that must include the
success/failure of the Redis op.

Signature: `func(key string, value []byte, committed bool, writeErr error)` — no error return.

`committed` tells you whether the key actually changed in Redis:

- `Set` / `SetWithTTL`: `committed = (writeErr == nil)`
- `SetNX` / `SetNXWithTTL`: `committed = ok && (writeErr == nil)` — so if
  the key already existed (`ok=false`), `committed` is `false` even when no
  error occurred.

**L1 cache evict on write commit success only**

```go
client.AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
    if !committed {
        return
    }
    go localL1.Evict(key)
})
```

**CDC to a Kafka topic**

```go
client.AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
    if !committed {
        cdcDropped.Add(1)
        return
    }
    cdcBatch.Enqueue(cdcEvent{Key: key, ValueLen: len(value), At: time.Now()})
})
```

## Lifecycle — when to add or remove hooks

- **Add once, early.** All registration functions are O(n) copy-on-write
  slice swaps. The intended pattern is: **register all hooks during process
  boot and leave them in place.**
- **`client.RemoveHook(id)` exists for tests** and for hot-reload style
  plugins. Removing a hook is also O(n) CoW.
- Concurrent `RemoveHook` + `Set` is safe. The running `Set` uses the
  registry snapshot it loaded when the write started.
- Timeout is **global** (`client.SetHookTimeout(d)`) — it applies to every
  hook of every type.

## Value read-only contract — except Transform returns a fresh copy

- `Abort(k, value)`: `value` is READ-ONLY. Do not write into it.
- `Transform(k, value)`: `value` is READ-ONLY. You MUST return a
  freshly-allocated slice for `newValue`.
- `Observer(k, value)` and `ObserverAfter(k, value, committed, writeErr)`:
  `value` is READ-ONLY. If you need to retain the bytes, copy them:
  `v := append([]byte(nil), value...)`.

## Troubleshooting

### Q: My hook is aborting writes but I thought it was "just an observer"

You used `AddAbortHook` (returns `error`) or `AddTransformHook` (returns
`error`) for logic that should never impact the write. Move it to
`AddObserverHook` or `AddObserverAfterHook` — their signatures give them no
way to return an error to `Set`, which is enforced by the compiler.

### Q: Observer sees encrypted bytes but I wanted the plain JSON

Observer hooks run after Abort and Transform. Capture the bytes you want
inside Transform (pre-encrypt or at the right stage of the chain) and
copy them out.

### Q: I set `SetHookTimeout(1 * time.Second)` but `Set` is still faster / slower

The timeout is **per hook**. If you register 4 ObserverBefore hooks + 4
ObserverAfter hooks, worst-case Before phase = 4 × 1 s on pathological
hangs (each hook times out sequentially). Keep `T` small (default 100 ms)
or for very heavy observers use `SetHookTimeout(0)` and manage timeouts
inside the hook's own goroutine.

### Q: After a test runs, subsequent tests see the hooks from earlier tests

Call `client.RemoveHook(id)` in test cleanup. Hooks are not reset by
`client.Disconnect`, because they are a property of the process-level
hook registry, not of a specific connection.

## API cheat sheet

```go
type HookID = hook.ID  // uint64

// Registration (client package)
func AddAbortHook(h func(key string, value []byte) error) HookID
func AddTransformHook(h func(key string, value []byte) ([]byte, error)) HookID
func AddObserverHook(h func(key string, value []byte)) HookID
func AddObserverAfterHook(h func(key string, value []byte, committed bool, writeErr error)) HookID
func RemoveHook(id HookID)               // removing 0 or unknown id is safe
func SetHookTimeout(d time.Duration)     // d<=0 disables timeouts; panic recover stays on
```

That's it. Register once; rely on the framework to dispatch correctly,
abort or continue with the right fail policy, and log + recover if a hook
misbehaves.
