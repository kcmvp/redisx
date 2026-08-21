# Write Hook Subsystem

> 📖 [Docs index](index.md) · ⬅️ [Back to README](../README.md) ·
> 🧱 [Architecture & KeyRange convention](architecture.md) ·
> 🧭 [How-to & examples](howto.md) ·
> 🏷️ [Typed document helpers](typed-document.md) ·
> 🔌 [Stream ingest](stream.md)

The Write Hook Subsystem lets you intercept every write through `client.Set` /
`doc.Set` **without modifying individual call sites**. Register a hook once at
process boot; it applies globally to every subsequent write.

Four semantic hook types are provided as distinct Go function signatures. The
type system enforces correct contracts: an AbortHook *must* return an error,
while an ObserverHook *cannot* — this prevents accidental “I’ll just return
nil here even though I’m an observer” bugs.

- Entry points
  - Raw bytes: `client.AddAbortHook`, `client.AddTransformHook`, `client.AddObserverHook`, `client.AddObserverAfterHook`
  - Typed JSON documents: `doc.AddAbortHook`, `doc.AddTransformHook`, `doc.AddObserverHook`, `doc.AddObserverAfterHook` — thin forwarders; parameter is named `valueJSON` to make the JSON-contract visible at call sites.
  - `client.RemoveHook(id)` — multi-removal of the same id is a safe no-op.
  - `client.SetHookTimeout(d)` — `d > 0` sets the per-hook wall-clock timeout; `d <= 0` disables timeouts (panic isolation remains on).
  - Hook ids are process-local `uint64`; `0` means “invalid / no-op for RemoveHook”.

For a quick tour and the API summary table, see the **Write Hook Subsystem**
section of the top-level README.

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
| 1 | **AbortHook** (all registered, in registration order) | original `value` bytes | **ABORT** the write immediately. Subsequent phases run only if every AbortHook returned `nil`. |
| 2 | **TransformHook** (all registered, in registration order) | output of previous TransformHook; original value for the first one | **ABORT** on any error; otherwise each TransformHook feeds its `newValue` into the next. |
| 3 | **ObserverHook** — the “Before” observer | **post-Transform** value (i.e. what will actually be written) | Only a `slog` warning/error is produced; the write and sibling observer hooks continue unaffected (fail-open). |
| 4 | Actual Redis `SET` / `SETNX` / `SETNX + TTL` | final value from phase 2 | Propagated to the caller as the `Set` return value; also passed to phase 5. |
| 5 | **ObserverAfterHook** (all registered, in registration order) | final written value + `committed bool` + `writeErr` (phase 4 result: `committed=true` iff actual data change occurred) | Only a `slog` warning/error is produced on panic/timeout (fail-open). |

Important invariants derived from the order:

- If **any AbortHook aborts**, phases 2–5 do **not** run. No Redis key is
  touched. No ObserverAfterHook fires.
- If **any TransformHook returns an error**, phases 3–5 do **not** run. No
  Redis key is touched. No ObserverAfterHook fires.
- ObserverHook (Before) always sees the **transformed** value, not the
  original. This is intentional: “what will actually be written” is the value
  that capture fixtures and metrics must record. If you need the pre-transform
  value for policy, that logic belongs in AbortHook or TransformHook.

### Fail-policy matrix at a glance

| Hook type | Error | Panic | Timeout |
|---|---|---|---|
| **AbortHook** | ❌ abort write | ❌ abort write | ❌ abort write |
| **TransformHook** | ❌ abort write | ❌ abort write | ❌ abort write |
| **ObserverHook** (Before) | N/A (no error return) | ✅ log only | ✅ log only |
| **ObserverAfterHook** | N/A (no error return) | ✅ log only | ✅ log only |

This is **non-negotiable per hook type**. If you find yourself wanting
“fail-closed observers” you should instead model the logic as an AbortHook or
a TransformHook — the type signatures are the tool that helps you notice the
semantic mismatch.

## Safety-net composition rules

The implementation enforces two always-on safety nets **per hook invocation**.
They compose as follows.

### Double recover — innermost-first, stack-safe, no side effects

Each call to a user-supplied hook body runs inside an inner `defer recover()`
that translates a panic into a returned error (for Abort/Transform) or into a
logged-and-ignored event (for Observer*). If the dispatch goroutine itself
also panicked (hypothetical), the outermost scope in the caller treats that
the same way. The first recover wins ("innermost-first"), so the hook label
remains attached to the panic log.

For end users this means: **a `panic()` inside your hook body can never
propagate out of the hook infrastructure**. Your `Set(...)` call will at worst
get a wrapped `fmt.Errorf("hook Abort#0 panic: boom")` error. The stack trace
is always written to `slog.Error` at key `"stack"`.

### Double timeout — user timeout vs framework timeout

Each hook runs inside a one-shot goroutine. The dispatcher then does:

```
select {
case err := <-done:   return err          // hook body completed
case <-timer.C:       return timeout err  // wall-clock d elapsed
}
```

`d` is `getHookTimeout()` (default 100 ms, or whatever you set via
`SetHookTimeout`).

If the caller also wraps their `Set(...)` in an outer `context.WithTimeout`:

- **Outer user timeout < framework timeout.** The outer context can cancel
  first. The hook goroutine will finish in the background (its work is
  synchronous within the hook), but the caller sees their own outer deadline
  first. No duplicate `slog` from the hook layer.
- **Outer user timeout > framework timeout.** The hook timeout fires first
  and the hook infrastructure emits a `slog.Warn("write hook timeout" ...)`.
  If the user’s outer timeout then fires later, the caller also sees their
  own deadline error. You get **duplicate logs** from two distinct layers.
  Fix: either keep the framework timeout strictly smaller than any outer
  user timeout, or use per-hook timeouts combined with a global
  `SetHookTimeout(0)` and rely entirely on outer user timeouts.

Rule of thumb for production: set `SetHookTimeout(T)` to **50–80% of your
tightest outer `Set` deadline** — that way the hook layer always attributes
slow hooks before the outer timeout blames “the whole call”.

### AbortHook and TransformHook timeout/panic = fail-closed (ABORT)

Non-negotiable security default. A policy-gate hook that panics or hangs is
functionally equivalent to “we can’t verify the policy”; the write is
rejected until the hook is fixed.

If you really want “best-effort gating that never blocks”, put that logic
inside an **ObserverHook** (which never aborts), not inside an AbortHook.

### Observer*Hook timeout/panic = fail-open (LOG ONLY)

Observers are side effects. They must never impact the write. A crash or
hang in an audit hook or an L1 invalidation hook:

- logs once via `slog.Error` (panic) or `slog.Warn` (timeout)
- never reaches the `Set` return value
- subsequent sibling observer hooks still run (dispatch iterates through the
  whole slice independently per hook)

If an observer hook needs to carry state across calls (batching metrics,
asynchronous forwarding), handle threading inside the hook itself — the
framework dispatches hooks synchronously so you can batch internally, or
spawn a goroutine inside the hook body to fan out.

## Hook-by-hook usage patterns

### AbortHook — "reject bad writes"

Use for policy gates that must run before the key touches storage.

Signature: `func(key string, value []byte) error`
Returning `nil` means “allow”; any non-`nil` error **stops everything**.

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

**ACL key-prefix gate (e.g. user-level writes can only touch `u:<uid>:*`)**

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

Key design notes for AbortHook:

- Keep logic CPU-cheap. If it needs I/O (e.g. remote policy service), either
  accept the latency cost of the per-hook timeout aborting the write, or
  move the check to an async gate *before* `Set` and only use AbortHook for
  the fast in-memory path.
- If AbortHook reads a shared map, protect it yourself. The hook body runs
  in the shared hook-dispatch goroutine, concurrent with other writes.
- AbortHook runs **first** and sees the original bytes. Any encoding or
  encryption happens later in TransformHook, so size checks here are against
  the pre-transform value.

### TransformHook — "rewrite what gets written"

Use for AES encryption, gzip/deflate, schema-version prefixing, payload
normalization, or any other “fresh-copy → store” step.

Signature: `func(key string, value []byte) (newValue []byte, err error)`

The contract:

- **Return a fresh byte slice.** Never mutate `value` in-place. Downstream
  Observer hooks and the actual Redis writer all receive the same slice
  returned here; in-place mutation would expose transient state to sibling
  logic running in the same dispatch.
- Chains in registration order. Hook 1’s `newValue` is Hook 2’s `value`.
- Returning `err != nil` aborts the write, including skipping all later
  transforms, Observer hooks, and the actual Redis write.

**Append a JSON "v1" schema-version field (typed docs)**

This pattern wraps an existing JSON object with `{"schema":"v1","doc":<original>}`
so readers can detect the version and evolve payloads without losing
compatibility. This is a realistic example; the earlier sketch (“prefix with a
bare `"v1",`”) produces invalid JSON and is only for illustration.

```go
doc.AddTransformHook(func(key string, valueJSON []byte) ([]byte, error) {
    if len(valueJSON) == 0 || valueJSON[0] != '{' {
        return nil, fmt.Errorf(
            "redisx: transform: expected JSON object, got %q",
            valueJSON[:min(16, len(valueJSON))])
    }
    out := make([]byte, 0, len(`{"schema":"v1","doc":}`)+len(valueJSON))
    out = append(out, []byte(`{"schema":"v1","doc":`)...)
    out = append(out, valueJSON...)
    out = append(out, '}')
    return out, nil
})
```

**AES-GCM envelope (skeleton)**

```go
client.AddTransformHook(func(key string, value []byte) ([]byte, error) {
    if aesKey == nil {
        return nil, errors.New("redisx: AES key not loaded, refusing clear-text write")
    }
    nonce := make([]byte, 12)
    if _, err := rand.Read(nonce); err != nil {
        return nil, err
    }
    ct := aesgcm.Seal(nil, nonce, value, []byte(key))
    out := make([]byte, 0, len(nonce)+len(ct))
    out = append(out, nonce...)
    out = append(out, ct...)
    return out, nil
})
```

**gzip with a size threshold (only pay CPU when it pays off)**

```go
client.AddTransformHook(func(key string, value []byte) ([]byte, error) {
    if len(value) < 512 {
        return value, nil // no transformation — return the input unchanged, still a fresh contract
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

### ObserverHook (Before) — "watch what will be written"

Use for debug capture, metrics counters, test fixtures, and anything that
needs the *final* bytes before storage but must never block a write.

Signature: `func(key string, value []byte)` — no error return.

The framework treats both panics and timeouts as **fail-open**: log, then
continue with the write and with sibling observers.

**Capture user-doc writes to regression fixtures (table-driven test seed)**

```go
var mu sync.Mutex
var caps [][]byte
captureID := doc.AddObserverHook(func(key string, valueJSON []byte) {
    if strings.HasPrefix(key, "user:") {
        mu.Lock()
        caps = append(caps, append([]byte(nil), valueJSON...))
        mu.Unlock()
    }
})
```

**Counter + histogram (Prometheus-style; adapt to your metrics library)**

```go
client.AddObserverHook(func(key string, value []byte) {
    writesTotal.Inc()
    writeBytes.Observe(float64(len(value)))
    keyPrefix.WithLabelValues(firstPrefix(key)).Inc()
})
```

**Sampled debug log (cheap, never noisy)**

```go
client.AddObserverHook(func(key string, value []byte) {
    if debugSample.Allow() {
        slog.Debug("redisx write sample", "key", key,
            "size", len(value), "prefix", firstBytes(value, 16))
    }
})
```

Observer hooks run after Abort + Transform. The value they see is the
post-policy, post-encryption bytes. This is almost always what debug fixtures
and audit snapshots need. If you specifically need the clear-text bytes,
record them inside TransformHook (where they still exist) and pass them on —
never re-derive “original input” from the output of another hook.

### ObserverAfterHook — "react to a completed write"

Use for CDC, L1 cache invalidation, audit logs that must include the
success/failure of the Redis op, and gradual dual-write migrations.

Signature: `func(key string, value []byte, committed bool, writeErr error)` — no error return.

`committed` tells you whether the key actually changed in Redis:

- `Set` / `SetWithTTL`: `committed = (writeErr == nil)`
- `SetNX` / `SetNXWithTTL`: `committed = ok && (writeErr == nil)` — so if
  the key already existed (`ok=false`), `committed` is `false` even when no
  error occurred. This is the critical semantic that distinguishes “the
  command ran fine” from “a *new value was actually written*”.

Same fail-open guarantees as ObserverHook.

**L1 cache evict on write commit success only (safe for SetNX)**

```go
client.AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
    if !committed {
        return // SetNX ok=false or network failure — don't evict L1, stale value is still valid
    }
    go localL1.Evict(key) // async — you decide, framework dispatch is sync
})
```

**CDC to a Kafka topic (batching inside the hook is fine)**

```go
client.AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
    if !committed {
        cdcDropped.Add(1)
        return
    }
    cdcBatch.Enqueue(cdcEvent{Key: key, ValueLen: len(value), At: time.Now()})
})
```

**Dual-write migration (old redisx → new external store, track divergence)**

```go
client.AddObserverAfterHook(func(key string, value []byte, committed bool, writeErr error) {
    if !committed {
        return
    }
    // Go routine inside the hook: CDC is side effect, not on the hot path.
    go func(k string, v []byte) {
        if err := newStore.Put(k, v); err != nil {
            slog.Warn("dual-write target diverged", "key", k, "err", err)
            migrationDivergence.Add(1)
        }
    }(key, append([]byte(nil), value...))
})
```

## Lifecycle — when to add or remove hooks

- **Add once, early.** All registration functions are O(n) copy-on-write
  slice swaps. Doing thousands of `Add*Hook` / `RemoveHook` in a loop is
  wasteful. The intended pattern is: **register all hooks during process
  boot (or during test suite setup) and leave them in place.**
- **`RemoveHook(id)` exists for tests** and for hot-reload style plugins
  that manage their own lifecycle. Removing a hook is also O(n) CoW.
- Concurrent `RemoveHook` + `Set` is safe. The running `Set` uses the
  registry snapshot it loaded when the write started; new `Set`s either see
  the pre-removal or post-removal snapshot, never a half-swap.
- There is intentionally no “unregister all hooks” public API. Use
  individual ids in tests; the library uses `resetHooks()` internally.
- Timeout is **global** (`SetHookTimeout(d)`) — it applies to every hook of
  every type. If different hooks need vastly different time budgets, split
  the work: keep only the fast synchronous check in the hook, and hand the
  heavy work to a worker pool via an `ObserverAfterHook` + goroutine.

## Value read-only contract — except TransformHook returns a fresh copy

- `AbortHook(k, value)`: `value` is READ-ONLY. Do not write into it.
- `TransformHook(k, value)`: `value` is READ-ONLY. You MUST return a
  freshly-allocated slice for `newValue`.
- `ObserverHook(k, value)` and `ObserverAfterHook(k, value, committed, writeErr)`:
  `value` is READ-ONLY. If you need to retain the bytes (e.g. for async
  forwarding), copy them: `v := append([]byte(nil), value...)`.

Violating this contract (writing to the shared slice, returning the input
slice after in-place mutation) produces **undefined behaviour**: downstream
sibling hooks, the actual Redis writer, and any future safety-net layer that
records the pre/post payload may see a torn, partially-mutated byte array.
Treat all inputs as immutable; TransformHook is the *only* allowed rewrite
point, and only via a freshly-returned copy.

## Troubleshooting

### Q: My hook is aborting writes but I thought it was “just an observer”

You used `AddAbortHook` (returns `error`) or `AddTransformHook` (returns
`error`) for logic that should never impact the write. Move it to
`AddObserverHook` or `AddObserverAfterHook` — their signatures give them no
way to return an error to `Set`, which is enforced by the compiler.

### Q: ObserverHook sees encrypted bytes but I wanted the plain JSON

Observer hooks run after Abort and Transform. Capture the bytes you want
inside TransformHook (pre-encrypt or at the right stage of the chain) and
copy them out; do not rely on ObserverHook to replay a value that has
already been transformed.

### Q: I set `SetHookTimeout(1 * time.Second)` but `Set` is still faster / slower

The timeout is **per hook**. If you register 4 ObserverBefore hooks + 4
ObserverAfter hooks, worst-case Before phase = 4 × 1 s on pathological
hangs (each hook times out sequentially). In practice Abort/Transform time
is far smaller because they run first and abort fast on timeout; Observer
hooks continue but never block the write.

If the aggregate matters, keep `T` small (default 100 ms is chosen to be
well below any reasonable outer SET deadline), or for very heavy observers
use `SetHookTimeout(0)` and manage timeouts inside the hook’s own goroutine.

### Q: After a test runs, subsequent tests see the hooks from earlier tests

Call `client.RemoveHook(id)` in test cleanup, or if you’re writing tests
inside `redisx/client`, the test suite’s `SetupTest` already calls an
internal reset. Do not rely on process shutdown to clear hooks — hooks are
not reset by `client.Disconnect`, because they are a property of the
process-level hook registry, not of a specific connection.

### Q: Panic logs show `hook Observer#0 panic: ...` — how do I identify which hook #0 was?

Hooks are numbered in registration order per type, starting from `#0`. Match
that to your registration order at boot. If you manage many hooks
dynamically, wrap registration in a helper that stores `(id, label)` pairs
yourself and logs label + id together. The stable public `HookID` returned
by `Add*Hook` is your primary identifier for that purpose.

### Q: Why can’t I return an error from ObserverHook / ObserverAfterHook?

Because the framework enforces that Observers can never impact a write.
Allowing an error return would create a footgun: a one-off “I’ll return nil
always, promise” hook turns into a surprise abort when a future maintainer
changes the return value. If you need to block writes, use AbortHook.

### Q: Hook IDs wrap? How likely is that?

`HookID` is a process-local `uint64`, monotonic per `Add*Hook` call,
skipping 0. At one million registrations per second, ID wraparound happens
after ~580 thousand years — effectively impossible for realistic usage.
Tests that add/remove in a loop can safely ignore wraparound, and the
increment uses CAS so concurrent registrations from multiple goroutines all
receive distinct ids.

## API cheat sheet

```go
// ---- client package (raw key/value bytes) ----
type HookID uint64

type AbortHook          func(key string, value []byte) error
type TransformHook      func(key string, value []byte) (newValue []byte, err error)
type ObserverHook       func(key string, value []byte)
type ObserverAfterHook  func(key string, value []byte, committed bool, writeErr error)

func AddAbortHook(h AbortHook) HookID
func AddTransformHook(h TransformHook) HookID
func AddObserverHook(h ObserverHook) HookID
func AddObserverAfterHook(h ObserverAfterHook) HookID
func RemoveHook(id HookID)               // removing 0 or unknown id is safe
func SetHookTimeout(d time.Duration)     // d<=0 disables timeouts (sync exec); panic recover stays on

// ---- doc package (typed JSON documents — thin forwarders) ----
func AddAbortHook(h func(key string, valueJSON []byte) error) client.HookID
func AddTransformHook(h func(key string, valueJSON []byte) ([]byte, error)) client.HookID
func AddObserverHook(h func(key string, valueJSON []byte)) client.HookID
func AddObserverAfterHook(h func(key string, valueJSON []byte, committed bool, writeErr error)) client.HookID
func RemoveHook(id client.HookID)
func SetHookTimeout(d time.Duration)
```

That’s it. Register once; rely on the framework to dispatch correctly,
abort or continue with the right fail policy, and log + recover if a hook
misbehaves.
