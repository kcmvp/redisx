# Typed Document Helpers

`redisx` provides a small document contract (`x.Document`) plus typed helpers on both the client and the embedded server side.

- Remote mode (RESP): use `client/doc`
- Embedded mode (in-process DB): use `server.DBX` via `server.As[D]`

This keeps `server.DB` as a minimal key-value core, while letting higher-level code work with document-level keys instead of remembering storage prefixes.

## Define a document type

Documents are represented as JSON strings. A document type is typically a `string` alias that implements `x.Document`:

```go
type UserDoc string

func (UserDoc) Namespace() string  { return "user" }
func (UserDoc) Mem() bool          { return false }
func (UserDoc) KeyAttrs() []string { return []string{"id"} }
func (u UserDoc) RawJSON() string  { return string(u) }
func (UserDoc) TTL() time.Duration { return time.Hour }
```

Write-side key derivation is done by `x.StorageKey(d)`:

- reads `d.RawJSON()`
- extracts JSON values by each path in `d.KeyAttrs()`
- joins them with `":"`
- prepends `d.Namespace()`

For example, `{"id":"200"}` becomes `user:200`.

Typed write helpers use the document TTL declared by `D` automatically:

- `dbx.Set(d)` and `doc.Set(d)` write with `d.TTL()`
- `dbx.SetNX(d)` and `doc.SetNX(d)` also use `d.TTL()`
- `dbx.Update(...)` and `doc.Update(...)` preserve an existing key TTL

Read-side key derivation is done by `x.StorageKeyValue[D](key)`:

- `key` is the document-level key value, such as `"200"`
- the helper derives the storage prefix from `D`
- the final storage key becomes `user:200`

## Embedded mode: bind one document type to a DB

Use `server.As[D]` to obtain a typed view over an existing `*server.DB`:

```go
db := server.Start("127.0.0.1:6380", ":memory:")
dbx := server.As[UserDoc](db)

_ = dbx.Set(UserDoc(`{"id":"200","name":"Test","age":30}`))

doc, _ := dbx.Get("200")
_ = doc
```

### What `As[D]` does

`server.As[D]` performs a zero-allocation pointer conversion:

- it does not allocate a wrapper object
- it does not copy `DB`
- it only changes the static type of the pointer (`*DB` -> `*DBX[D]`)

`D` is constrained by:

```go
type x.Document interface {
    ~string
    Namespace() string
    Mem() bool
    KeyAttrs() []string
    RawJSON() string
    TTL() time.Duration
}
```

`~string` ensures `Get(...) (D, error)` can safely cast the loaded raw JSON string to `D`.

## Remote mode: typed helpers on the shared client connection

The remote equivalent lives in `client/doc` and matches the same idea:

```go
if err := client.Connect("127.0.0.1:6380", "demo-key"); err != nil {
    panic(err)
}
defer client.Disconnect()

_ = doc.Set(UserDoc(`{"id":"200","name":"Test","age":30}`))
got, _ := doc.Get[UserDoc]("200")
_ = got
```

## Pattern semantics

For document-scoped methods that accept a `keyPattern` (`Keys`, `SearchKey`, `Update`), the keyPattern is a _sub-pattern_ that is automatically prefixed:

- `dbx.Keys("*")` matches `user:*`
- `dbx.SearchKey("*", filter, false)` searches over `user:*`
- `dbx.Update("*", filter, x.Set(...))` updates `user:*`

This avoids hardcoding the `"user"` namespace at call sites.

## Practical notes

- `Get(key)` accepts the document-level key value, not the prefixed storage key. For `UserDoc`, pass `"200"` instead of `"user:200"`.
- `Delete(d)` still accepts a document and derives the storage key from `KeyAttrs()`. For example, `{"id":"200"}` is enough when `KeyAttrs() == []{"id"}`.
