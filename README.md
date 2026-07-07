<p align="center">
  Redis compatible server implemented with <a href="https://github.com/tidwall/buntdb">buntdb</a>
  <br/>
  <br/>
  <a href="https://github.com/kcmvp/respx/blob/main/LICENSE">
    <img alt="GitHub" src="https://img.shields.io/github/license/kcmvp/respx"/>
  </a>
  <a href="https://pkg.go.dev/github.com/kcmvp/respx">
    <img src="https://pkg.go.dev/badge/github.com/kcmvp/respx.svg" alt="Go Reference"/>
  </a>
  <a href="https://goreportcard.com/report/github.com/kcmvp/respx">
    <img src="https://goreportcard.com/badge/github.com/kcmvp/respx" alt="report"/>
  </a>
  <a href="https://github.com/kcmvp/respx/blob/main/.github/workflows/ci.yml" rel="nofollow">
     <img src="https://img.shields.io/github/actions/workflow/status/kcmvp/respx/ci.yml?branch=main" alt="Build" />
  </a>
  <a href="https://app.codecov.io/gh/kcmvp/respx" ref="nofollow">
    <img src ="https://img.shields.io/codecov/c/github/kcmvp/respx" alt="coverage"/>
  </a>

</p>

## Features

**respx** is an embedded, high-performance Document DB with a Redis-compatible API. It seamlessly blends standard Redis Key-Value operations with advanced MongoDB-style JSON querying capabilities.

### 1. Native Redis Commands

`respx` natively supports a subset of standard Redis commands, allowing you to drop it into existing ecosystems with minimal friction:

- **Connection Management:** `AUTH`, `HELLO`, `PING`, `QUIT`, `CLIENT`
- **Key-Value Operations:** `SET`, `SETEX`, `SETNX`, `GET`, `DEL`, `KEYS`
- **Pub/Sub:** `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`

### 2. Extend (X) Commands

The true power of `respx` lies in its extended document querying capabilities. By treating stored strings as JSON documents and defining schemas with indexes, you can perform complex queries using a declarative DSL.

#### `BYINDEX`

Performs a MongoDB-style query on a specific JSON index.

**Syntax:**

```text
BYINDEX <schema_name> <index_attribute> <json_filter> [ASC|DESC]
```

- **`schema_name`**: The logical namespace of the data (e.g., `user`).
- **`index_attribute`**: The JSON path used as the driving index for the query (e.g., `email`, `age`).
- **`json_filter`**: A MongoDB-style JSON string defining the composite filtering conditions.
- **`[ASC|DESC]`**: Optional order direction. Default is `ASC`.

#### `BYKEY`

Performs a MongoDB-style query over keys matching a glob pattern within a schema.

**Syntax:**

```text
BYKEY <schema_name> <pattern> <json_filter> [ASC|DESC]
```

- **`schema_name`**: The logical namespace of the data (e.g., `user`).
- **`pattern`**: A BuntDB glob pattern to match the keys (e.g., `*`, `123*`).
- **`json_filter`**: A MongoDB-style JSON string defining the composite filtering conditions.
- **`[ASC|DESC]`**: Optional order direction. Default is `ASC`.

#### Examples & Client Usage

`respx` provides a powerful, fluent Go client that automatically translates your Go code into the underlying JSON query expressions.

##### 1. Simple Equality Match

Find a user whose email is exactly `ken@example.com`.

**Raw Redis Command:**

```text
BYINDEX user email {"email": {"$eq": "ken@example.com"}}
// or simply
BYINDEX user email {"email": "ken@example.com"}
```

**Go Client (`client.ByIndex`):**

```go
import "github.com/kcmvp/respx/x"

res := client.ByIndex("user", "email", x.Eq("email", "ken@example.com"), false)
users := res.MustGet()
```

##### 2. Range Query (Greater Than)

Find users whose age is greater than 18.

**Raw Redis Command:**

```text
BYINDEX user age {"age": {"$gt": 18}}
```

**Go Client:**

```go
res := client.ByIndex("user", "age", x.Gt("age", 18), false)
```

##### 3. Composite Query (Logical AND)

Find users whose age is greater than 18 AND status is "active".

**Raw Redis Command:**

```text
BYINDEX user age {"$and": [{"age": {"$gt": 18}}, {"status": "active"}]}
```

**Go Client:**

```go
res := client.ByIndex("user", "age", x.And(
    x.Gt("age", 18),
    x.Eq("status", "active"),
), false)
```

##### 4. Complex Nested Query (AND + OR)

Find users who are either (age < 20) OR (age > 18 AND status is "active").

**Raw Redis Command:**

```text
BYINDEX user age {"$or": [
    {"age": {"$lt": 20}},
    {"$and": [{"age": {"$gt": 18}}, {"status": "active"}]}
]}
```

**Go Client:**

```go
res := client.ByIndex("user", "age", x.Or(
    x.Lt("age", 20),
    x.And(
        x.Gt("age", 18),
        x.Eq("status", "active"),
    ),
), false)
```

## Installation

```bash
go get github.com/kcmvp/respx
```
