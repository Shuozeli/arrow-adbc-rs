# Architecture

This document explains the internal design of the `arrow-adbc-rs` workspace: the trait
hierarchy, generics strategy, and per-crate implementation notes.

## Trait Hierarchy

The four core traits mirror the ADBC v1.1.0 object model exactly:

```
Driver
  └─ creates ──▶ Database
                   └─ creates ──▶ Connection
                                    └─ creates ──▶ Statement
```

Each trait is parameterised over the type it produces:

```rust
pub trait Driver {
    type DatabaseType: Database;
    fn new_database(&mut self) -> Result<Self::DatabaseType>;
    fn new_database_with_opts(...) -> Result<Self::DatabaseType>;
}

pub trait Database {
    type ConnectionType: Connection;
    fn new_connection(&self) -> Result<Self::ConnectionType>;
    fn new_connection_with_opts(...) -> Result<Self::ConnectionType>;
}

pub trait Connection {
    type StatementType: Statement;
    // SQL execution, transaction management, catalog metadata …
}

pub trait Statement {
    // set_sql_query, prepare, execute, execute_update, bind …
}
```

Because every associated type is concrete, the compiler can **monomorphise the entire
chain** — no `Box<dyn …>`, no vtable, no heap allocation in the object creation path.
Result sets themselves are returned as `Box<dyn RecordBatchReader + Send>` because their
lifetime must outlive the statement.

## `adbc` — Core Crate

### `error.rs`

Defines `Status` (14 variants matching the ADBC spec) and `Error` (message + status +
optional vendor code + optional 5-char SQLSTATE). The `Result<T>` alias is
`std::result::Result<T, adbc::Error>`.

Short-hand constructors (`Error::not_impl`, `Error::io`, `Error::not_found`, …) keep
driver code terse.

### `driver.rs`

All enums and traits described in the hierarchy above, plus:

| Type               | Purpose                                                  |
| ------------------ | -------------------------------------------------------- |
| `OptionValue`      | Polymorphic option value (String / Int / Double / Bytes) |
| `DatabaseOption`   | URI, username, password, or `Other(String)`              |
| `ConnectionOption` | AutoCommit, ReadOnly, IsolationLevel, or `Other`         |
| `StatementOption`  | TargetTable, IngestMode, or `Other`                      |
| `IsolationLevel`   | Default → Linearizable (6 levels)                        |
| `IngestMode`       | Create / Append / Replace / CreateAppend                 |
| `ObjectDepth`      | Filter depth for `get_objects`                           |
| `InfoCode`         | Numeric info codes for `get_info` (matches ADBC spec)    |

### `schema.rs`

Static `LazyLock<SchemaRef>` / `LazyLock<DataType>` constants for every metadata result
set defined by the spec:

| Constant                                                                                 | Used by                                    |
| ---------------------------------------------------------------------------------------- | ------------------------------------------ |
| `TABLE_TYPES_SCHEMA`                                                                     | `Connection::get_table_types`              |
| `GET_INFO_SCHEMA`                                                                        | `Connection::get_info`                     |
| `GET_OBJECTS_SCHEMA`                                                                     | `Connection::get_objects`                  |
| `USAGE_SCHEMA`, `CONSTRAINT_SCHEMA`, `COLUMN_SCHEMA`, `TABLE_SCHEMA`, `DB_SCHEMA_SCHEMA` | nested structs inside `GET_OBJECTS_SCHEMA` |

`GET_INFO_SCHEMA` uses a **dense union** Arrow type for the `info_value` column (string /
bool / int64 / int32_bitmask / string_list / int32→int32_list map), exactly as the spec
requires.

## `adbc::sql` -- Compile-Time SQL Safety

The `sql` module provides a system for constructing SQL strings that are safe from
injection by design. The key types are:

| Type              | Purpose                                                        |
| ----------------- | -------------------------------------------------------------- |
| `TrustedSql`      | An opaque SQL string that can only be built from safe parts    |
| `QuotedIdent`     | A properly quoted identifier (ANSI `"…"` or MySQL `` `…` ``)  |
| `SqlLiteral`      | A compile-time SQL keyword or fragment (`&'static str`)        |
| `SqlJoined`       | A comma-separated list of `SqlSafe` items                      |
| `SqlColumnDef`    | A `QuotedIdent` + type literal for DDL column definitions      |
| `SqlPlaceholders` | Generates `$1, $2, …` (Postgres) or `?, ?, …` (others)        |

All types implement the sealed `SqlSafe` trait, which prevents arbitrary `String`
or `&str` from being interpolated into SQL. The `trusted_sql!` macro enforces this
at compile time:

```rust
// Compiles:
let sql = trusted_sql!("SELECT * FROM {}", QuotedIdent::ansi("users"));

// Does NOT compile -- String is not SqlSafe:
let table = String::from("users");
let sql = trusted_sql!("SELECT * FROM {}", table); // compile error
```

## `adbc-sqlite` Driver

### Connection ownership model

`SqliteConnection` wraps `rusqlite::Connection` in `Arc<Mutex<rusqlite::Connection>>`.
This is necessary because `Connection` metadata methods (`get_info`, `get_objects`, …)
take `&self`, but locking a `Mutex` requires a shared reference. `SqliteStatement` clones
the `Arc` so statements and connections can coexist without lifetime issues.

### Transaction management

| `autocommit`     | Behaviour                                                                                                                                                       |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `true` (default) | Each `execute_update` runs in SQLite's implicit transaction                                                                                                     |
| `false`          | `BEGIN DEFERRED` is issued when autocommit is disabled; `COMMIT` / `ROLLBACK` re-open a new `BEGIN DEFERRED` immediately so the connection stays in manual mode |

### Bulk ingest (`execute_update` with `IngestMode`)

1. Bind one or more `RecordBatch`es via `bind` / `bind_stream`.
2. Set the target table name via `StatementOption::TargetTable`.
3. Call `execute_update`.

Internally, `convert::ingest_batches` creates the table (or not, depending on
`IngestMode`) and inserts rows using `rusqlite` prepared statements. Arrow columns are
converted to SQLite values in `convert.rs`.

### `convert.rs`

Implements `SqliteReader`, a `RecordBatchReader` that:

1. Prepares and steps through a `rusqlite::Statement`.
2. Introspects column types from `rusqlite`'s `column_type()`.
3. Builds Arrow arrays column-by-column and emits `RecordBatch`es.

Type mapping (SQLite → Arrow):

| SQLite affinity | Arrow type |
| --------------- | ---------- |
| INTEGER         | Int64      |
| REAL            | Float64    |
| TEXT            | Utf8       |
| BLOB            | Binary     |
| NULL            | Null       |

### `catalog.rs`

Implements all four catalog metadata methods by querying SQLite's internal
`sqlite_master` / `pragma_table_info` / `pragma_foreign_key_list` tables and building
Arrow `RecordBatch`es matching the schemas from `adbc::schema`.

## `adbc-postgres` Driver

### Connection ownership model

Same `Arc<Mutex<…>>` pattern as SQLite. `PostgresConnection` wraps `postgres::Client`.

### Transaction management

| `autocommit`     | Behaviour                                                                 |
| ---------------- | ------------------------------------------------------------------------- |
| `true` (default) | Each statement runs in PostgreSQL's implicit transaction                   |
| `false`          | `BEGIN` is issued; `COMMIT`/`ROLLBACK` re-open a new `BEGIN` immediately |

Supports all standard isolation levels (Read Uncommitted through Serializable) via
`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL …`.

### `convert.rs`

Maps PostgreSQL types to Arrow types using `postgres::Column` metadata. Supports
signed/unsigned integers, floats, booleans, text, and binary. Bulk ingest uses
parameterized `INSERT` statements with `$1, $2, …` placeholders via `SqlPlaceholders::dollar()`.

## `adbc-mysql` Driver

### Connection ownership model

Same `Arc<Mutex<…>>` pattern. `MysqlConnection` wraps `mysql::PooledConn`.

### Transaction management

| `autocommit`     | Behaviour                                                                            |
| ---------------- | ------------------------------------------------------------------------------------ |
| `true` (default) | Each statement auto-commits                                                          |
| `false`          | `SET autocommit=0` + `BEGIN`; `COMMIT`/`ROLLBACK` re-open a new `BEGIN` immediately |

Supports isolation levels via `SET TRANSACTION ISOLATION LEVEL …` and read-only mode
via `SET TRANSACTION READ ONLY / READ WRITE`.

### `convert.rs`

Maps MySQL column types (including unsigned flag detection) to Arrow types. Uses
`try_from()` checked integer casts to prevent overflow. Bulk ingest uses anonymous
`?` placeholders.

**Note:** `Statement::prepare` is currently a no-op -- see `docs/feature-matrix.md`
for known gaps.

## `adbc-flightsql` Driver

### Async → sync bridge

ADBC traits are synchronous. The FlightSQL driver bridges internally:

```rust
fn block<F: Future<Output = T>, T>(f: F) -> T {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| Handle::current().block_on(f))
    } else {
        Builder::new_current_thread().enable_all().build()?.block_on(f)
    }
}
```

This handles two cases:

- **Inside a Tokio runtime** (e.g. an async `#[tokio::test]`): uses `block_in_place` to
  avoid deadlocking the runtime.
- **Outside a Tokio runtime**: builds a fresh single-threaded runtime per call.

### Connection and transactions

`FlightSqlConnection` holds:

- A `FlightSqlServiceClient<Channel>` (cheaply cloneable — each statement gets its own clone).
- `autocommit: bool`.
- `transaction_id: Option<bytes::Bytes>` — the opaque handle returned by
  `BeginTransaction`; present only when a server-side transaction is open.

Commit / rollback call `EndTransaction` and immediately open a new transaction to keep the
connection in manual-commit mode.

### `catalog.rs`

Wraps all FlightSQL metadata RPCs:

| ADBC method        | FlightSQL RPC                                       |
| ------------------ | --------------------------------------------------- |
| `get_table_schema` | `GetSchema` (via `CommandGetTableTypes` descriptor) |
| `get_table_types`  | `GetFlightInfo(CommandGetTableTypes)` → `DoGet`     |
| `get_info`         | `GetFlightInfo(CommandGetSqlInfo)` → `DoGet`        |
| `get_objects`      | `GetFlightInfo(CommandGetObjects)` → `DoGet`        |
| `execute`          | `GetFlightInfo(CommandStatementQuery)` → `DoGet`    |
| `execute_update`   | `DoPut(CommandStatementUpdate)`                     |
| `prepare`          | `CreatePreparedStatement`                           |

Results are collected into `Vec<RecordBatch>` and returned via the `VecReader` helper.
