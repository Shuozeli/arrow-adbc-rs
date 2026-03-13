# Design: Async-First ADBC Traits

## Problem

The current ADBC traits (`Driver`, `Database`, `Connection`, `Statement`) are
synchronous. FlightSQL bridges async gRPC calls to the sync interface via a
`block()` helper that creates or enters a Tokio runtime. This works but has
drawbacks:

1. **Callers in async contexts must spawn_blocking**: Any Quiver/ADBC consumer
   running in a tokio runtime must wrap every call in `spawn_blocking`, adding
   overhead and boilerplate.
2. **FlightSQL fights the API**: The FlightSQL driver is natively async (tonic
   gRPC) but must `block()` every call to satisfy the sync trait, then the
   caller must `spawn_blocking` to avoid blocking the executor. Double bridge.
3. **PostgreSQL and MySQL have mature async crates**: `tokio-postgres` and
   `mysql_async` are production-grade, but cannot be used because the traits
   are sync-only. The sync `postgres` and `mysql` crates require `&mut self`
   which forces `RefCell`/`Mutex` workarounds.
4. **Consumers want async**: Quiver ORM needs async as a first-class citizen
   for connection pooling, streaming results, and concurrent queries.

## Proposal: Replace Sync Traits with Async Traits

This is a **breaking change**. The sync traits are replaced with async
equivalents. Sync callers can use `block_on()` to call async methods.

### Why Replace Instead of Add Alongside?

- Adding parallel `AsyncDriver`/`AsyncConnection` traits doubles the API
  surface and forces every driver to implement both.
- The async version is strictly more general: sync callers can `block_on()`
  an async function, but async callers cannot safely call a sync function
  without `spawn_blocking`.
- FlightSQL is already async internally -- the sync trait is overhead.
- New drivers (DuckDB, ClickHouse, etc.) will be async-first.

### Why This is Safe as a Breaking Change

- The only consumers are Quiver ORM and internal tooling (both owned by us).
- The workspace is pre-1.0 (v0.1.0).
- The trait surface is small (4 traits, ~20 methods total).

## New Trait Signatures

All traits gain `Send + Sync` bounds and `async fn` methods. Rust 1.85+
supports native async fn in traits (RPITIT) -- no `async-trait` crate needed.

### Driver

```rust
pub trait Driver: Send + Sync {
    type DatabaseType: Database;

    async fn new_database(&self) -> Result<Self::DatabaseType>;

    async fn new_database_with_opts(
        &self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)> + Send,
    ) -> Result<Self::DatabaseType>;
}
```

**Changes:**
- `&mut self` -> `&self` (drivers are stateless, `&mut` was unnecessary)
- Added `Send + Sync` bound
- Methods are `async fn`

### Database

```rust
pub trait Database: Send + Sync {
    type ConnectionType: Connection;

    async fn new_connection(&self) -> Result<Self::ConnectionType>;

    async fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption> + Send,
    ) -> Result<Self::ConnectionType>;
}
```

**Changes:**
- Added `Send + Sync` bound
- Methods are `async fn`

### Connection

```rust
pub trait Connection: Send + Sync {
    type StatementType: Statement;

    async fn new_statement(&self) -> Result<Self::StatementType>;
    async fn set_option(&self, opt: ConnectionOption) -> Result<()>;

    // Transaction management
    async fn commit(&self) -> Result<()>;
    async fn rollback(&self) -> Result<()>;

    // Metadata
    async fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        name: &str,
    ) -> Result<Schema>;

    async fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>>;

    async fn get_info(
        &self,
        codes: Option<&[InfoCode]>,
    ) -> Result<Box<dyn RecordBatchReader + Send>>;

    async fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send>>;
}
```

**Changes:**
- `&mut self` -> `&self` for all methods (async drivers use internal
  synchronization instead of exclusive borrows)
- Added `Send + Sync` bound
- Methods are `async fn`
- `new_statement` returns owned `StatementType` (no lifetime bound to `&mut self`)

### Statement

```rust
pub trait Statement: Send + Sync {
    async fn set_sql_query(&mut self, sql: &str) -> Result<()>;
    async fn prepare(&mut self) -> Result<()>;
    async fn execute(
        &mut self,
    ) -> Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)>;
    async fn execute_update(&mut self) -> Result<i64>;
    async fn bind(&mut self, batch: RecordBatch) -> Result<()>;
    async fn bind_stream(
        &mut self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()>;
    async fn set_option(&mut self, opt: StatementOption) -> Result<()>;
}
```

**Changes:**
- Added `Send + Sync` bound
- Methods are `async fn`
- `&mut self` kept for Statement (statements are single-owner, mutable state
  is fine)

## &self vs &mut self Decision

| Trait | Old | New | Rationale |
|-------|-----|-----|-----------|
| Driver | `&mut self` | `&self` | Drivers are stateless factories. `&mut` was unnecessary. |
| Database | `&self` | `&self` | No change. Databases hold shared config. |
| Connection | `&mut self` (most) | `&self` | Async connections use internal `Mutex`/`RwLock`. Exclusive borrow prevents concurrent async operations. |
| Connection metadata | `&self` | `&self` | No change. Already `&self`. |
| Statement | `&mut self` | `&mut self` | Statements are single-owner. Mutation is natural. |

The key change is `Connection` methods going from `&mut self` to `&self`.
This allows:
- Multiple concurrent queries on the same connection (if the driver supports it)
- Sharing the connection across tasks via `Arc<C>`
- Internal synchronization via `tokio::sync::Mutex` or `RwLock`

Drivers that don't support concurrent operations (SQLite) use internal
`tokio::sync::Mutex` to serialize access.

## Per-Driver Migration

### adbc-sqlite

Current: `rusqlite::Connection` wrapped in `Arc<Mutex<...>>`

Async approach: `rusqlite` is sync-only and `!Send`. Use
`tokio::task::spawn_blocking` for all operations:

```rust
pub struct SqliteConnection {
    conn: Arc<std::sync::Mutex<rusqlite::Connection>>,
    // ... flags
}

impl Connection for SqliteConnection {
    async fn commit(&self) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| Error::internal(e.to_string()))?;
            conn.execute_batch("COMMIT").map_err(sqlite_err)?;
            conn.execute_batch("BEGIN").map_err(sqlite_err)
        }).await.map_err(|e| Error::internal(e.to_string()))?
    }
}
```

`spawn_blocking` moves the closure to a blocking thread pool, keeping the
async executor free. `std::sync::Mutex` is used (not `tokio::sync::Mutex`)
because the lock is held only inside the blocking closure.

### adbc-postgres

Current: `postgres::Client` (sync) wrapped in `Arc<Mutex<...>>`

**Migration: Replace `postgres` with `tokio-postgres`.**

`tokio-postgres` is the async counterpart to `postgres` from the same
`rust-postgres` project. The type systems (`ToSql`, `FromSql`, `Type`) are
shared between the two crates. The API is nearly identical but async:

```rust
pub struct PostgresConnection {
    client: tokio_postgres::Client,
    // ... flags
}

impl Connection for PostgresConnection {
    async fn commit(&self) -> Result<()> {
        self.client.batch_execute("COMMIT; BEGIN").await.map_err(pg_err)
    }
}
```

**Key benefit**: No `Mutex` needed. `tokio_postgres::Client` methods take
`&self` and handle internal concurrency. The `Client` is `Send + Sync`.

**Connection setup**: `tokio_postgres::connect()` returns
`(Client, Connection)`. The `Connection` future must be spawned as a
background task:

```rust
async fn new_connection(&self) -> Result<PostgresConnection> {
    let (client, connection) = tokio_postgres::connect(&self.uri, tls)
        .await
        .map_err(pg_err)?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    Ok(PostgresConnection { client, ... })
}
```

### adbc-mysql

Current: `mysql::PooledConn` (sync) wrapped in `Arc<Mutex<...>>`

**Migration: Replace `mysql` with `mysql_async`.**

`mysql_async` is the established async MySQL driver for Rust. It provides
`Conn`, `Pool`, and `Transaction` types. The API is similar to the sync
`mysql` crate:

```rust
pub struct MysqlConnection {
    conn: tokio::sync::Mutex<mysql_async::Conn>,
    // ... flags
}

impl Connection for MysqlConnection {
    async fn commit(&self) -> Result<()> {
        let mut conn = self.conn.lock().await;
        conn.query_drop("COMMIT").await.map_err(mysql_err)?;
        conn.query_drop("BEGIN").await.map_err(mysql_err)
    }
}
```

`mysql_async::Conn` requires `&mut self` for queries, so we use
`tokio::sync::Mutex` for interior mutability.

### adbc-flightsql

Current: Async internally, bridges to sync via `block()`.

**Migration: Remove the `block()` helper.** Call async methods directly:

```rust
// Before (sync trait, async bridge):
fn commit(&mut self) -> Result<()> {
    block(self.client.end_transaction(...))?
}

// After (async trait, direct):
async fn commit(&self) -> Result<()> {
    self.client.end_transaction(...).await.map_err(flight_err)
}
```

This is the simplest migration -- just remove `block()` wrappers and add
`async`/`.await`.

## New Dependencies

| Crate | Add | Remove | Notes |
|-------|-----|--------|-------|
| adbc (core) | -- | -- | No runtime dependency |
| adbc-sqlite | `tokio` (rt, sync) | -- | For `spawn_blocking` |
| adbc-postgres | `tokio-postgres` | `postgres` | Async PostgreSQL |
| adbc-mysql | `mysql_async` | `mysql` | Async MySQL |
| adbc-flightsql | -- | -- | Already has tokio |

Workspace Cargo.toml:
```toml
# Replace
postgres = "0.19"          # REMOVE
mysql = "25"               # REMOVE

# With
tokio-postgres = "0.7"     # ADD
mysql_async = "0.34"       # ADD

# Keep
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync"] }
rusqlite = { version = "0.35", features = ["bundled"] }
```

## Sync Compatibility Helper

For callers that need synchronous access (tests, CLI tools, scripts), provide
a `block_on()` helper in the `adbc` core crate:

```rust
// adbc/src/sync.rs

/// Run an async ADBC operation synchronously.
///
/// If already inside a Tokio runtime, uses `block_in_place`.
/// Otherwise, creates a temporary single-threaded runtime.
pub fn block_on<F: std::future::Future<Output = T>, T>(f: F) -> T {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(f))
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime")
            .block_on(f)
    }
}
```

This is the same pattern FlightSQL already uses, promoted to a public utility.

## dyn Dispatch

Native `async fn` in traits is not dyn-safe. For consumers that need
`&dyn Connection` (e.g., generic middleware, test harnesses), provide a
`DynConnection` trait with boxed futures:

```rust
// adbc/src/dyn_api.rs

pub trait DynConnection: Send + Sync {
    fn commit(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn rollback(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    // ... other methods
}

// Blanket impl: every Connection is also a DynConnection
impl<T: Connection> DynConnection for T {
    fn commit(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(Connection::commit(self))
    }
    // ...
}
```

## Migration Checklist

- [ ] Update `adbc/src/driver.rs`: async traits + Send + Sync bounds
- [ ] Add `adbc/src/sync.rs`: `block_on()` helper
- [ ] Add `adbc/src/dyn_api.rs`: `DynConnection` for trait objects
- [ ] Update `adbc-sqlite`: `spawn_blocking` wrapper, add `tokio` dep
- [ ] Update `adbc-postgres`: replace `postgres` with `tokio-postgres`
- [ ] Update `adbc-mysql`: replace `mysql` with `mysql_async`
- [ ] Update `adbc-flightsql`: remove `block()`, use async directly
- [ ] Update all tests to use `#[tokio::test]`
- [ ] Update `Cargo.toml` workspace deps
- [ ] Update docs (architecture.md, driver docs)
- [ ] Bump version to 0.2.0 (breaking change)

## Impact on Quiver ORM

After this change, Quiver's driver layer can build async traits directly
on top of the async ADBC traits instead of wrapping sync calls in
`spawn_blocking`. The Quiver `AsyncConnection` trait becomes a thin
delegation to `adbc::Connection`:

```rust
// In quiver-driver-core, the async Quiver trait delegates to ADBC
impl quiver::AsyncConnection for QuiverSqliteConnection {
    async fn query(&self, stmt: &Statement) -> Result<Vec<Row>> {
        let mut adbc_stmt = self.adbc_conn.new_statement().await?;
        adbc_stmt.set_sql_query(&stmt.sql).await?;
        // bind params...
        let (reader, _) = adbc_stmt.execute().await?;
        // convert RecordBatchReader -> Vec<Row>
        record_batch_reader_to_rows(reader)
    }
}
```

No `spawn_blocking` needed at the Quiver layer -- ADBC handles it internally.
