# arrow-adbc-rs -- Project Overview

> Last updated: 2026-03-19

A **clean-room, idiomatic Rust** implementation of the
[Arrow Database Connectivity (ADBC) v1.1.0](https://arrow.apache.org/adbc/) specification.

## What Is ADBC?

ADBC is an open standard for database drivers that return results as
[Apache Arrow](https://arrow.apache.org/) `RecordBatch`es instead of row-at-a-time ODBC/JDBC
result sets. This means query results are already in a columnar, zero-copy format that works
directly with Arrow-native analytics engines like DataFusion, Polars, and DuckDB.

```
Application
    │
    │ Arrow RecordBatches
    ▼
ADBC Driver  ◄──► Database (SQLite, FlightSQL, …)
    │
    │ Arrow RecordBatches
    ▼
Analytics engine (DataFusion, Polars, …)
```

## Why a Clean-Room Implementation?

The upstream [apache/arrow-adbc](https://github.com/apache/arrow-adbc) repository ships a C
library with dynamic loading and a thin Rust binding around it. This workspace instead:

- Uses **pure Rust** traits with no C FFI or dynamic library loading.
- Avoids any dependency on the upstream `adbc` crate.
- Implements the full ADBC v1.1.0 **trait hierarchy** generically, enabling monomorphisation
  with zero heap allocation in the driver chain.
- Provides a foundation for studying or extending ADBC without the complexity of C interop.

## Workspace Layout

```
arrow-adbc-rs/
├── crates/
│   ├── adbc/           # Core library: traits, error types, Arrow schemas, SQL safety
│   ├── adbc-sqlite/    # Driver: SQLite (via bundled rusqlite)
│   ├── adbc-postgres/  # Driver: PostgreSQL (via tokio-postgres)
│   ├── adbc-mysql/     # Driver: MySQL (via mysql_async)
│   └── adbc-flightsql/ # Driver: Apache Arrow FlightSQL (via tonic / arrow-flight)
├── examples/           # Per-crate examples (analytics, ETL pipeline, inventory)
└── docs/               # This documentation
    ├── overview.md          # <- you are here
    ├── architecture.md      # Trait design and crate internals
    ├── design-async.md      # Design record: sync-to-async migration (completed)
    ├── development.md       # Building, testing, and contributing
    ├── feature-matrix.md    # Driver feature and test coverage
    ├── publish-checklist.md # Pre-publish checklist for crates.io
    ├── audit-findings.md    # Code audit findings
    ├── postmortem-sqlite-bound-params.md
    ├── adbc-sqlite/         # Per-driver test catalogs
    ├── adbc-postgres/
    └── adbc-mysql/
```

## Quick Start

### SQLite (in-memory)

```rust
use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
use adbc_sqlite::SqliteDriver;

let drv = SqliteDriver::default();
let db = drv.new_database_with_opts([
    (DatabaseOption::Uri, OptionValue::String(":memory:".into())),
]).await.unwrap();
let conn = db.new_connection().await.unwrap();
let mut stmt = conn.new_statement().await.unwrap();
stmt.set_sql_query("SELECT 42 AS answer").await.unwrap();
let (mut reader, _) = stmt.execute().await.unwrap();
while let Some(batch) = reader.next() {
    println!("{:?}", batch.unwrap());
}
```

### PostgreSQL

```rust
use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
use adbc_postgres::PostgresDriver;

let drv = PostgresDriver::default();
let db = drv.new_database_with_opts([
    (DatabaseOption::Uri, OptionValue::String(
        "host=localhost port=5432 user=myuser password=mypass dbname=mydb".into(),
    )),
]).await.unwrap();
let conn = db.new_connection().await.unwrap();
let mut stmt = conn.new_statement().await.unwrap();
stmt.set_sql_query("SELECT 42 AS answer").await.unwrap();
let (mut reader, _) = stmt.execute().await.unwrap();
while let Some(batch) = reader.next() {
    println!("{:?}", batch.unwrap());
}
```

### MySQL

```rust
use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
use adbc_mysql::MysqlDriver;

let drv = MysqlDriver::default();
let db = drv.new_database_with_opts([
    (DatabaseOption::Uri, OptionValue::String(
        "mysql://myuser:mypass@localhost:3306/mydb".into(),
    )),
]).await.unwrap();
let conn = db.new_connection().await.unwrap();
let mut stmt = conn.new_statement().await.unwrap();
stmt.set_sql_query("SELECT 42 AS answer").await.unwrap();
let (mut reader, _) = stmt.execute().await.unwrap();
while let Some(batch) = reader.next() {
    println!("{:?}", batch.unwrap());
}
```

### FlightSQL

```rust
use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
use adbc_flightsql::FlightSqlDriver;

let drv = FlightSqlDriver::default();
let db = drv.new_database_with_opts([
    (DatabaseOption::Uri,      OptionValue::String("grpc://localhost:32010".into())),
    (DatabaseOption::Username, OptionValue::String("admin".into())),
    (DatabaseOption::Password, OptionValue::String("password".into())),
]).await.unwrap();
let conn = db.new_connection().await.unwrap();
let mut stmt = conn.new_statement().await.unwrap();
stmt.set_sql_query("SELECT 1").await.unwrap();
let (mut reader, _) = stmt.execute().await.unwrap();
while let Some(batch) = reader.next() {
    println!("{:?}", batch.unwrap());
}
```

## Crate Summary

| Crate            | Description                                                                                                                                    |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `adbc`           | Core ADBC traits (`Driver`, `Database`, `Connection`, `Statement`), error types, Arrow schemas, and compile-time SQL safety (`TrustedSql`)     |
| `adbc-sqlite`    | SQLite driver (bundled via rusqlite); SQL queries, DML, bulk ingest, and all catalog metadata methods                                           |
| `adbc-postgres`  | PostgreSQL driver (via `tokio-postgres`); full transaction control, isolation levels, and catalog metadata                                       |
| `adbc-mysql`     | MySQL driver (via `mysql_async`); full transaction control, isolation levels, read-only mode, and catalog metadata                               |
| `adbc-flightsql` | FlightSQL driver (via tonic/arrow-flight); supports plaintext and TLS, basic-auth, and server-side transactions                                |

## Further Reading

- [Architecture](architecture.md) -- trait hierarchy, generics design, and per-crate internals
- [Design: Async-First Traits](design-async.md) -- design record for the sync-to-async migration
- [Development](development.md) -- building, running tests, and how to add a new driver
- [Feature Matrix](feature-matrix.md) -- driver feature and test coverage status
- [Publish Checklist](publish-checklist.md) -- pre-publish checklist for crates.io
- [Audit Findings](audit-findings.md) -- code audit findings and status
- [ADBC specification](https://arrow.apache.org/adbc/current/format/specification.html)
- [Arrow Rust crates](https://github.com/apache/arrow-rs)
