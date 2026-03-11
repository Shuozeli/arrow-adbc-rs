# arrow-adbc-rs — Project Overview

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
├── adbc/               # Core library: traits, error types, Arrow schemas, SQL safety
├── adbc-sqlite/        # Driver: SQLite (via bundled rusqlite)
├── adbc-postgres/      # Driver: PostgreSQL (via postgres crate)
├── adbc-mysql/         # Driver: MySQL (via mysql crate)
├── adbc-flightsql/     # Driver: Apache Arrow FlightSQL (via tonic / arrow-flight)
└── docs/               # This documentation
    ├── overview.md     # <- you are here
    ├── architecture.md # Trait design and crate internals
    ├── development.md  # Building, testing, and contributing
    └── feature-matrix.md # Driver feature and test coverage
```

## Quick Start

### SQLite (in-memory)

```rust
use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
use adbc_sqlite::SqliteDriver;

let mut drv = SqliteDriver::default();
let db = drv.new_database_with_opts([
    (DatabaseOption::Uri, OptionValue::String(":memory:".into())),
]).unwrap();
let mut conn = db.new_connection().unwrap();
let mut stmt = conn.new_statement().unwrap();
stmt.set_sql_query("SELECT 42 AS answer").unwrap();
let (mut reader, _) = stmt.execute().unwrap();
while let Some(batch) = reader.next() {
    println!("{:?}", batch.unwrap());
}
```

### PostgreSQL

```rust
use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
use adbc_postgres::PostgresDriver;

let mut drv = PostgresDriver::default();
let db = drv.new_database_with_opts([
    (DatabaseOption::Uri, OptionValue::String(
        "host=localhost port=5432 user=myuser password=mypass dbname=mydb".into(),
    )),
]).unwrap();
let mut conn = db.new_connection().unwrap();
let mut stmt = conn.new_statement().unwrap();
stmt.set_sql_query("SELECT 42 AS answer").unwrap();
let (mut reader, _) = stmt.execute().unwrap();
while let Some(batch) = reader.next() {
    println!("{:?}", batch.unwrap());
}
```

### MySQL

```rust
use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
use adbc_mysql::MysqlDriver;

let mut drv = MysqlDriver::default();
let db = drv.new_database_with_opts([
    (DatabaseOption::Uri, OptionValue::String(
        "mysql://myuser:mypass@localhost:3306/mydb".into(),
    )),
]).unwrap();
let mut conn = db.new_connection().unwrap();
let mut stmt = conn.new_statement().unwrap();
stmt.set_sql_query("SELECT 42 AS answer").unwrap();
let (mut reader, _) = stmt.execute().unwrap();
while let Some(batch) = reader.next() {
    println!("{:?}", batch.unwrap());
}
```

### FlightSQL

```rust
use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
use adbc_flightsql::FlightSqlDriver;

let mut drv = FlightSqlDriver::default();
let db = drv.new_database_with_opts([
    (DatabaseOption::Uri,      OptionValue::String("grpc://localhost:32010".into())),
    (DatabaseOption::Username, OptionValue::String("admin".into())),
    (DatabaseOption::Password, OptionValue::String("password".into())),
]).unwrap();
let mut conn = db.new_connection().unwrap();
let mut stmt = conn.new_statement().unwrap();
stmt.set_sql_query("SELECT 1").unwrap();
let (mut reader, _) = stmt.execute().unwrap();
while let Some(batch) = reader.next() {
    println!("{:?}", batch.unwrap());
}
```

## Crate Summary

| Crate            | Description                                                                                                                                    |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `adbc`           | Core ADBC traits (`Driver`, `Database`, `Connection`, `Statement`), error types, Arrow schemas, and compile-time SQL safety (`TrustedSql`)     |
| `adbc-sqlite`    | SQLite driver (bundled via rusqlite); SQL queries, DML, bulk ingest, and all catalog metadata methods                                           |
| `adbc-postgres`  | PostgreSQL driver (via `postgres` crate); full transaction control, isolation levels, and catalog metadata                                      |
| `adbc-mysql`     | MySQL driver (via `mysql` crate); full transaction control, isolation levels, read-only mode, and catalog metadata                              |
| `adbc-flightsql` | FlightSQL driver (via tonic/arrow-flight); supports plaintext and TLS, basic-auth, and server-side transactions                                |

## Further Reading

- [Architecture](architecture.md) -- trait hierarchy, generics design, and per-crate internals
- [Development](development.md) -- building, running tests, and how to add a new driver
- [Feature Matrix](feature-matrix.md) -- driver feature and test coverage status
- [ADBC specification](https://arrow.apache.org/adbc/current/format/specification.html)
- [Arrow Rust crates](https://github.com/apache/arrow-rs)
