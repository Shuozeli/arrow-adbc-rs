# Development Guide

## Prerequisites

- **Rust** >= 1.81 (the MSRV set in `Cargo.toml`)
- `cargo` (comes with rustup)
- No system libraries required -- SQLite is bundled via `rusqlite`'s `bundled` feature
- **Docker** (optional, for PostgreSQL and MySQL integration tests)

## Building

```bash
# Check all crates compile cleanly
cargo check --workspace

# Build (debug)
cargo build --workspace

# Build (release)
cargo build --workspace --release
```

## Running Tests

### Unit and offline tests

SQLite tests run fully in-process with no external services:

```bash
cargo test -p adbc-sqlite
```

### Integration tests (PostgreSQL and MySQL)

PostgreSQL and MySQL integration tests are marked `#[ignore]` and require live database
servers. You can use Docker to spin them up locally, or point the environment variables at
any compatible server you already have running.

#### Option A: Docker (recommended)

Start PostgreSQL and MySQL containers:

```bash
# PostgreSQL
docker run -d --name adbc-postgres \
  -e POSTGRES_USER=adbc_test \
  -e POSTGRES_PASSWORD=adbc_test \
  -e POSTGRES_DB=adbc_test \
  -p 5432:5432 \
  postgres:16

# MySQL
docker run -d --name adbc-mysql \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=adbc_test \
  -e MYSQL_USER=adbc_test \
  -e MYSQL_PASSWORD=adbc_test \
  -p 3306:3306 \
  mysql:8
```

Then run the tests:

```bash
# PostgreSQL
ADBC_POSTGRES_URI="host=localhost port=5432 user=adbc_test password=adbc_test dbname=adbc_test" \
  cargo test -p adbc-postgres -- --include-ignored

# MySQL
ADBC_MYSQL_URI="mysql://adbc_test:adbc_test@localhost:3306/adbc_test" \
  cargo test -p adbc-mysql -- --include-ignored
```

#### Option B: Existing database server

Set the environment variables to point at your server:

```bash
ADBC_POSTGRES_URI="host=<host> port=5432 user=<user> password=<pass> dbname=<db>" \
  cargo test -p adbc-postgres -- --include-ignored

ADBC_MYSQL_URI="mysql://<user>:<pass>@<host>:3306/<db>" \
  cargo test -p adbc-mysql -- --include-ignored
```

#### Cleanup

```bash
docker stop adbc-postgres adbc-mysql
docker rm adbc-postgres adbc-mysql
```

### FlightSQL integration tests

FlightSQL tests require a running FlightSQL server. The tests are marked `#[ignore]` so
they are skipped by default:

```bash
# Skip FlightSQL tests (default)
cargo test -p adbc-flightsql

# Run FlightSQL tests against a local server on the default port (32010)
FLIGHTSQL_URI=grpc://localhost:32010 cargo test -p adbc-flightsql -- --ignored
```

### All tests

```bash
# Offline only
cargo test --workspace

# Full suite (requires running databases)
ADBC_POSTGRES_URI="host=localhost port=5432 user=adbc_test password=adbc_test dbname=adbc_test" \
ADBC_MYSQL_URI="mysql://adbc_test:adbc_test@localhost:3306/adbc_test" \
  cargo test --workspace -- --include-ignored
```

## Generating API Docs

```bash
cargo doc --workspace --no-deps --open
```

This opens the generated rustdoc in your default browser. All public items should have
doc comments; `--no-deps` keeps the output focused on this workspace.

## Project Layout Reference

```
arrow-adbc-rs/
├── Cargo.toml              # Workspace manifest; shared dependency versions
├── Cargo.lock
│
├── adbc/                   # Core crate (no std I/O, just traits + types)
│   └── src/
│       ├── lib.rs          # Re-exports everything public
│       ├── driver.rs       # Driver / Database / Connection / Statement traits + option enums
│       ├── error.rs        # Error, Status, Result<T>
│       ├── schema.rs       # Static Arrow schemas for all metadata result sets
│       └── sql.rs          # Compile-time SQL safety (TrustedSql, QuotedIdent, etc.)
│
├── adbc-sqlite/            # SQLite driver
│   └── src/
│       ├── lib.rs          # SqliteDriver / SqliteDatabase / SqliteConnection / SqliteStatement
│       ├── convert.rs      # Arrow <-> SQLite type mapping (SqliteReader + ingest_batches)
│       └── catalog.rs      # get_info / get_objects / get_table_types / get_table_schema
│
├── adbc-flightsql/         # FlightSQL driver
│   └── src/
│       ├── lib.rs          # FlightSqlDriver / ... / FlightSqlStatement + async bridge
│       └── catalog.rs      # All FlightSQL RPC wrappers
│
├── adbc-postgres/          # PostgreSQL driver
│   └── src/
│       ├── lib.rs          # PostgresDriver / PostgresDatabase / PostgresConnection / PostgresStatement
│       ├── convert.rs      # Arrow <-> PostgreSQL type mapping
│       └── catalog.rs      # Catalog metadata methods
│
├── adbc-mysql/             # MySQL driver
│   └── src/
│       ├── lib.rs          # MysqlDriver / MysqlDatabase / MysqlConnection / MysqlStatement
│       ├── convert.rs      # Arrow <-> MySQL type mapping
│       └── catalog.rs      # Catalog metadata methods
│
└── docs/                   # Documentation
    ├── overview.md
    ├── architecture.md
    └── development.md      # <- you are here
```

## Adding a New Driver

1. Create a new crate: `cargo new --lib adbc-<name>` inside the workspace root and add it
   to `[workspace].members` in the root `Cargo.toml`.

2. Add `adbc.workspace = true` to the new crate's `[dependencies]`.

3. Implement the four traits from `adbc`:

   ```rust
   use adbc::{Driver, Database, Connection, Statement};

   pub struct MyDriver;
   pub struct MyDatabase { /* connection config */ }
   pub struct MyConnection { /* live connection */ }
   pub struct MyStatement { /* query + bound params */ }

   impl Driver for MyDriver { type DatabaseType = MyDatabase; ... }
   impl Database for MyDatabase { type ConnectionType = MyConnection; ... }
   impl Connection for MyConnection { type StatementType = MyStatement; ... }
   impl Statement for MyStatement { ... }
   ```

4. For catalog metadata, build Arrow `RecordBatch`es that match the schemas in
   `adbc::schema` (e.g. `adbc::schema::GET_INFO_SCHEMA`).

5. Add integration tests under `adbc-<name>/tests/integration.rs` following the pattern
   in `adbc-sqlite/tests/integration.rs`.

## Code Style

- Follow standard `rustfmt` formatting (`cargo fmt --all`).
- Use `cargo clippy --workspace` and fix all warnings before submitting.
- Every public item should have a `///` doc comment.
- Prefer `Error::not_impl(...)` / `Error::invalid_arg(...)` short-hands over
  constructing `Error` manually.
