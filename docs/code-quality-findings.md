# Code Quality Findings

## 1. Potential Bugs (High Severity)

### 1.1 Postgres `ingest_batches` unconditionally wraps in BEGIN/COMMIT -- breaks manual transactions
- **Location:** `crates/adbc-postgres/src/convert.rs` (`ingest_batches`)
- **Problem:** `ingest_batches` always executes `BEGIN` and `COMMIT`, even if the connection is already inside a manual transaction (autocommit=false). PostgreSQL treats `BEGIN` inside an existing transaction as a no-op (with a warning), but the `COMMIT` will commit the outer transaction prematurely.
- **Fix:** Pass a boolean flag `needs_txn` to `ingest_batches` (or query the connection's autocommit state). Only issue `BEGIN`/`COMMIT` when the flag is true.
- **Status: DONE.** Added `autocommit: bool` parameter to `ingest_batches`. Shared `PgState` via `Arc<Mutex<PgState>>` so the statement can read autocommit state. Transaction wrapping is now conditional, matching the SQLite driver pattern.

### 1.2 MySQL `ingest_batches` has the same nested transaction bug
- **Location:** `crates/adbc-mysql/src/convert.rs` (`ingest_batches`)
- **Problem:** Same issue as 1.1 -- unconditionally calls `BEGIN`/`COMMIT` regardless of current transaction state.
- **Fix:** Same approach as 1.1 -- check `autocommit` state before wrapping.
- **Status: DONE.** Added `autocommit: bool` parameter to `ingest_batches`. Call site reads `inner.autocommit` and passes it. Transaction wrapping is now conditional.

### 1.3 Inconsistent NUMERIC type mapping in Postgres driver creates schema/data mismatch
- **Location:** `crates/adbc-postgres/src/convert.rs:30` (`pg_type_to_arrow`)
- **Also at:** `crates/adbc-postgres/src/catalog.rs` (`pg_type_str_to_arrow`)
- **Problem:** In `convert.rs`, `PgType::NUMERIC` maps to `DataType::Utf8`. In `catalog.rs`, `"numeric"` and `"decimal"` map to `DataType::Float64`. Schema metadata and runtime data types were inconsistent.
- **Fix:** Map `"numeric" | "decimal"` to `DataType::Utf8` in `catalog.rs` to match runtime conversion.
- **Status: DONE.** `pg_type_str_to_arrow` now maps `"numeric" | "decimal"` to `Utf8`.

## 2. Unsafe Patterns (High Severity)

### 2.1 `collect_reader` uses `unwrap()` in public non-test code
- **Location:** `crates/adbc/src/helpers.rs` (`collect_reader`)
- **Problem:** `collect_reader` calls `.unwrap()` twice -- once on each batch and once on `concat_batches`. The function is `pub` and exported from the core `adbc` crate's top-level namespace.
- **Fix:** Change the return type to `Result<RecordBatch>` and propagate errors with `?`. Update callers.
- **Status: DONE.** `collect_reader` now returns `Result<RecordBatch>`. All 7 callers (tests, examples) updated to `.unwrap()` at the call site.

### 2.2 TLS certificate loading silently ignores all failures
- **Location:** `crates/adbc-postgres/src/lib.rs` (in `#[cfg(feature = "tls")]` connect)
- **Problem:** `let _ = root_store.add(cert);` silently discards any certificate that fails to load. If the root store ends up empty, a confusing TLS error results.
- **Fix:** After the loop, check `if root_store.is_empty()` and return a clear error.
- **Status: DONE.** Added an `is_empty()` check after the cert loading loop that returns `Error::io("No valid TLS root certificates found in system store")`.

## 3. Duplication (Medium Severity)

### 3.1 `SqliteReader` reimplements `OneBatch`
- **Location:** `crates/adbc-sqlite/src/convert.rs` (was `SqliteReader`)
- **Also at:** `crates/adbc/src/helpers.rs` (`OneBatch`)
- **Problem:** `SqliteReader` was structurally identical to `OneBatch`.
- **Fix:** Changed `SqliteReader::execute` to a standalone `execute_query` function that returns `OneBatch` directly. Removed the duplicate `Iterator`/`RecordBatchReader` impls.
- **Status: DONE.** Eliminated ~40 lines of duplicated code.

### 3.2 `extract_bound_params` duplicated across three drivers
- **Location:** `crates/adbc-sqlite/src/lib.rs`, `crates/adbc-postgres/src/convert.rs`, `crates/adbc-mysql/src/convert.rs`
- **Problem:** Identical control flow in all three drivers.
- **Fix:** Extracted a generic `extract_first_row<T>` helper into `adbc::helpers`. All three drivers now delegate to it.
- **Status: DONE.**

### 3.3 `prepare()` match structure duplicated across all four drivers
- **Location:** All four driver `lib.rs` files
- **Problem:** Every driver's `prepare()` has the same 4-arm match. Only the validation call varies.
- **Fix:** Extract a helper `fn prepare_mode(...)` into `adbc::helpers`.
- **Status: SKIPPED.** The duplication is real but each driver's prepare logic has subtle differences (SQLite uses `with_conn` for thread safety, Postgres uses async client calls, FlightSQL is a no-op validation). Extracting a common helper would require an async closure or trait-based abstraction that adds more complexity than it removes. The 4-arm match is idiomatic and easy to understand in each driver.

### 3.4 `new_connection_with_opts` boilerplate duplicated across all four Database impls
- **Location:** All four driver `lib.rs` files
- **Problem:** Identical pattern: `new_connection().await? -> for opt in opts { set_option(opt).await? }`.
- **Fix:** Provide a default implementation in the `Database` trait.
- **Status: SKIPPED.** This would require adding a `where Self::ConnectionType: Connection` bound and making `new_connection_with_opts` a provided method on the trait. While technically correct, it changes the trait's public API contract and the duplication is only ~10 lines per driver. Low ROI for the risk.

### 3.5 `get_table_types_batch` duplicated across three catalog modules
- **Location:** `crates/adbc-sqlite/src/catalog.rs`, `crates/adbc-postgres/src/catalog.rs`, `crates/adbc-mysql/src/catalog.rs`
- **Problem:** Structurally identical batch construction for table types.
- **Fix:** Add `build_table_types_batch(types: &[&str])` to `adbc::helpers`.
- **Status: SKIPPED.** Each implementation is only 5-7 lines and tightly coupled to driver-specific table type lists. Extracting a helper would save minimal code and add an indirection for a trivial operation.

### 3.6 Copy-pasted downcast pattern in Postgres `convert.rs`
- **Location:** `crates/adbc-postgres/src/convert.rs` (`col_to_copy_text`, `batch_row_to_params`)
- **Problem:** Repeated 4-line downcast pattern for every data type.
- **Fix:** Add a `downcast_col` helper.
- **Status: SKIPPED.** The Postgres downcast patterns are not identical across `col_to_copy_text` (returns formatted string) and `batch_row_to_params` (returns boxed trait object). A shared helper would need to be generic over the output transform, making it harder to read. The MySQL `downcast_col` helper works because MySQL only has one usage pattern.

## 4. Inconsistency (Medium Severity)

### 4.1 `arrow-buffer` version not using workspace dependency in `adbc-sqlite`
- **Location:** `crates/adbc-sqlite/Cargo.toml`
- **Problem:** Declares `arrow-buffer = { version = ">=53, <59" }` instead of `arrow-buffer.workspace = true`.
- **Fix:** Changed to `arrow-buffer.workspace = true`.
- **Status: DONE.**

### 4.2 `#[allow(unused_mut)]` in FlightSQL driver
- **Location:** `crates/adbc-flightsql/src/lib.rs`
- **Problem:** `#[allow(unused_mut)]` suppresses a warning that can be avoided by restructuring.
- **Fix:** Restructured to use `let endpoint = ...;` followed by `#[cfg(feature = "tls")] let endpoint = if use_tls { ... } else { endpoint };`. No `mut` or `#[allow]` needed.
- **Status: DONE.**

## 5. Missing Abstractions (Medium Severity)

### 5.1 `sql_info_to_adbc_codes` manually maps u32 to InfoCode
- **Location:** `crates/adbc-flightsql-gateway/src/lib.rs`
- **Problem:** Manual match mapping u32 values to `InfoCode` variants, must be kept in sync.
- **Fix:** Implemented `TryFrom<u32> for InfoCode` on the enum. Replaced the manual `filter_map` match with `.filter_map(|&code| InfoCode::try_from(code).ok())`.
- **Status: DONE.**

### 5.2 No shared ingest DDL dispatch or transaction wrapper
- **Location:** All three SQL driver `convert.rs` files
- **Problem:** All three `ingest_batches` follow the same structure.
- **Fix:** Extract DDL dispatch and transaction-wrapping into shared helpers.
- **Status: SKIPPED.** The transaction-wrapping bug (1.1/1.2) has been fixed. The remaining structural similarity involves driver-specific SQL dialects (Postgres uses COPY, MySQL uses INSERT, SQLite uses synchronous rusqlite), making a shared abstraction awkward. The DDL generation is already driver-specific. Forcing a shared helper would require complex trait bounds or callbacks that hurt readability.

## 6. Noise (Low Severity)

### 6.1 Excessive section divider comments
- **Location:** All source files
- **Problem:** Box-drawing character horizontal rules appearing ~50 times across the codebase.
- **Status: SKIPPED.** This is an established project style. Changing it would create unnecessary churn across every file with no functional benefit.

## 7. Minor Issues (Low Severity)

### 7.1 `SqliteStatement::prepare` clones SQL string twice
- **Location:** `crates/adbc-sqlite/src/lib.rs`
- **Problem:** Both clones used the same name `sql`, making intent unclear.
- **Fix:** Renamed to `sql_owned` and `sql_for_validate`.
- **Status: DONE.**

### 7.2 `unwrap()` on `.last()` in SQLite catalog offsets
- **Location:** `crates/adbc-sqlite/src/catalog.rs`
- **Problem:** `col_offsets.last().unwrap()` called in a loop.
- **Fix:** Replaced with running counters `col_offset` and `cons_offset`.
- **Status: DONE.**

### 7.3 `unwrap()` in `schema.rs` LazyLock initializer
- **Location:** `crates/adbc/src/schema.rs`
- **Problem:** `UnionFields::try_new(...).unwrap()` in a static `LazyLock` with no explanation.
- **Fix:** Added comment: `// Field IDs 0-5 are unique and match the 6 child fields; this cannot fail.`
- **Status: DONE.**
