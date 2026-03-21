# Code Quality Audit Findings

## 1. Duplication (High Severity)

### 1.1 `with_conn` duplicated in SqliteConnection and SqliteStatement
- **Files:** `crates/adbc-sqlite/src/lib.rs`
- **Problem:** The `with_conn` method was identically copy-pasted in both `SqliteConnection` and `SqliteStatement`. Both clone an `Arc<Mutex<SqliteInner>>`, spawn_blocking, lock, and call a closure.
- **Fix:** Extract a standalone `with_conn` function that takes `Arc<Mutex<SqliteInner>>` as a parameter.
- **Status:** DONE

### 1.2 `make_empty_col_struct` copy-pasted across 3 catalog modules
- **Files:** `crates/adbc-sqlite/src/catalog.rs`, `crates/adbc-postgres/src/catalog.rs`, `crates/adbc-mysql/src/catalog.rs`
- **Problem:** The 19-field column struct construction was identical in all three drivers. Same applies to `make_empty_cons_struct`, `make_empty_str_list`, `make_empty_i32_map`, `make_empty_col_list`, `make_empty_cons_list`.
- **Fix:** Move these shared builders into the `adbc` core crate's `helpers` module.
- **Status:** DONE

### 1.3 `get_info_batch` union construction duplicated across 3 catalog modules
- **Files:** `crates/adbc-sqlite/src/catalog.rs`, `crates/adbc-postgres/src/catalog.rs`, `crates/adbc-mysql/src/catalog.rs`
- **Problem:** The union array construction logic (extracting union_fields from GET_INFO_SCHEMA, building child arrays, constructing the UnionArray, assembling the RecordBatch) was nearly identical across all three. Only the data values differ.
- **Fix:** Extract a shared `build_get_info_batch` function in `adbc::helpers` that takes string/bool/int values and an INFO_ITEMS table, and builds the batch.
- **Status:** DONE

### 1.4 `Mode` enum duplicated across 3 drivers
- **Files:** `crates/adbc-sqlite/src/lib.rs`, `crates/adbc-postgres/src/lib.rs`, `crates/adbc-mysql/src/lib.rs`
- **Problem:** Identical `Mode` enum (`Idle`, `Sql(String)`, `Prepared(String)`, `Ingest { table, mode }`) defined in each driver.
- **Fix:** Move the `Mode` enum to the `adbc` core crate as `StatementMode`.
- **Status:** DONE

### 1.5 `set_option` on Statement nearly identical across 3 drivers
- **Files:** All three driver `lib.rs` files, in the `Statement::set_option` implementation.
- **Problem:** The logic for `TargetTable` and `IngestMode` was identical in all 3 drivers.
- **Fix:** Extract a `set_statement_option` helper in `adbc::helpers` that operates on the shared `StatementMode` type.
- **Status:** DONE

### 1.6 `extract_bound_params` pattern duplicated across all 3 drivers
- **Files:** `crates/adbc-sqlite/src/lib.rs`, `crates/adbc-postgres/src/convert.rs`, `crates/adbc-mysql/src/convert.rs`
- **Problem:** Same pattern: get batches ref, get first, check num_rows, call batch_row_to_params. The inner batch_row_to_params differs per driver (different DB param types), but the outer shell is identical.
- **Fix:** Not fixable without generics since inner types differ. Acceptable duplication.
- **Status:** SKIPPED (acceptable -- different return types)

### 1.7 `build_table_arrays` duplicated in Postgres/MySQL catalogs
- **Files:** `crates/adbc-postgres/src/catalog.rs`, `crates/adbc-mysql/src/catalog.rs`
- **Problem:** These were identical.
- **Fix:** Consolidated via the shared `build_table_arrays_simple` helper in `adbc::helpers`.
- **Status:** DONE

### 1.8 `get_objects_batch` struct assembly duplicated across all 3 catalog modules
- **Files:** `crates/adbc-sqlite/src/catalog.rs`, `crates/adbc-postgres/src/catalog.rs`, `crates/adbc-mysql/src/catalog.rs`
- **Problem:** The table struct -> tables list -> schema struct -> schemas list -> final batch assembly (~50 lines) was copy-pasted in all three drivers with only the schema name value differing.
- **Fix:** Extract `build_get_objects_batch` helper in `adbc::helpers` that takes table arrays, schema name, and flags.
- **Status:** DONE

## 2. Unnecessary Clone (Medium Severity)

### 2.1 Double clone in SQLite `prepare`
- **File:** `crates/adbc-sqlite/src/lib.rs` lines 350-351
- **Problem:** `let sql = sql.clone(); let validate_sql = sql.clone();` -- the first clone is from the match ref, and then it's immediately cloned again.
- **Fix:** The two clones are structurally necessary (one for the `'static` closure sent to `spawn_blocking`, one for setting `StatementMode::Prepared(sql)` after the await). Renamed for clarity.
- **Status:** DONE (clarified naming, two clones are required)

## 3. Dead Code / Indirection

### 3.1 `collect_info_pub` unnecessary wrapper in FlightSQL catalog
- **File:** `crates/adbc-flightsql/src/catalog.rs`
- **Problem:** `collect_info_pub` was a `pub(crate)` function that simply called `collect_info`. This added a layer of indirection with no value.
- **Fix:** Made `collect_info` directly `pub(crate)` and removed the wrapper.
- **Status:** DONE

## 4. Noise / Excessive Dividers (Low Severity)

### 4.1 Heavy box-drawing section dividers throughout codebase
- **Files:** All `.rs` files
- **Problem:** Lines like `// ─────────────────────────────────────────────────────────────` appear repeatedly.
- **Fix:** Leave as-is -- consistent across the project and part of established style.
- **Status:** SKIPPED (project style)

## 5. `#[allow(...)]` Suppression

### 5.1 `#[allow(unused_mut)]` in FlightSQL
- **File:** `crates/adbc-flightsql/src/lib.rs` line 124
- **Problem:** `#[allow(unused_mut)]` on `endpoint` variable. The `mut` is only needed when the `tls` feature is enabled.
- **Fix:** Leave as-is -- this is `unused_mut` (not clippy), and is a legitimate pattern for cfg-conditional code.
- **Status:** SKIPPED (legitimate cfg pattern)

## 6. Unsafe Code

### 6.1 Unsafe pointer cast in FlightSQL catalog
- **File:** `crates/adbc-flightsql/src/catalog.rs` lines 60-63, 95-98
- **Problem:** Raw pointer casts between arrow-flight's RecordBatch and this crate's RecordBatch. Has a runtime layout assertion. Documented with SAFETY comments.
- **Fix:** Leave as-is -- known workaround for semver-incompatible arrow crate versions. Well-documented and guarded.
- **Status:** SKIPPED (documented, guarded)

## 7. `unwrap()` in Non-Test Code

### 7.1 `collect_reader` helper panics on error
- **File:** `crates/adbc/src/helpers.rs` lines 103-104
- **Problem:** Uses `.unwrap()` twice. However, the doc comment explicitly states "Panics if any batch fails or concatenation fails" and it is described as "useful in tests and examples."
- **Fix:** Leave as-is -- this is a test/example utility with documented panic behavior.
- **Status:** SKIPPED (documented test utility)

### 7.2 `.last().unwrap()` in SQLite catalog `build_table_arrays`
- **File:** `crates/adbc-sqlite/src/catalog.rs` lines 285, 287, 289
- **Problem:** Uses `.last().unwrap()` on offset vectors. These vectors are initialized with `vec![0]` and only appended to, so `.last()` always returns `Some`.
- **Fix:** Leave as-is -- the invariant (non-empty vec) is locally obvious and structurally guaranteed.
- **Status:** SKIPPED (invariant guaranteed)

## Summary

| Category | Found | Fixed | Skipped |
|----------|-------|-------|---------|
| Duplication | 8 | 7 | 1 |
| Unnecessary Clone | 1 | 1 | 0 |
| Dead Code / Indirection | 1 | 1 | 0 |
| Noise | 1 | 0 | 1 |
| `#[allow]` Suppression | 1 | 0 | 1 |
| Unsafe Code | 1 | 0 | 1 |
| `unwrap()` in non-test code | 2 | 0 | 2 |
| **Total** | **15** | **9** | **6** |

### Net Impact
- Removed ~130 lines of duplicated code across the three catalog modules
- Added `build_get_objects_batch` helper to `adbc::helpers`, reducing each driver's catalog module by ~50 lines
- Removed unnecessary `collect_info_pub` wrapper in FlightSQL
- All changes verified with `cargo build`, `cargo test`, `cargo clippy`, and `cargo fmt`
