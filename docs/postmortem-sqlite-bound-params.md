# Post-mortem: SQLite driver silently dropped bound parameters

**Date:** 2026-03-13
**Severity:** High -- data corruption (silent NULL insertion)
**Affected:** `adbc-sqlite` crate, `execute_update` and `execute` methods

## Summary

The SQLite ADBC driver's `execute_update` and `execute` methods ignored bound
parameters entirely. Parameterized queries like `INSERT INTO t (v) VALUES (?1)`
with bound data would silently insert NULL instead of the bound value. SELECT
queries with bound parameters would return incorrect results.

## Root cause

Three independent bugs combined:

1. **`execute_update` used `execute_batch` instead of `execute`.**
   `rusqlite::Connection::execute_batch` does not support parameter binding.
   Unbound `?N` placeholders are treated as NULL by SQLite, so INSERTs
   succeeded but wrote NULLs. The `bound_data` field (populated by `bind()`)
   was never read in this code path.

2. **`execute` (query path) passed empty params to `SqliteReader`.**
   `SqliteReader::execute` always called `stmt.query([])` with no parameters,
   ignoring any data bound via `Statement::bind()`.

3. **`array_value` didn't handle `DataType::Binary`.**
   Even after fixing (1) and (2), BLOB parameters failed because the
   `array_value` function -- which converts Arrow arrays to rusqlite values --
   had no match arm for `DataType::Binary`, causing an "unsupported type" error.

## Impact

- Any parameterized DML (`INSERT`, `UPDATE`, `DELETE`) via ADBC would silently
  write NULLs for all bound columns.
- Parameterized SELECT queries would ignore WHERE-clause parameters, returning
  wrong result sets.
- The bug was masked in tests because most adbc-sqlite tests used non-parameterized
  SQL or bulk ingest (which had its own separate, correct parameter path).

## Fix

- `execute_update`: Check for `bound_data`. If present, use
  `rusqlite::Connection::execute` (which supports params) instead of
  `execute_batch`. Fall back to `execute_batch` only when no params are bound.
- `execute` (query): Pass extracted bound params through to
  `SqliteReader::execute`, which now accepts `Option<&[rusqlite::types::Value]>`
  and binds them to the prepared statement.
- `array_value`: Added `DataType::Binary` arm that downcasts to `BinaryArray`
  and returns `rusqlite::types::Value::Blob`.
- Added `batch_row_to_params` public helper in `convert.rs` to extract a single
  row of rusqlite params from a bound `RecordBatch`.

## Detection

Caught by `quiver-driver-sqlite` integration tests (`blob_roundtrip`,
`bool_roundtrip`) which do INSERT-then-SELECT roundtrips with typed assertions.
These tests failed on CI after the async-first migration (v0.2.0) because the
rewrite introduced the `execute_batch` regression.

## Lessons

- **Bound data must be tested end-to-end.** The adbc-sqlite test suite tested
  ingest and raw SQL separately, but never tested `bind()` + `execute_update()`
  together for parameterized DML.
- **Silent NULL insertion is the worst failure mode.** SQLite's permissive type
  system means unbound parameters don't error -- they just produce NULL. Prefer
  explicit parameter count validation at the driver level.
- **Exhaustive type coverage in conversion functions.** Any function that maps
  Arrow types to driver-native types must handle all types that the driver's DDL
  generator can produce. If `dt_to_sqlite` maps `Binary` to `BLOB`, then
  `array_value` must handle `DataType::Binary`.
