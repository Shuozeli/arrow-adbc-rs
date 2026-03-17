# Code Audit Findings

**Date:** 2026-03-17
**Status:** In progress

## Overview

Comprehensive audit of the arrow-adbc-rs codebase. Findings ordered by severity.

---

## Finding 1: Bound Parameters Silently Ignored (3 of 4 Drivers)

**Severity:** CRITICAL -- silent data corruption
**Status:** FIXED (PostgreSQL, MySQL) / PARTIAL (FlightSQL -- txn_id fixed, bound_data still TODO)
**Affected:** adbc-postgres, adbc-mysql, adbc-flightsql

When `bind()` is called to set query parameters then `execute()` or
`execute_update()` is called, the parameters were silently dropped. The SQLite
driver had this same bug and was fixed in commit `6d37dd9`, but the fix was
never propagated.

**Fix:** Added `batch_row_to_params()` and `extract_bound_params()` to
`convert.rs` in both adbc-postgres and adbc-mysql, mirroring the SQLite fix.
`execute()` and `execute_update()` now extract bound params and pass them
through to the underlying database clients.

---

## Finding 2: Unsafe Pointer Cast With No Compile-Time Guard

**Severity:** CRITICAL -- potential undefined behavior
**Status:** MITIGATED (runtime size assertion added)
**Affected:** adbc-flightsql

Casts between `arrow_flight::RecordBatch` (v55) and `arrow_array::RecordBatch`
(v58) via raw pointer reinterpretation. Root cause: `arrow-flight 55.x` pins to
`arrow-array 55.x` while the workspace resolves `arrow-array 58.x`.

**Fix:** Added `assert_recordbatch_layout_compatible()` runtime guard that
panics if size diverges. Full fix requires aligning arrow-flight and arrow-array
to the same major version (blocked on tonic 0.12 compatibility).

---

## Finding 3: FlightSQL Silently Drops Transaction IDs

**Severity:** HIGH -- broken transaction isolation
**Status:** FIXED
**Affected:** adbc-flightsql

`crates/adbc-flightsql/src/catalog.rs:60-63,76-79`:
Both `execute_query` and `execute_update` accepted `_transaction_id` then passed
`None` to the underlying FlightSQL client.

**Fix:** Changed both functions to pass `transaction_id` through to the
FlightSQL client's `execute()` and `execute_update()` calls.

---

## Finding 4: Tests Verify Shape But Not Data

**Severity:** HIGH -- bugs masked by weak tests
**Status:** FIXED (sqlite); postgres/mysql still TODO (require live databases)
**Affected:** all integration test suites

`ingest_roundtrip` (sqlite/tests/integration.rs:299-322) checks `num_rows`,
`num_columns`, and column names but never checks actual cell values. This test
would pass even if every value were NULL (exactly the bug from Finding 1).

Similar pattern across postgres and mysql test suites.

**Fix:** SQLite `ingest_roundtrip` test now verifies actual Int64, Float64, and
Utf8 cell values match the input batch, not just shape.

---

## Finding 5: Inverted Boolean Naming in Transaction Logic

**Severity:** MEDIUM -- misleading, accident-prone
**Status:** FIXED
**Affected:** adbc-sqlite

**Fix:** Renamed `in_explicit_txn` to `needs_txn` with a clarifying comment.

---

## Finding 6: Redundant Clone

**Severity:** MEDIUM -- wasteful allocation
**Status:** FIXED
**Affected:** adbc-sqlite

**Fix:** Renamed `sql_clone` to `sql_for_closure` for clarity. The two clones
are still needed (one moves into the closure, one stays for `Mode::Prepared`),
but the naming now makes the intent clear.

---

## Finding 7: MySQL prepare() Is a Silent No-Op

**Severity:** MEDIUM -- API contract violation
**Status:** TODO
**Affected:** adbc-mysql

`crates/adbc-mysql/src/lib.rs:384-391`: `prepare()` always returns `Ok(())`
without actually preparing anything. Invalid SQL is not detected until execute
time.

---

## Finding 8: Weak Error Assertions in Tests

**Severity:** MEDIUM -- tests pass for wrong reasons
**Status:** FIXED
**Affected:** all test suites

**Fix:** Changed `assert_ne!(err.status, Status::Ok)` to
`assert_eq!(err.status, Status::InvalidArguments)` in all three test suites.

---

## Finding 9: Code Duplication

**Severity:** LOW -- maintenance burden
**Status:** TODO
**Affected:** all crates

Identical `collect()` helper copy-pasted 6 times across test/example files.
Near-identical catalog implementations (`get_info_batch`, `get_table_types_batch`,
`get_objects_batch`) across sqlite/postgres/mysql with no shared abstraction.

---

## Finding 10: Unimplemented Core Features

**Severity:** LOW -- incomplete driver implementations
**Status:** TODO
**Affected:** adbc-flightsql, adbc-mysql

| Feature | Driver | Status |
|---------|--------|--------|
| Bulk ingest | FlightSQL | `not_impl` error |
| `execute_update` on prepared stmt | FlightSQL | `not_impl` error |
| `get_table_schema` | FlightSQL | Missing entirely |
| Isolation level | FlightSQL | Not implemented |
| Read-only mode | FlightSQL | Not implemented |
| `prepare()` | MySQL | No-op |
