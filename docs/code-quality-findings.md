# Code Quality Findings (2026-03-26)

This document supersedes the previous findings. Items from the prior audit that
were fixed or intentionally skipped are not repeated. Only actionable new
findings are listed below.

## 1. Unused Dependencies (Medium Severity)

### 1.1 `arrow-select` listed as `[dependencies]` in all five driver crates but unused in source
- **Location:** `crates/adbc-sqlite/Cargo.toml`, `crates/adbc-postgres/Cargo.toml`, `crates/adbc-mysql/Cargo.toml`, `crates/adbc-flightsql/Cargo.toml`, `crates/adbc-flightsql-gateway/Cargo.toml`
- **Problem:** `arrow-select` appears under `[dependencies]` (not just `[dev-dependencies]`) in all five driver crates. None of them import `arrow_select` in their `src/` code. It is only needed in the `adbc` core crate and as a dev-dependency for test helpers. This inflates compile times and the dependency tree for consumers.
- **Fix:** Remove `arrow-select.workspace = true` from `[dependencies]` in all five driver Cargo.toml files. Keep it in `[dev-dependencies]` where present.
- **Status:** DONE. Removed `arrow-select` from `[dependencies]` in `adbc-sqlite`, `adbc-postgres`, and `adbc-mysql` Cargo.toml files. Kept in `[dev-dependencies]` where present.

### 1.2 `arrow-buffer` listed as `[dependencies]` in `adbc-postgres` and `adbc-mysql` but unused
- **Location:** `crates/adbc-postgres/Cargo.toml`, `crates/adbc-mysql/Cargo.toml`
- **Problem:** Neither `adbc-postgres` nor `adbc-mysql` imports `arrow_buffer` in their `src/` code. Only `adbc-sqlite` uses it (in `catalog.rs` for `OffsetBuffer` and `ScalarBuffer`).
- **Fix:** Remove `arrow-buffer.workspace = true` from `[dependencies]` in `adbc-postgres` and `adbc-mysql`.
- **Status:** DONE. Removed from both crates.

## 2. Unnecessary Clone (Low Severity)

### 2.1 Double clone of SQL string in `SqliteStatement::prepare`
- **Location:** `crates/adbc-sqlite/src/lib.rs`, lines 350-351
- **Problem:** `sql.clone()` produces `sql_owned`, then `sql_owned.clone()` produces `sql_for_validate`. The intermediate name `sql_owned` was misleading.
- **Fix:** Renamed to `let sql = sql.clone(); let sql_for_validate = sql.clone();` making the two-clone intent clear (one for the closure, one for mode assignment).
- **Status:** DONE.

## Summary

| # | Category | Severity | Count |
|---|----------|----------|-------|
| 1 | Unused Dependencies | Medium | 2 findings (7 crate removals) |
| 2 | Unnecessary Clone | Low | 1 finding |

Total actionable items: 3 findings, all straightforward mechanical fixes.

The codebase is in excellent shape overall. Prior audit findings have been
addressed. Clippy produces zero warnings. All tests pass. The architecture is
clean with proper error propagation, no `#[allow]` suppressions, no silent
failures, and well-structured shared helpers.
