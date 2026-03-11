# adbc-sqlite — Integration Tests

Tests live in [`adbc-sqlite/tests/integration.rs`](../../adbc-sqlite/tests/integration.rs).

All 34 tests run fully in-process using SQLite in `:memory:` mode — **no external server required**.

```bash
cargo test -p adbc-sqlite
```

---

## Driver / Database / Connection

| Test                      | Description                                               |
| ------------------------- | --------------------------------------------------------- |
| `driver_new_database`     | Creates a default and a `:memory:` database               |
| `driver_bad_option`       | Returns `InvalidArguments` for an unknown database option |
| `db_new_connection`       | Opens a connection                                        |
| `db_connection_with_opts` | Opens a connection with `AutoCommit(true)` option         |

## Transactions

| Test                                | Description                                                                                  |
| ----------------------------------- | -------------------------------------------------------------------------------------------- |
| `conn_autocommit_toggle`            | Disable → commit → rollback → re-enable autocommit                                           |
| `conn_commit_in_autocommit_fails`   | `commit()` in autocommit mode → `InvalidState`                                               |
| `conn_rollback_in_autocommit_fails` | `rollback()` in autocommit mode → `InvalidState`                                             |
| `conn_unknown_option`               | Unknown connection option → non-Ok error                                                     |
| `conn_transaction_isolation`        | Rows visible within tx; CREATE TABLE rolled back post-rollback (SQLite DDL is transactional) |

## Metadata

| Test                              | Description                                                       |
| --------------------------------- | ----------------------------------------------------------------- |
| `conn_get_table_types`            | Returns ≥ 2 rows (at least `"table"` and `"view"`)                |
| `conn_get_info_all`               | All info codes → > 0 rows                                         |
| `conn_get_info_filtered`          | Filter to 2 codes → exactly 2 rows                                |
| `conn_get_info_vendor_name_code`  | Single-code filter; verifies code column value and non-null entry |
| `conn_get_objects`                | `ObjectDepth::All` → output matches `GET_OBJECTS_SCHEMA`          |
| `conn_get_objects_catalogs_depth` | `Catalogs` depth → valid schema, ≥ 1 row                          |
| `conn_get_objects_schemas_depth`  | `Schemas` depth → valid schema                                    |
| `conn_get_table_schema_missing`   | Non-existent table → `NotFound`                                   |
| `conn_get_table_schema_existing`  | Created table → correct field count and names (`a`, `b`)          |

## Queries

| Test                         | Description                                                  |
| ---------------------------- | ------------------------------------------------------------ |
| `stmt_execute_no_query`      | `execute()` without SQL → `InvalidState`                     |
| `stmt_execute_select`        | `SELECT 42 AS answer` → 1 row, 1 column                      |
| `stmt_execute_select_values` | Verifies actual integer and string cell values               |
| `stmt_execute_multi_row`     | 3-row `UNION ALL` query; checks row count and specific cells |
| `stmt_prepare_idempotent`    | `prepare()` is idempotent (called twice)                     |
| `stmt_prepare_no_query`      | `prepare()` without SQL → `InvalidState`                     |
| `stmt_execute_update`        | `CREATE TABLE` + `INSERT`; `execute_update` succeeds         |
| `stmt_bad_option`            | Unknown statement option → `InvalidArguments`                |
| `stmt_reuse_with_new_query`  | Reuse same statement object with a different SQL query       |

## NULL Values

| Test             | Description                                                      |
| ---------------- | ---------------------------------------------------------------- |
| `null_roundtrip` | Arrow nulls in `Int64` and `Utf8` survive write → read unchanged |

## Bulk Ingest

| Test                           | Description                                                          |
| ------------------------------ | -------------------------------------------------------------------- |
| `ingest_roundtrip`             | Create mode; verifies row count, column count, and column names      |
| `ingest_create_already_exists` | Create mode on existing table → `AlreadyExists`                      |
| `ingest_replace`               | Replace mode on an existing table succeeds                           |
| `ingest_append`                | Append mode doubles row count (3 → 6) without truncating             |
| `ingest_bind_stream`           | `bind_stream()` ingests 3 rows; verifies count via `SELECT count(*)` |
| `ingest_large_batch`           | Ingests 1 000 rows; verifies count via `SELECT count(*)`             |
