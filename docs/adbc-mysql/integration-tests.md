# adbc-mysql — Integration Tests

Tests live in [`adbc-mysql/tests/integration.rs`](../../adbc-mysql/tests/integration.rs).

All tests except `driver_bad_option` are marked `#[ignore]` and require a live MySQL server.

```bash
ADBC_MYSQL_URI="mysql://adbc_test:adbc_test@localhost:3306/adbc_test" \
  cargo test -p adbc-mysql -- --include-ignored
```

> See [`docs/development.md`](../development.md) for database setup instructions.

> **Non-transactional DDL:** MySQL `CREATE TABLE` / `DROP TABLE` are auto-committed
> and **not** rolled back by `rollback()`. Ingest tests therefore use
> `IngestMode::Replace` for the initial table creation, making them idempotent
> across re-runs.

---

## Driver / Database / Connection

| Test                | Description                                                           |
| ------------------- | --------------------------------------------------------------------- |
| `driver_bad_option` | Returns `InvalidArguments` for an unknown database option _(offline)_ |
| `db_new_connection` | Opens a connection                                                    |

## Transactions

| Test                         | Description                                                                  |
| ---------------------------- | ---------------------------------------------------------------------------- |
| `conn_autocommit_toggle`     | Disable autocommit -> rollback -> re-enable                                  |
| `conn_transaction_isolation` | Table pre-dropped; row inserted and visible within tx; DML rolled back after |

## Metadata

| Test                              | Description                                                       |
| --------------------------------- | ----------------------------------------------------------------- |
| `conn_get_table_types`            | Returns >= 1 row                                                  |
| `conn_get_info_all`               | All info codes -> > 0 rows                                        |
| `conn_get_info_filtered`          | Filter to 2 codes -> exactly 2 rows                               |
| `conn_get_info_vendor_name_code`  | Single-code filter; verifies code column value and non-null entry |
| `conn_get_objects`                | `ObjectDepth::All` -> output matches `GET_OBJECTS_SCHEMA`         |
| `conn_get_objects_catalogs_depth` | `Catalogs` depth -> valid schema, >= 1 row                        |
| `conn_get_objects_schemas_depth`  | `Schemas` depth -> valid schema                                   |
| `conn_get_table_schema_missing`   | Non-existent table -> `NotFound`                                  |
| `conn_get_table_schema_existing`  | Created table -> correct field count and names (`a`, `b`)         |

## Queries

| Test                         | Description                                                            |
| ---------------------------- | ---------------------------------------------------------------------- |
| `stmt_execute_select`        | `SELECT 42` -> 1 row                                                   |
| `stmt_execute_select_values` | Verifies actual integer and string cell values                         |
| `stmt_execute_multi_row`     | 3-row `UNION ALL` query; checks row count and specific cells           |
| `stmt_prepare_no_query`      | Documents known gap: MySQL `prepare()` is a no-op, always returns `Ok` |
| `stmt_reuse_with_new_query`  | Reuse same statement object with a different SQL query                 |
| `stmt_execute_update`        | `CREATE TABLE` + `INSERT`; cleanup via `DROP TABLE IF EXISTS`          |

## NULL Values

| Test             | Description                                                                                             |
| ---------------- | ------------------------------------------------------------------------------------------------------- |
| `null_roundtrip` | Arrow nulls in `Int64` and `Utf8` survive ingest -> read unchanged (uses `Replace` mode for idempotency) |

## Bulk Ingest

| Test                 | Description                                              |
| -------------------- | -------------------------------------------------------- |
| `ingest_roundtrip`   | Replace mode; verifies row count                         |
| `ingest_replace`     | Two successive Replace ingests both succeed              |
| `ingest_append`      | Replace then Append -> 6 rows total                      |
| `ingest_bind_stream` | Replace via `bind_stream()` (streaming reader)           |
| `ingest_large_batch` | Ingests 1 000 rows; verifies count via `SELECT count(*)` |
