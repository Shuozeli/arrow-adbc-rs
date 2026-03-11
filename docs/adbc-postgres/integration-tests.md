# adbc-postgres — Integration Tests

Tests live in [`adbc-postgres/tests/integration.rs`](../../adbc-postgres/tests/integration.rs).

All tests except `driver_bad_option` are marked `#[ignore]` and require a live PostgreSQL server.

```bash
ADBC_POSTGRES_URI="host=localhost port=5432 user=adbc_test password=adbc_test dbname=adbc_test" \
  cargo test -p adbc-postgres -- --include-ignored
```

> See [`docs/development.md`](../development.md) for database setup instructions.

---

## Driver / Database / Connection

| Test                            | Description                                                           |
| ------------------------------- | --------------------------------------------------------------------- |
| `driver_new_database_with_opts` | Connects using the URI option                                         |
| `driver_bad_option`             | Returns `InvalidArguments` for an unknown database option _(offline)_ |
| `db_new_connection`             | Opens a connection                                                    |

## Transactions

| Test                              | Description                                                 |
| --------------------------------- | ----------------------------------------------------------- |
| `conn_autocommit_toggle`          | Disable -> commit -> rollback -> re-enable autocommit       |
| `conn_commit_in_autocommit_fails` | `commit()` in autocommit mode -> `InvalidState`             |
| `conn_transaction_isolation`      | Table created inside tx; row visible; rollback discards DML |
| `conn_unknown_option`             | Unknown connection option -> non-Ok error                   |

## Metadata

| Test                              | Description                                                       |
| --------------------------------- | ----------------------------------------------------------------- |
| `conn_get_table_types`            | Returns >= 2 rows                                                 |
| `conn_get_info_all`               | All info codes -> > 0 rows                                        |
| `conn_get_info_filtered`          | Filter to 2 codes -> exactly 2 rows                               |
| `conn_get_info_vendor_name_code`  | Single-code filter; verifies code column value and non-null entry |
| `conn_get_objects`                | `ObjectDepth::All` -> output matches `GET_OBJECTS_SCHEMA`         |
| `conn_get_objects_catalogs_depth` | `Catalogs` depth -> valid schema, >= 1 row                        |
| `conn_get_objects_schemas_depth`  | `Schemas` depth -> valid schema                                   |
| `conn_get_table_schema_missing`   | Non-existent table -> `NotFound`                                  |
| `conn_get_table_schema_existing`  | Created table -> correct field count and names (`a`, `b`)         |

## Queries

| Test                         | Description                                            |
| ---------------------------- | ------------------------------------------------------ |
| `stmt_execute_select`        | `SELECT 42::BIGINT` -> 1 row                           |
| `stmt_execute_select_values` | Verifies actual integer and string cell values         |
| `stmt_execute_multi_row`     | `generate_series(1,5)` -> 5 rows                       |
| `stmt_prepare_and_execute`   | `prepare()` then `execute()` returns correct result    |
| `stmt_reuse_with_new_query`  | Reuse same statement object with a different SQL query |
| `stmt_execute_update`        | `CREATE TABLE` + `INSERT`; row count == 1              |

## NULL Values

| Test             | Description                                                      |
| ---------------- | ---------------------------------------------------------------- |
| `null_roundtrip` | Arrow nulls in `Int64` and `Utf8` survive write -> read unchanged |

## Bulk Ingest

| Test                 | Description                                                      |
| -------------------- | ---------------------------------------------------------------- |
| `ingest_roundtrip`   | Create mode; verifies row count and column names                 |
| `ingest_replace`     | Replace mode on existing table succeeds                          |
| `ingest_append`      | Append mode doubles the row count without truncating             |
| `ingest_bind_stream` | `bind_stream()` ingests 3 rows via streaming `RecordBatchReader` |
| `ingest_large_batch` | Ingests 1 000 rows; verifies count via `SELECT count(*)`        |
