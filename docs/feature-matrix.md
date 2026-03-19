# Driver Feature Matrix

> Last updated: 2026-03-19

This document tracks feature implementation status and test coverage across all ADBC drivers.
Update this file when a new feature is implemented or a gap is resolved.

---

## Implementation Status

| Feature                                |        SQLite        | Postgres |  MySQL   |     FlightSQL     |
| -------------------------------------- | :------------------: | :------: | :------: | :---------------: |
| `Driver::new_database`                 |          Y          |    Y    |    Y    |        Y         |
| `Driver::new_database_with_opts` (Uri) |          Y          |    Y    |    Y    |        Y         |
| `Database::new_connection`             |          Y          |    Y    |    Y    |        Y         |
| `Connection::AutoCommit`               |          Y          |    Y    |    Y    |        Y         |
| `Connection::IsolationLevel`           | Serializable only    | Y full  | Y full  |    N not impl    |
| `Connection::ReadOnly`                 |          N          |    Y    |    Y    |    N not impl    |
| `Connection::commit / rollback`        |          Y          |    Y    |    Y    |        Y         |
| `Connection::get_table_types`          |          Y          |    Y    |    Y    |        Y         |
| `Connection::get_info`                 |          Y          |    Y    |    Y    |        Y         |
| `Connection::get_objects` (all depths) |          Y          |    Y    |    Y    |        Y         |
| `Connection::get_table_schema`         |          Y          |    Y    |    Y    |        Y         |
| `Statement::execute` (SELECT)          |          Y          |    Y    |    Y    |        Y         |
| `Statement::execute_update` (DML/DDL)  |          Y          |    Y    |    Y    |        Y         |
| `Statement::prepare`                   |          Y          |    Y    |  Y server-side  |  Y server-side   |
| `Statement::bind` (RecordBatch)        |          Y          |    Y    |    Y    |        Y         |
| `Statement::bind_stream`               |          Y          |    Y    |    Y    |        Y         |
| `IngestMode::Create`                   |          Y          |    Y    |    Y    |    N not impl    |
| `IngestMode::Replace`                  |          Y          |    Y    |    Y    |    N not impl    |
| `IngestMode::Append`                   |          Y          |    Y    |    Y    |    N not impl    |
| `IngestMode::CreateAppend`             |          Y          |    Y    |    Y    |    N not impl    |
| NULL value roundtrip (ingest + read)   |          Y          |    Y    |    Y    |        n/a        |

**Legend:** Y = Implemented, Partial = Partial support, N = Not implemented

---

## Known Gaps

### Bulk Ingest -- FlightSQL

All `IngestMode` variants return `NotImplemented`. The ADBC spec allows drivers to not support bulk
ingest, but it should be documented.

**Resolution path:** Use DoPut with a FlightDescriptor to write Arrow data to a server table.

### `Connection::IsolationLevel` -- SQLite

SQLite accepts `IsolationLevel::Serializable` and `IsolationLevel::Default` silently
(SQLite is serializable-only), but rejects all other levels with `NotImplemented`.
This is intentionally correct behavior.

### `Connection::ReadOnly` -- SQLite, FlightSQL

SQLite and FlightSQL do not implement ReadOnly.
PostgreSQL **is** implemented via `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY / READ WRITE`.
MySQL **is** implemented via `SET TRANSACTION READ ONLY / READ WRITE`.

**Resolution path:** SQLite: open with `SQLITE_OPEN_READONLY`.

---

## Test Coverage Matrix

| Test                                 | SQLite | Postgres |   MySQL    | FlightSQL |
| ------------------------------------ | :----: | :------: | :--------: | :-------: |
| `driver_new_database`                |   Y    |    Y    |     --      |     --     |
| `driver_bad_option`                  |   Y    |    Y    |     Y     |     --     |
| `db_new_connection`                  |   Y    |    Y    |     Y     |    Y     |
| `conn_autocommit_toggle`             |   Y    |    Y    |     Y     |     --     |
| `conn_commit_in_autocommit_fails`    |   Y    |    Y    |    Y     |     --     |
| `conn_rollback_in_autocommit_fails`  |   Y    |    Y    |    Y     |     --     |
| `conn_transaction_isolation`         |   Y    |    Y    |     Y     |     --     |
| `conn_unknown_option`                |   Y    |    Y    |     Y     |     --     |
| `conn_get_table_types`               |   Y    |    Y    |     Y     |    Y     |
| `conn_get_info_all`                  |   Y    |    Y    |     Y     |    Y     |
| `conn_get_info_filtered`             |   Y    |    Y    |     Y     |     --     |
| `conn_get_info_vendor_name_code`     |   Y    |    Y    |     Y     |    Y     |
| `conn_get_objects` (All)             |   Y    |    Y    |     Y     |    Y     |
| `conn_get_objects_catalogs_depth`    |   Y    |    Y    |     Y     |     --     |
| `conn_get_objects_schemas_depth`     |   Y    |    Y    |     Y     |     --     |
| `conn_get_table_schema_missing`      |   Y    |    Y    |     Y     |    Y     |
| `conn_get_table_schema_existing`     |   Y    |    Y    |     Y     |     --     |
| `stmt_execute_no_query`              |   Y    |    Y    |     --      |     --     |
| `stmt_execute_select`                |   Y    |    Y    |     Y     |    Y     |
| `stmt_execute_select_values`         |   Y    |    Y    |     Y     |     --     |
| `stmt_execute_multi_row`             |   Y    |    Y    |     Y     |     --     |
| `stmt_execute_update`                |   Y    |    Y    |     Y     |     --     |
| `stmt_prepare_and_execute`           |   Y    |    Y    |     --      |    Y     |
| `stmt_prepare_no_query`              |   Y    |    Y    | Y (no-op) |    Y     |
| `stmt_bad_option`                    |   Y    |    Y    |     --      |     --     |
| `stmt_reuse_with_new_query`          |   Y    |    Y    |     Y     |     --     |
| `null_roundtrip`                     |   Y    |    Y    |     Y     |     --     |
| `ingest_roundtrip`                   |   Y    |    Y    |     Y     |     --     |
| `ingest_create_already_exists`       |   Y    |    Y    |     --      |     --     |
| `ingest_replace`                     |   Y    |    Y    |     Y     |     --     |
| `ingest_append`                      |   Y    |    Y    |     Y     |     --     |
| `ingest_bind_stream`                 |   Y    |    Y    |     Y     |     --     |
| `ingest_large_batch`                 |   Y    |    Y    |     Y     |     --     |
| `ingest_not_implemented` (FlightSQL) |   --    |    --     |     --      |    Y     |
| **Total tests**                      | **34** |  **32**  |   **28**   |  **10**   |

**Legend:** Y = Has test, -- = Not applicable or not yet added
