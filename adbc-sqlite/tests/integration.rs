//! Integration tests for adbc-sqlite.

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;

use adbc::{
    Connection, ConnectionOption, Database, DatabaseOption, Driver, InfoCode, IngestMode,
    ObjectDepth, Statement, StatementOption, Status,
};
use adbc_sqlite::SqliteDriver;

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

fn memory() -> adbc_sqlite::SqliteDatabase {
    SqliteDriver
        .new_database_with_opts([(DatabaseOption::Uri, ":memory:".into())])
        .unwrap()
}

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("val", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(Float64Array::from(vec![1.1f64, 2.2, 3.3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

fn collect(reader: Box<dyn RecordBatchReader + Send>) -> RecordBatch {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&schema, &batches).unwrap()
}

// ─────────────────────────────────────────────────────────────
// Driver
// ─────────────────────────────────────────────────────────────

#[test]
fn driver_new_database() {
    SqliteDriver.new_database().unwrap();
    SqliteDriver
        .new_database_with_opts([(DatabaseOption::Uri, ":memory:".into())])
        .unwrap();
}

#[test]
fn driver_bad_option() {
    let err = SqliteDriver
        .new_database_with_opts([(DatabaseOption::Other("bad".into()), "v".into())])
        .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
}

// ─────────────────────────────────────────────────────────────
// Database
// ─────────────────────────────────────────────────────────────

#[test]
fn db_new_connection() {
    memory().new_connection().unwrap();
}

#[test]
fn db_connection_with_opts() {
    memory()
        .new_connection_with_opts([ConnectionOption::AutoCommit(true)])
        .unwrap();
}

// ─────────────────────────────────────────────────────────────
// Connection — options & transactions
// ─────────────────────────────────────────────────────────────

#[test]
fn conn_autocommit_toggle() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();
    conn.commit().unwrap();
    conn.rollback().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(true)).unwrap();
}

#[test]
fn conn_commit_in_autocommit_fails() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    assert_eq!(conn.commit().unwrap_err().status, Status::InvalidState);
}

#[test]
fn conn_rollback_in_autocommit_fails() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    assert_eq!(conn.rollback().unwrap_err().status, Status::InvalidState);
}

#[test]
fn conn_unknown_option() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    let err = conn
        .set_option(ConnectionOption::Other("x".into(), "v".into()))
        .unwrap_err();
    assert_ne!(err.status, Status::Ok);
}

// ─────────────────────────────────────────────────────────────
// Connection — metadata
// ─────────────────────────────────────────────────────────────

#[test]
fn conn_get_table_types() {
    let db = memory();
    let conn = db.new_connection().unwrap();
    let batch = collect(conn.get_table_types().unwrap());
    assert!(batch.num_rows() >= 2); // at least "table" and "view"
}

#[test]
fn conn_get_info_all() {
    let db = memory();
    let conn = db.new_connection().unwrap();
    let batch = collect(conn.get_info(None).unwrap());
    assert!(batch.num_rows() > 0);
}

#[test]
fn conn_get_info_filtered() {
    let db = memory();
    let conn = db.new_connection().unwrap();
    let codes = [InfoCode::VendorName, InfoCode::DriverName];
    let batch = collect(conn.get_info(Some(&codes)).unwrap());
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn conn_get_objects() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let db = memory();
    let conn = db.new_connection().unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::All, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

#[test]
fn conn_get_table_schema_missing() {
    let db = memory();
    let conn = db.new_connection().unwrap();
    assert_eq!(
        conn.get_table_schema(None, None, "no_such_table")
            .unwrap_err()
            .status,
        Status::NotFound
    );
}

#[test]
fn conn_get_table_schema_existing() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("CREATE TABLE t_schema(a INTEGER, b TEXT)")
        .unwrap();
    stmt.execute_update().unwrap();

    let schema = conn.get_table_schema(None, None, "t_schema").unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "a");
    assert_eq!(schema.field(1).name(), "b");
    conn.rollback().unwrap();
}

// ─────────────────────────────────────────────────────────────
// Statement — queries
// ─────────────────────────────────────────────────────────────

#[test]
fn stmt_execute_no_query() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    assert_eq!(stmt.execute().err().unwrap().status, Status::InvalidState);
}

#[test]
fn stmt_execute_select() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 42 AS answer").unwrap();
    let (reader, _) = stmt.execute().unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
}

#[test]
fn stmt_prepare_idempotent() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 1").unwrap();
    stmt.prepare().unwrap();
    stmt.prepare().unwrap(); // idempotent
}

#[test]
fn stmt_prepare_no_query() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    assert_eq!(stmt.prepare().unwrap_err().status, Status::InvalidState);
}

#[test]
fn stmt_execute_update() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("CREATE TABLE t_upd(x INTEGER)").unwrap();
    stmt.execute_update().unwrap();
    stmt.set_sql_query("INSERT INTO t_upd VALUES(1)").unwrap();
    let rows = stmt.execute_update().unwrap();
    assert!(rows >= 0);
    conn.rollback().unwrap();
}

#[test]
fn stmt_bad_option() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    let err = stmt
        .set_option(StatementOption::Other("bad".into(), "v".into()))
        .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
}

// ─────────────────────────────────────────────────────────────
// Statement — bulk ingest
// ─────────────────────────────────────────────────────────────

#[test]
fn ingest_roundtrip() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    let input = sample_batch();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("ingest_t".into()))
        .unwrap();
    stmt.bind(input.clone()).unwrap();
    stmt.execute_update().unwrap();

    stmt.set_sql_query("SELECT * FROM ingest_t").unwrap();
    let (reader, _) = stmt.execute().unwrap();
    let got = collect(reader);

    assert_eq!(got.num_rows(), input.num_rows());
    assert_eq!(got.num_columns(), input.num_columns());
    for (i, f) in input.schema().fields().iter().enumerate() {
        assert_eq!(got.schema().field(i).name(), f.name());
    }
    conn.rollback().unwrap();
}

#[test]
fn ingest_create_already_exists() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("dup".into()))
        .unwrap();
    stmt.bind(sample_batch()).unwrap();
    stmt.execute_update().unwrap();

    let mut stmt2 = conn.new_statement().unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("dup".into()))
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Create))
        .unwrap();
    stmt2.bind(sample_batch()).unwrap();
    assert_eq!(
        stmt2.execute_update().unwrap_err().status,
        Status::AlreadyExists
    );
    conn.rollback().unwrap();
}

#[test]
fn ingest_replace() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    // First ingest (Create)
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("rep".into()))
        .unwrap();
    stmt.bind(sample_batch()).unwrap();
    stmt.execute_update().unwrap();

    // Replace — should succeed
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("rep".into()))
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Replace))
        .unwrap();
    stmt2.bind(sample_batch()).unwrap();
    stmt2.execute_update().unwrap();

    conn.rollback().unwrap();
}

#[test]
fn ingest_bind_stream() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    struct SingleBatch {
        batch: Option<RecordBatch>,
        schema: Arc<Schema>,
    }
    impl Iterator for SingleBatch {
        type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
        fn next(&mut self) -> Option<Self::Item> {
            Ok(self.batch.take()).transpose()
        }
    }
    impl RecordBatchReader for SingleBatch {
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }
    }

    let batch = sample_batch();
    let schema = batch.schema();
    let reader = SingleBatch {
        batch: Some(batch),
        schema,
    };

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("stream_t".into()))
        .unwrap();
    stmt.bind_stream(Box::new(reader)).unwrap();
    stmt.execute_update().unwrap();

    // Verify row count.
    stmt.set_sql_query("SELECT count(*) FROM stream_t").unwrap();
    let (r, _) = stmt.execute().unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_any().downcast_ref().unwrap();
    assert_eq!(cnt.value(0), 3);
    conn.rollback().unwrap();
}

// ─────────────────────────────────────────────────────────────
// New tests — parity with DuckDB
// ─────────────────────────────────────────────────────────────

/// Rows written inside a transaction are visible within it; gone after rollback.
#[test]
fn conn_transaction_isolation() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("CREATE TABLE sq_iso_t(x INTEGER)")
        .unwrap();
    stmt.execute_update().unwrap();
    stmt.set_sql_query("INSERT INTO sq_iso_t VALUES(99)")
        .unwrap();
    stmt.execute_update().unwrap();

    // Row should be visible within the same transaction.
    stmt.set_sql_query("SELECT x FROM sq_iso_t").unwrap();
    let (r, _) = stmt.execute().unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    conn.rollback().unwrap();

    // After rollback the table is gone (SQLite DDL IS transactional).
    stmt.set_sql_query("SELECT count(*) FROM sqlite_master WHERE name='sq_iso_t'")
        .unwrap();
    let (r2, _) = stmt.execute().unwrap();
    let got = collect(r2);
    let cnt: &Int64Array = got.column(0).as_any().downcast_ref().unwrap();
    assert_eq!(cnt.value(0), 0);
}

/// Filter get_info to VendorName; verify code column and non-null value.
#[test]
fn conn_get_info_vendor_name_code() {
    use arrow_array::cast::AsArray;
    use arrow_array::types::UInt32Type;
    let db = memory();
    let conn = db.new_connection().unwrap();
    let codes = [InfoCode::VendorName];
    let batch = collect(conn.get_info(Some(&codes)).unwrap());
    assert_eq!(batch.num_rows(), 1);
    let code_col = batch.column(0).as_primitive::<UInt32Type>();
    assert_eq!(code_col.value(0), InfoCode::VendorName as u32);
    // Value column must be non-null.
    assert!(!batch.column(1).is_null(0));
}

/// ObjectDepth::Catalogs returns a valid schema with at least one row.
#[test]
fn conn_get_objects_catalogs_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let db = memory();
    let conn = db.new_connection().unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
    assert!(batch.num_rows() >= 1);
}

/// ObjectDepth::Schemas returns a valid schema.
#[test]
fn conn_get_objects_schemas_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let db = memory();
    let conn = db.new_connection().unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::Schemas, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

/// SELECT returns correct cell values, not just the right row count.
#[test]
fn stmt_execute_select_values() {
    use arrow_array::cast::AsArray;
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 7 AS n, 'hello' AS s").unwrap();
    let (r, _) = stmt.execute().unwrap();
    let got = collect(r);
    assert_eq!(got.num_rows(), 1);
    let n: &Int64Array = got.column(0).as_primitive();
    assert_eq!(n.value(0), 7);
    let s: &StringArray = got.column(1).as_string();
    assert_eq!(s.value(0), "hello");
}

/// Multi-row VALUES query returns the right count and specific cells.
#[test]
fn stmt_execute_multi_row() {
    use arrow_array::cast::AsArray;
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    // SQLite doesn't support VALUES() as a table expression; use UNION ALL instead.
    stmt.set_sql_query(
        "SELECT CAST(10 AS INTEGER) AS col1 \
         UNION ALL SELECT 20 \
         UNION ALL SELECT 30",
    )
    .unwrap();
    let (r, _) = stmt.execute().unwrap();
    let got = collect(r);
    assert_eq!(got.num_rows(), 3);
    let col: &Int64Array = got.column(0).as_primitive();
    assert_eq!(col.value(0), 10);
    assert_eq!(col.value(2), 30);
}

/// Reuse the same statement object with a different SQL query.
#[test]
fn stmt_reuse_with_new_query() {
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT 1").unwrap();
    let (r, _) = stmt.execute().unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    stmt.set_sql_query("SELECT 2, 3").unwrap();
    let (r2, _) = stmt.execute().unwrap();
    let got = collect(r2);
    assert_eq!(got.num_rows(), 1);
    assert_eq!(got.num_columns(), 2);
}

/// Arrow NULLs in Int64 and Utf8 survive a write→read roundtrip unchanged.
#[test]
fn null_roundtrip() {
    use arrow_array::cast::AsArray;
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    // Build a batch with deliberate NULLs.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("val", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ],
    )
    .unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("sq_null_t".into()))
        .unwrap();
    stmt.bind(batch).unwrap();
    stmt.execute_update().unwrap();

    stmt.set_sql_query("SELECT id, val FROM sq_null_t ORDER BY rowid")
        .unwrap();
    let (reader, _) = stmt.execute().unwrap();
    let got = collect(reader);

    let ids: &Int64Array = got.column(0).as_primitive();
    assert!(Array::is_null(ids, 1));
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(2), 3);

    let vals: &StringArray = got.column(1).as_string();
    assert!(Array::is_null(vals, 2));
    assert_eq!(vals.value(0), "a");

    conn.rollback().unwrap();
}

/// Append mode adds rows on top of an existing table (does not truncate).
#[test]
fn ingest_append() {
    use arrow_array::cast::AsArray;
    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    // Create with 3 rows.
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("sq_app_t".into()))
        .unwrap();
    stmt.bind(sample_batch()).unwrap();
    stmt.execute_update().unwrap();

    // Append 3 more.
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("sq_app_t".into()))
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Append))
        .unwrap();
    stmt2.bind(sample_batch()).unwrap();
    stmt2.execute_update().unwrap();

    stmt2
        .set_sql_query("SELECT count(*) FROM sq_app_t")
        .unwrap();
    let (r, _) = stmt2.execute().unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), 6);
    conn.rollback().unwrap();
}

/// Ingest 1 000 rows and verify the full count via SELECT count(*).
#[test]
fn ingest_large_batch() {
    use arrow_array::cast::AsArray;
    let n = 1_000usize;
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let big = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..n as i64))],
    )
    .unwrap();

    let db = memory();
    let mut conn = db.new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("sq_big_t".into()))
        .unwrap();
    stmt.bind(big).unwrap();
    stmt.execute_update().unwrap();

    stmt.set_sql_query("SELECT count(*) FROM sq_big_t").unwrap();
    let (r, _) = stmt.execute().unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), n as i64);
    conn.rollback().unwrap();
}
