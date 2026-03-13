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

async fn memory() -> adbc_sqlite::SqliteDatabase {
    SqliteDriver
        .new_database_with_opts([(DatabaseOption::Uri, ":memory:".into())])
        .await
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

#[tokio::test]
async fn driver_new_database() {
    SqliteDriver.new_database().await.unwrap();
    SqliteDriver
        .new_database_with_opts([(DatabaseOption::Uri, ":memory:".into())])
        .await
        .unwrap();
}

#[tokio::test]
async fn driver_bad_option() {
    let err = SqliteDriver
        .new_database_with_opts([(DatabaseOption::Other("bad".into()), "v".into())])
        .await
        .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
}

// ─────────────────────────────────────────────────────────────
// Database
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn db_new_connection() {
    memory().await.new_connection().await.unwrap();
}

#[tokio::test]
async fn db_connection_with_opts() {
    memory()
        .await
        .new_connection_with_opts([ConnectionOption::AutoCommit(true)])
        .await
        .unwrap();
}

// ─────────────────────────────────────────────────────────────
// Connection — options & transactions
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn conn_autocommit_toggle() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();
    conn.commit().await.unwrap();
    conn.rollback().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(true))
        .await
        .unwrap();
}

#[tokio::test]
async fn conn_commit_in_autocommit_fails() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    assert_eq!(
        conn.commit().await.unwrap_err().status,
        Status::InvalidState
    );
}

#[tokio::test]
async fn conn_rollback_in_autocommit_fails() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    assert_eq!(
        conn.rollback().await.unwrap_err().status,
        Status::InvalidState
    );
}

#[tokio::test]
async fn conn_unknown_option() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let err = conn
        .set_option(ConnectionOption::Other("x".into(), "v".into()))
        .await
        .unwrap_err();
    assert_ne!(err.status, Status::Ok);
}

// ─────────────────────────────────────────────────────────────
// Connection — metadata
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn conn_get_table_types() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let batch = collect(conn.get_table_types().await.unwrap());
    assert!(batch.num_rows() >= 2); // at least "table" and "view"
}

#[tokio::test]
async fn conn_get_info_all() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let batch = collect(conn.get_info(None).await.unwrap());
    assert!(batch.num_rows() > 0);
}

#[tokio::test]
async fn conn_get_info_filtered() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let codes = [InfoCode::VendorName, InfoCode::DriverName];
    let batch = collect(conn.get_info(Some(&codes)).await.unwrap());
    assert_eq!(batch.num_rows(), 2);
}

#[tokio::test]
async fn conn_get_objects() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::All, None, None, None, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

#[tokio::test]
async fn conn_get_table_schema_missing() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    assert_eq!(
        conn.get_table_schema(None, None, "no_such_table")
            .await
            .unwrap_err()
            .status,
        Status::NotFound
    );
}

#[tokio::test]
async fn conn_get_table_schema_existing() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("CREATE TABLE t_schema(a INTEGER, b TEXT)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();

    let schema = conn.get_table_schema(None, None, "t_schema").await.unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "a");
    assert_eq!(schema.field(1).name(), "b");
    conn.rollback().await.unwrap();
}

// ─────────────────────────────────────────────────────────────
// Statement — queries
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn stmt_execute_no_query() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    assert_eq!(
        stmt.execute().await.err().unwrap().status,
        Status::InvalidState
    );
}

#[tokio::test]
async fn stmt_execute_select() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("SELECT 42 AS answer").await.unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
}

#[tokio::test]
async fn stmt_prepare_idempotent() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("SELECT 1").await.unwrap();
    stmt.prepare().await.unwrap();
    stmt.prepare().await.unwrap(); // idempotent
}

#[tokio::test]
async fn stmt_prepare_no_query() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    assert_eq!(
        stmt.prepare().await.unwrap_err().status,
        Status::InvalidState
    );
}

#[tokio::test]
async fn stmt_execute_update() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("CREATE TABLE t_upd(x INTEGER)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();
    stmt.set_sql_query("INSERT INTO t_upd VALUES(1)")
        .await
        .unwrap();
    let rows = stmt.execute_update().await.unwrap();
    assert!(rows >= 0);
    conn.rollback().await.unwrap();
}

#[tokio::test]
async fn stmt_bad_option() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    let err = stmt
        .set_option(StatementOption::Other("bad".into(), "v".into()))
        .await
        .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
}

// ─────────────────────────────────────────────────────────────
// Statement — bulk ingest
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn ingest_roundtrip() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let input = sample_batch();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("ingest_t".into()))
        .await
        .unwrap();
    stmt.bind(input.clone()).await.unwrap();
    stmt.execute_update().await.unwrap();

    stmt.set_sql_query("SELECT * FROM ingest_t").await.unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let got = collect(reader);

    assert_eq!(got.num_rows(), input.num_rows());
    assert_eq!(got.num_columns(), input.num_columns());
    for (i, f) in input.schema().fields().iter().enumerate() {
        assert_eq!(got.schema().field(i).name(), f.name());
    }
    conn.rollback().await.unwrap();
}

#[tokio::test]
async fn ingest_create_already_exists() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("dup".into()))
        .await
        .unwrap();
    stmt.bind(sample_batch()).await.unwrap();
    stmt.execute_update().await.unwrap();

    let mut stmt2 = conn.new_statement().await.unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("dup".into()))
        .await
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Create))
        .await
        .unwrap();
    stmt2.bind(sample_batch()).await.unwrap();
    assert_eq!(
        stmt2.execute_update().await.unwrap_err().status,
        Status::AlreadyExists
    );
    conn.rollback().await.unwrap();
}

#[tokio::test]
async fn ingest_replace() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    // First ingest (Create)
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("rep".into()))
        .await
        .unwrap();
    stmt.bind(sample_batch()).await.unwrap();
    stmt.execute_update().await.unwrap();

    // Replace — should succeed
    let mut stmt2 = conn.new_statement().await.unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("rep".into()))
        .await
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Replace))
        .await
        .unwrap();
    stmt2.bind(sample_batch()).await.unwrap();
    stmt2.execute_update().await.unwrap();

    conn.rollback().await.unwrap();
}

#[tokio::test]
async fn ingest_bind_stream() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
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

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("stream_t".into()))
        .await
        .unwrap();
    stmt.bind_stream(Box::new(reader)).await.unwrap();
    stmt.execute_update().await.unwrap();

    // Verify row count.
    stmt.set_sql_query("SELECT count(*) FROM stream_t")
        .await
        .unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_any().downcast_ref().unwrap();
    assert_eq!(cnt.value(0), 3);
    conn.rollback().await.unwrap();
}

// ─────────────────────────────────────────────────────────────
// New tests — parity with DuckDB
// ─────────────────────────────────────────────────────────────

/// Rows written inside a transaction are visible within it; gone after rollback.
#[tokio::test]
async fn conn_transaction_isolation() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("CREATE TABLE sq_iso_t(x INTEGER)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();
    stmt.set_sql_query("INSERT INTO sq_iso_t VALUES(99)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();

    // Row should be visible within the same transaction.
    stmt.set_sql_query("SELECT x FROM sq_iso_t").await.unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    conn.rollback().await.unwrap();

    // After rollback the table is gone (SQLite DDL IS transactional).
    stmt.set_sql_query("SELECT count(*) FROM sqlite_master WHERE name='sq_iso_t'")
        .await
        .unwrap();
    let (r2, _) = stmt.execute().await.unwrap();
    let got = collect(r2);
    let cnt: &Int64Array = got.column(0).as_any().downcast_ref().unwrap();
    assert_eq!(cnt.value(0), 0);
}

/// Filter get_info to VendorName; verify code column and non-null value.
#[tokio::test]
async fn conn_get_info_vendor_name_code() {
    use arrow_array::cast::AsArray;
    use arrow_array::types::UInt32Type;
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let codes = [InfoCode::VendorName];
    let batch = collect(conn.get_info(Some(&codes)).await.unwrap());
    assert_eq!(batch.num_rows(), 1);
    let code_col = batch.column(0).as_primitive::<UInt32Type>();
    assert_eq!(code_col.value(0), InfoCode::VendorName as u32);
    // Value column must be non-null.
    assert!(!batch.column(1).is_null(0));
}

/// ObjectDepth::Catalogs returns a valid schema with at least one row.
#[tokio::test]
async fn conn_get_objects_catalogs_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
    assert!(batch.num_rows() >= 1);
}

/// ObjectDepth::Schemas returns a valid schema.
#[tokio::test]
async fn conn_get_objects_schemas_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::Schemas, None, None, None, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

/// SELECT returns correct cell values, not just the right row count.
#[tokio::test]
async fn stmt_execute_select_values() {
    use arrow_array::cast::AsArray;
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("SELECT 7 AS n, 'hello' AS s")
        .await
        .unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    let got = collect(r);
    assert_eq!(got.num_rows(), 1);
    let n: &Int64Array = got.column(0).as_primitive();
    assert_eq!(n.value(0), 7);
    let s: &StringArray = got.column(1).as_string();
    assert_eq!(s.value(0), "hello");
}

/// Multi-row VALUES query returns the right count and specific cells.
#[tokio::test]
async fn stmt_execute_multi_row() {
    use arrow_array::cast::AsArray;
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    // SQLite doesn't support VALUES() as a table expression; use UNION ALL instead.
    stmt.set_sql_query(
        "SELECT CAST(10 AS INTEGER) AS col1 \
         UNION ALL SELECT 20 \
         UNION ALL SELECT 30",
    )
    .await
    .unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    let got = collect(r);
    assert_eq!(got.num_rows(), 3);
    let col: &Int64Array = got.column(0).as_primitive();
    assert_eq!(col.value(0), 10);
    assert_eq!(col.value(2), 30);
}

/// Reuse the same statement object with a different SQL query.
#[tokio::test]
async fn stmt_reuse_with_new_query() {
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();

    stmt.set_sql_query("SELECT 1").await.unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    stmt.set_sql_query("SELECT 2, 3").await.unwrap();
    let (r2, _) = stmt.execute().await.unwrap();
    let got = collect(r2);
    assert_eq!(got.num_rows(), 1);
    assert_eq!(got.num_columns(), 2);
}

/// Arrow NULLs in Int64 and Utf8 survive a write->read roundtrip unchanged.
#[tokio::test]
async fn null_roundtrip() {
    use arrow_array::cast::AsArray;
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
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

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("sq_null_t".into()))
        .await
        .unwrap();
    stmt.bind(batch).await.unwrap();
    stmt.execute_update().await.unwrap();

    stmt.set_sql_query("SELECT id, val FROM sq_null_t ORDER BY rowid")
        .await
        .unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let got = collect(reader);

    let ids: &Int64Array = got.column(0).as_primitive();
    assert!(Array::is_null(ids, 1));
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(2), 3);

    let vals: &StringArray = got.column(1).as_string();
    assert!(Array::is_null(vals, 2));
    assert_eq!(vals.value(0), "a");

    conn.rollback().await.unwrap();
}

/// Append mode adds rows on top of an existing table (does not truncate).
#[tokio::test]
async fn ingest_append() {
    use arrow_array::cast::AsArray;
    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    // Create with 3 rows.
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("sq_app_t".into()))
        .await
        .unwrap();
    stmt.bind(sample_batch()).await.unwrap();
    stmt.execute_update().await.unwrap();

    // Append 3 more.
    let mut stmt2 = conn.new_statement().await.unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("sq_app_t".into()))
        .await
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Append))
        .await
        .unwrap();
    stmt2.bind(sample_batch()).await.unwrap();
    stmt2.execute_update().await.unwrap();

    stmt2
        .set_sql_query("SELECT count(*) FROM sq_app_t")
        .await
        .unwrap();
    let (r, _) = stmt2.execute().await.unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), 6);
    conn.rollback().await.unwrap();
}

/// Ingest 1 000 rows and verify the full count via SELECT count(*).
#[tokio::test]
async fn ingest_large_batch() {
    use arrow_array::cast::AsArray;
    let n = 1_000usize;
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let big = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..n as i64))],
    )
    .unwrap();

    let db = memory().await;
    let conn = db.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("sq_big_t".into()))
        .await
        .unwrap();
    stmt.bind(big).await.unwrap();
    stmt.execute_update().await.unwrap();

    stmt.set_sql_query("SELECT count(*) FROM sq_big_t")
        .await
        .unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), n as i64);
    conn.rollback().await.unwrap();
}
