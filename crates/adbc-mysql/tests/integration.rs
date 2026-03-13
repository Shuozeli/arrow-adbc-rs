//! Integration tests for adbc-mysql.
//!
//! Requires a live MySQL server. Set:
//!   ADBC_MYSQL_URI="mysql://adbc_test:adbc_test@localhost:3306/adbc_test"
//!
//! Tests are `#[ignore]` when the env var is absent so `cargo test` passes offline.

use std::sync::Arc;

use arrow_array::{
    cast::AsArray, types::UInt32Type, Array, Float64Array, Int64Array, RecordBatch,
    RecordBatchReader, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;

use adbc::{
    Connection, ConnectionOption, Database, DatabaseOption, Driver, InfoCode, IngestMode,
    ObjectDepth, Statement, StatementOption, Status,
};
use adbc_mysql::MysqlDriver;

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

async fn connect() -> adbc_mysql::MysqlConnection {
    let uri = std::env::var("ADBC_MYSQL_URI").expect("ADBC_MYSQL_URI not set");
    MysqlDriver
        .new_database_with_opts([(DatabaseOption::Uri, uri.into())])
        .await
        .unwrap()
        .new_connection()
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
// Driver / Database
// ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn driver_bad_option() {
    let err = MysqlDriver
        .new_database_with_opts([(DatabaseOption::Other("bad".into()), "v".into())])
        .await
        .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
}

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn db_new_connection() {
    connect().await;
}

// ─────────────────────────────────────────────────────────────
// Connection — transactions
// ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_autocommit_toggle() {
    let conn = connect().await;
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();
    conn.rollback().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(true))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_commit_in_autocommit_fails() {
    let conn = connect().await;
    assert_eq!(
        conn.commit().await.unwrap_err().status,
        Status::InvalidState
    );
}

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_rollback_in_autocommit_fails() {
    let conn = connect().await;
    assert_eq!(
        conn.rollback().await.unwrap_err().status,
        Status::InvalidState
    );
}

/// Unknown connection option → non-Ok error.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_unknown_option() {
    let conn = connect().await;
    let err = conn
        .set_option(ConnectionOption::Other("bad_opt".into(), "v".into()))
        .await
        .unwrap_err();
    assert_ne!(err.status, Status::Ok);
}

/// MySQL DDL (CREATE/DROP) is non-transactional — changes persist even after rollback.
/// We pre-drop the table so the test is idempotent across runs.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_transaction_isolation() {
    let conn = connect().await;
    // Pre-cleanup since MySQL DDL is not rolled back.
    let mut cleanup = conn.new_statement().await.unwrap();
    cleanup
        .set_sql_query("DROP TABLE IF EXISTS mysql_iso_t")
        .await
        .unwrap();
    cleanup.execute_update().await.unwrap();

    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("CREATE TABLE mysql_iso_t(x BIGINT)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();
    stmt.set_sql_query("INSERT INTO mysql_iso_t VALUES(99)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();

    // Row visible within same transaction.
    stmt.set_sql_query("SELECT x FROM mysql_iso_t")
        .await
        .unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    // Rollback doesn't undo CREATE, but DML is rolled back.
    conn.rollback().await.unwrap();

    // Clean up the table (DDL committed already).
    let mut cleanup2 = conn.new_statement().await.unwrap();
    cleanup2
        .set_sql_query("DROP TABLE IF EXISTS mysql_iso_t")
        .await
        .unwrap();
    cleanup2.execute_update().await.unwrap();
}

// ─────────────────────────────────────────────────────────────
// Metadata
// ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_get_table_types() {
    let conn = connect().await;
    let batch = collect(conn.get_table_types().await.unwrap());
    assert!(batch.num_rows() >= 1);
}

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_get_info_all() {
    let conn = connect().await;
    let batch = collect(conn.get_info(None).await.unwrap());
    assert!(batch.num_rows() > 0);
}

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_get_info_filtered() {
    let conn = connect().await;
    let codes = [InfoCode::VendorName, InfoCode::DriverName];
    let batch = collect(conn.get_info(Some(&codes)).await.unwrap());
    assert_eq!(batch.num_rows(), 2);
}

/// Single-code filter: code column must match the requested InfoCode.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_get_info_vendor_name_code() {
    let conn = connect().await;
    let batch = collect(conn.get_info(Some(&[InfoCode::VendorName])).await.unwrap());
    assert_eq!(batch.num_rows(), 1);
    let names = batch.column(0).as_primitive::<UInt32Type>();
    assert_eq!(names.value(0), InfoCode::VendorName as u32);
    assert!(!batch.column(1).is_null(0));
}

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_get_objects() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let conn = connect().await;
    let batch = collect(
        conn.get_objects(ObjectDepth::All, None, None, None, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

/// Catalogs depth still returns the canonical schema.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_get_objects_catalogs_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let conn = connect().await;
    let batch = collect(
        conn.get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
    assert!(batch.num_rows() >= 1);
}

/// Schemas depth still returns the canonical schema.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_get_objects_schemas_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let conn = connect().await;
    let batch = collect(
        conn.get_objects(ObjectDepth::Schemas, None, None, None, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_get_table_schema_missing() {
    let conn = connect().await;
    assert_eq!(
        conn.get_table_schema(None, None, "no_such_table")
            .await
            .unwrap_err()
            .status,
        Status::NotFound,
    );
}

/// Created table → correct field count and names (`a`, `b`).
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn conn_get_table_schema_existing() {
    let conn = connect().await;
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();
    // Pre-drop to be idempotent (MySQL DDL non-transactional).
    let mut cleanup = conn.new_statement().await.unwrap();
    cleanup
        .set_sql_query("DROP TABLE IF EXISTS mysql_schema_t")
        .await
        .unwrap();
    cleanup.execute_update().await.unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("CREATE TABLE mysql_schema_t(a BIGINT, b TEXT)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();

    let schema = conn
        .get_table_schema(None, None, "mysql_schema_t")
        .await
        .unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "a");
    assert_eq!(schema.field(1).name(), "b");

    // DDL already committed, so clean up explicitly.
    let mut drop_stmt = conn.new_statement().await.unwrap();
    drop_stmt
        .set_sql_query("DROP TABLE IF EXISTS mysql_schema_t")
        .await
        .unwrap();
    drop_stmt.execute_update().await.unwrap();
}

// ─────────────────────────────────────────────────────────────
// Queries
// ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn stmt_execute_select() {
    let conn = connect().await;
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("SELECT 42 AS answer").await.unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
}

/// Verify actual cell values returned from a simple SELECT.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn stmt_execute_select_values() {
    let conn = connect().await;
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("SELECT 7 AS n, 'hello' AS s")
        .await
        .unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
    // MySQL returns integers as Int64 through our driver.
    let n_col: &Int64Array = batch.column(0).as_primitive();
    assert_eq!(n_col.value(0), 7);
    let s_col: &StringArray = batch.column(1).as_string();
    assert_eq!(s_col.value(0), "hello");
}

/// Reuse the same statement with a different query.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn stmt_reuse_with_new_query() {
    let conn = connect().await;
    let mut stmt = conn.new_statement().await.unwrap();

    stmt.set_sql_query("SELECT 1 AS n").await.unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    stmt.set_sql_query("SELECT 2 AS n, 3 AS m").await.unwrap();
    let (r2, _) = stmt.execute().await.unwrap();
    let b2 = collect(r2);
    assert_eq!(b2.num_rows(), 1);
    assert_eq!(b2.num_columns(), 2);
}

/// Multi-row SELECT returns correct count and cell values.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn stmt_execute_multi_row() {
    let conn = connect().await;
    let mut stmt = conn.new_statement().await.unwrap();
    // MySQL supports VALUES ROW() syntax but UNION ALL is more portable.
    stmt.set_sql_query("SELECT 10 AS col1 UNION ALL SELECT 20 UNION ALL SELECT 30")
        .await
        .unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    let got = collect(r);
    assert_eq!(got.num_rows(), 3);
    let col: &Int64Array = got.column(0).as_primitive();
    assert_eq!(col.value(0), 10);
    assert_eq!(col.value(2), 30);
}

/// MySQL's `prepare()` is a no-op — it always returns `Ok(())` regardless of
/// whether SQL is set. This test documents that known gap (see feature-matrix.md).
/// When real prepared-statement support is added, this test should be updated.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn stmt_prepare_no_query() {
    let conn = connect().await;
    let mut stmt = conn.new_statement().await.unwrap();
    // prepare() with no SQL is a no-op on MySQL (returns Ok).
    // This documents the known feature gap; other drivers return InvalidState here.
    let _ = stmt.prepare().await; // just must not panic
}

/// Arrow NULLs in Int64 and Utf8 survive a write→read roundtrip (`Replace` for idempotency).
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn null_roundtrip() {
    let conn = connect().await;
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

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
    stmt.set_option(StatementOption::TargetTable("mysql_null_t".into()))
        .await
        .unwrap();
    stmt.set_option(StatementOption::IngestMode(IngestMode::Replace))
        .await
        .unwrap();
    stmt.bind(batch).await.unwrap();
    stmt.execute_update().await.unwrap();

    let mut qstmt = conn.new_statement().await.unwrap();
    qstmt
        .set_sql_query("SELECT id, val FROM mysql_null_t ORDER BY id IS NULL, id")
        .await
        .unwrap();
    let (reader, _) = qstmt.execute().await.unwrap();
    let got = collect(reader);
    assert_eq!(got.num_rows(), 3);

    // id column: row with NULL id comes last (ORDER BY id IS NULL, id)
    let ids: &Int64Array = got.column(0).as_primitive();
    assert!(!Array::is_null(ids, 0)); // id=1
    assert!(!Array::is_null(ids, 1)); // id=3
    assert!(Array::is_null(ids, 2)); // NULL id

    // val column: non-null rows first
    let vals: &StringArray = got.column(1).as_string();
    // At least one NULL in val column.
    let has_null_val = (0..got.num_rows()).any(|i| Array::is_null(vals, i));
    assert!(has_null_val);

    conn.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn stmt_execute_update() {
    let conn = connect().await;
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("CREATE TABLE mysql_upd_test(x BIGINT)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();
    stmt.set_sql_query("INSERT INTO mysql_upd_test VALUES(1)")
        .await
        .unwrap();
    let n = stmt.execute_update().await.unwrap();
    assert!(n >= 0);
    conn.rollback().await.unwrap();
    // Clean up.
    let mut stmt2 = conn.new_statement().await.unwrap();
    stmt2
        .set_sql_query("DROP TABLE IF EXISTS mysql_upd_test")
        .await
        .unwrap();
    stmt2.execute_update().await.unwrap();
}

// ─────────────────────────────────────────────────────────────
// Bulk ingest
// ─────────────────────────────────────────────────────────────

/// MySQL DDL is non-transactional; using Replace mode makes the test idempotent.
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn ingest_roundtrip() {
    let conn = connect().await;
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let input = sample_batch();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("mysql_ingest_t".into()))
        .await
        .unwrap();
    stmt.set_option(StatementOption::IngestMode(IngestMode::Replace))
        .await
        .unwrap();
    stmt.bind(input.clone()).await.unwrap();
    stmt.execute_update().await.unwrap();

    let mut qstmt = conn.new_statement().await.unwrap();
    qstmt
        .set_sql_query("SELECT * FROM mysql_ingest_t")
        .await
        .unwrap();
    let (reader, _) = qstmt.execute().await.unwrap();
    let got = collect(reader);
    assert_eq!(got.num_rows(), input.num_rows());
    conn.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn ingest_replace() {
    let conn = connect().await;
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    // First ingest with Replace (also serves as Create, naturally idempotent).
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("mysql_rep_t".into()))
        .await
        .unwrap();
    stmt.set_option(StatementOption::IngestMode(IngestMode::Replace))
        .await
        .unwrap();
    stmt.bind(sample_batch()).await.unwrap();
    stmt.execute_update().await.unwrap();

    // Second Replace — should succeed.
    let mut stmt2 = conn.new_statement().await.unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("mysql_rep_t".into()))
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

/// Append adds rows without truncating.
/// Uses Replace for first ingest so the test is idempotent (MySQL DDL non-transactional).
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn ingest_append() {
    let conn = connect().await;
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    // Create/replace the table with 3 rows.
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("mysql_app_t".into()))
        .await
        .unwrap();
    stmt.set_option(StatementOption::IngestMode(IngestMode::Replace))
        .await
        .unwrap();
    stmt.bind(sample_batch()).await.unwrap();
    stmt.execute_update().await.unwrap();

    // Append 3 more rows.
    let mut stmt2 = conn.new_statement().await.unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("mysql_app_t".into()))
        .await
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Append))
        .await
        .unwrap();
    stmt2.bind(sample_batch()).await.unwrap();
    stmt2.execute_update().await.unwrap();

    let mut qstmt = conn.new_statement().await.unwrap();
    qstmt
        .set_sql_query("SELECT count(*) FROM mysql_app_t")
        .await
        .unwrap();
    let (r, _) = qstmt.execute().await.unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), 6);
    conn.rollback().await.unwrap();
}

/// bind_stream ingest via RecordBatchReader.
/// Uses Replace for first ingest so the test is idempotent (MySQL DDL non-transactional).
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn ingest_bind_stream() {
    let conn = connect().await;
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    struct OneBatch {
        batch: Option<RecordBatch>,
        schema: Arc<Schema>,
    }
    impl Iterator for OneBatch {
        type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
        fn next(&mut self) -> Option<Self::Item> {
            Ok(self.batch.take()).transpose()
        }
    }
    impl RecordBatchReader for OneBatch {
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }
    }

    let batch = sample_batch();
    let schema = batch.schema();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("mysql_stream_t".into()))
        .await
        .unwrap();
    stmt.set_option(StatementOption::IngestMode(IngestMode::Replace))
        .await
        .unwrap();
    stmt.bind_stream(Box::new(OneBatch {
        batch: Some(batch),
        schema,
    }))
    .await
    .unwrap();
    stmt.execute_update().await.unwrap();

    let mut qstmt = conn.new_statement().await.unwrap();
    qstmt
        .set_sql_query("SELECT count(*) FROM mysql_stream_t")
        .await
        .unwrap();
    let (r, _) = qstmt.execute().await.unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), 3);
    conn.rollback().await.unwrap();
}

/// Ingest 1 000 rows and verify the full count.
/// Uses Replace for first ingest so the test is idempotent (MySQL DDL non-transactional).
#[tokio::test]
#[ignore = "requires ADBC_MYSQL_URI"]
async fn ingest_large_batch() {
    let n = 1_000usize;
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let big = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..n as i64))],
    )
    .unwrap();

    let conn = connect().await;
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("mysql_big_t".into()))
        .await
        .unwrap();
    stmt.set_option(StatementOption::IngestMode(IngestMode::Replace))
        .await
        .unwrap();
    stmt.bind(big).await.unwrap();
    stmt.execute_update().await.unwrap();

    let mut qstmt = conn.new_statement().await.unwrap();
    qstmt
        .set_sql_query("SELECT count(*) FROM mysql_big_t")
        .await
        .unwrap();
    let (r, _) = qstmt.execute().await.unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), n as i64);
    conn.rollback().await.unwrap();
}
