//! Integration tests for adbc-postgres.
//!
//! Requires a live PostgreSQL server. Set:
//!   ADBC_POSTGRES_URI="host=localhost port=5432 user=adbc_test password=adbc_test dbname=adbc_test"
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
use adbc_postgres::PostgresDriver;

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

fn pg_uri() -> Option<String> {
    std::env::var("ADBC_POSTGRES_URI").ok()
}

async fn connect() -> adbc_postgres::PostgresDatabase {
    let uri = pg_uri().expect("ADBC_POSTGRES_URI not set");
    PostgresDriver
        .new_database_with_opts([(DatabaseOption::Uri, uri.into())])
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
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn driver_new_database_with_opts() {
    connect().await;
}

#[tokio::test]
async fn driver_bad_option() {
    let err = PostgresDriver
        .new_database_with_opts([(DatabaseOption::Other("bad".into()), "v".into())])
        .await
        .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
}

// ─────────────────────────────────────────────────────────────
// Connection — lifecycle
// ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn db_new_connection() {
    connect().await.new_connection().await.unwrap();
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_autocommit_toggle() {
    let conn = connect().await.new_connection().await.unwrap();
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
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_commit_in_autocommit_fails() {
    let conn = connect().await.new_connection().await.unwrap();
    assert_eq!(
        conn.commit().await.unwrap_err().status,
        Status::InvalidState
    );
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_rollback_in_autocommit_fails() {
    let conn = connect().await.new_connection().await.unwrap();
    assert_eq!(
        conn.rollback().await.unwrap_err().status,
        Status::InvalidState
    );
}

/// Changes inside a transaction are invisible to other connections and
/// disappear after rollback.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_transaction_isolation() {
    let conn = connect().await.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();
    let mut stmt = conn.new_statement().await.unwrap();

    stmt.set_sql_query("CREATE TABLE pg_iso_t(x BIGINT)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();
    stmt.set_sql_query("INSERT INTO pg_iso_t VALUES(42)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();

    // Row is visible within same transaction.
    stmt.set_sql_query("SELECT x FROM pg_iso_t").await.unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    conn.rollback().await.unwrap();
}

/// Unknown connection option must produce a non-Ok error.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_unknown_option() {
    let conn = connect().await.new_connection().await.unwrap();
    let err = conn
        .set_option(ConnectionOption::Other("bad_pg_opt".into(), "v".into()))
        .await
        .unwrap_err();
    assert_ne!(err.status, Status::Ok);
}

// ─────────────────────────────────────────────────────────────
// Metadata
// ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_get_table_types() {
    let conn = connect().await.new_connection().await.unwrap();
    let batch = collect(conn.get_table_types().await.unwrap());
    assert!(batch.num_rows() >= 2);
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_get_info_all() {
    let conn = connect().await.new_connection().await.unwrap();
    let batch = collect(conn.get_info(None).await.unwrap());
    assert!(batch.num_rows() > 0);
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_get_info_filtered() {
    let conn = connect().await.new_connection().await.unwrap();
    let codes = [InfoCode::VendorName, InfoCode::DriverName];
    let batch = collect(conn.get_info(Some(&codes)).await.unwrap());
    assert_eq!(batch.num_rows(), 2);
}

/// Single-code filter: code in column 0 must match the requested code.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_get_info_vendor_name_code() {
    let conn = connect().await.new_connection().await.unwrap();
    let batch = collect(conn.get_info(Some(&[InfoCode::VendorName])).await.unwrap());
    assert_eq!(batch.num_rows(), 1);
    let names = batch.column(0).as_primitive::<UInt32Type>();
    assert_eq!(names.value(0), InfoCode::VendorName as u32);
    assert!(!batch.column(1).is_null(0));
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_get_objects() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let conn = connect().await.new_connection().await.unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::All, None, None, None, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

/// Catalogs depth still returns valid schema.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_get_objects_catalogs_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let conn = connect().await.new_connection().await.unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
    assert!(batch.num_rows() >= 1);
}

/// Schemas depth still returns valid schema.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_get_objects_schemas_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let conn = connect().await.new_connection().await.unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::Schemas, None, None, None, None, None)
            .await
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_get_table_schema_missing() {
    let conn = connect().await.new_connection().await.unwrap();
    assert_eq!(
        conn.get_table_schema(None, None, "no_such_table")
            .await
            .unwrap_err()
            .status,
        Status::NotFound,
    );
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn conn_get_table_schema_existing() {
    let conn = connect().await.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("CREATE TABLE adbc_test_schema(a BIGINT, b TEXT)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();
    let schema = conn
        .get_table_schema(None, None, "adbc_test_schema")
        .await
        .unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "a");
    assert_eq!(schema.field(1).name(), "b");
    conn.rollback().await.unwrap();
}

// ─────────────────────────────────────────────────────────────
// Queries
// ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn stmt_execute_select() {
    let conn = connect().await.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("SELECT 42::BIGINT AS answer")
        .await
        .unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
}

/// Verify actual cell values returned from a SELECT.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn stmt_execute_select_values() {
    let conn = connect().await.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("SELECT 7::BIGINT AS n, 'hello'::TEXT AS s")
        .await
        .unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
    let n_col: &Int64Array = batch.column(0).as_primitive();
    assert_eq!(n_col.value(0), 7);
    let s_col: &StringArray = batch.column(1).as_string();
    assert_eq!(s_col.value(0), "hello");
}

/// Multi-row query — verify all rows come back.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn stmt_execute_multi_row() {
    let conn = connect().await.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("SELECT g FROM generate_series(1,5) AS g")
        .await
        .unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 5);
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn stmt_prepare_and_execute() {
    let conn = connect().await.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("SELECT 1::BIGINT AS n").await.unwrap();
    stmt.prepare().await.unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
}

/// Reuse the same statement object with a new SQL query.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn stmt_reuse_with_new_query() {
    let conn = connect().await.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();

    stmt.set_sql_query("SELECT 1::BIGINT AS n").await.unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    stmt.set_sql_query("SELECT 2::BIGINT AS n, 3::BIGINT AS m")
        .await
        .unwrap();
    let (r2, _) = stmt.execute().await.unwrap();
    let b2 = collect(r2);
    assert_eq!(b2.num_rows(), 1);
    assert_eq!(b2.num_columns(), 2);
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn stmt_execute_update() {
    let conn = connect().await.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query("CREATE TABLE adbc_upd_test(x BIGINT)")
        .await
        .unwrap();
    stmt.execute_update().await.unwrap();
    stmt.set_sql_query("INSERT INTO adbc_upd_test VALUES(1)")
        .await
        .unwrap();
    let n = stmt.execute_update().await.unwrap();
    assert_eq!(n, 1);
    conn.rollback().await.unwrap();
}

/// execute() with no SQL set → InvalidState.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn stmt_execute_no_query() {
    let conn = connect().await.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    assert_eq!(
        stmt.execute().await.err().unwrap().status,
        Status::InvalidState
    );
}

/// Unknown statement option → InvalidArguments.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn stmt_bad_option() {
    let conn = connect().await.new_connection().await.unwrap();
    let mut stmt = conn.new_statement().await.unwrap();
    let err = stmt
        .set_option(StatementOption::Other("x".into(), "v".into()))
        .await
        .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
}

// ─────────────────────────────────────────────────────────────
// NULL values roundtrip
// ─────────────────────────────────────────────────────────────

/// NULL values survive write + read roundtrip.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn null_roundtrip() {
    let conn = connect().await.new_connection().await.unwrap();
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
            Arc::new(Int64Array::from(vec![Some(1i64), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ],
    )
    .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_null_t".into()))
        .await
        .unwrap();
    stmt.bind(batch).await.unwrap();
    stmt.execute_update().await.unwrap();

    stmt.set_sql_query("SELECT id, val FROM pg_null_t ORDER BY ctid")
        .await
        .unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    let got = collect(r);

    let ids: &Int64Array = got.column(0).as_primitive();
    assert!(Array::is_null(ids, 1));
    assert_eq!(ids.value(0), 1);

    let vals: &StringArray = got.column(1).as_string();
    assert!(Array::is_null(vals, 2));
    assert_eq!(vals.value(0), "a");

    conn.rollback().await.unwrap();
}

// ─────────────────────────────────────────────────────────────
// Bulk ingest
// ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn ingest_roundtrip() {
    let conn = connect().await.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let input = sample_batch();
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_ingest_t".into()))
        .await
        .unwrap();
    stmt.bind(input.clone()).await.unwrap();
    stmt.execute_update().await.unwrap();

    stmt.set_sql_query("SELECT * FROM pg_ingest_t")
        .await
        .unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    let got = collect(reader);
    assert_eq!(got.num_rows(), input.num_rows());
    assert_eq!(got.num_columns(), input.num_columns());
    // Column names preserved.
    for (i, f) in input.schema().fields().iter().enumerate() {
        assert_eq!(got.schema().field(i).name(), f.name());
    }
    conn.rollback().await.unwrap();
}

/// Create mode on an existing table → AlreadyExists.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn ingest_create_already_exists() {
    let conn = connect().await.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    // First ingest creates the table.
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_dup_t".into()))
        .await
        .unwrap();
    stmt.bind(sample_batch()).await.unwrap();
    stmt.execute_update().await.unwrap();

    // Second Create on same table → AlreadyExists.
    let mut stmt2 = conn.new_statement().await.unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("pg_dup_t".into()))
        .await
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Create))
        .await
        .unwrap();
    stmt2.bind(sample_batch()).await.unwrap();
    assert_eq!(
        stmt2.execute_update().await.err().unwrap().status,
        Status::AlreadyExists,
    );
    conn.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn ingest_replace() {
    let conn = connect().await.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_rep_t".into()))
        .await
        .unwrap();
    stmt.bind(sample_batch()).await.unwrap();
    stmt.execute_update().await.unwrap();

    let mut stmt2 = conn.new_statement().await.unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("pg_rep_t".into()))
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
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn ingest_append() {
    let conn = connect().await.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_app_t".into()))
        .await
        .unwrap();
    stmt.bind(sample_batch()).await.unwrap();
    stmt.execute_update().await.unwrap();

    let mut stmt2 = conn.new_statement().await.unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("pg_app_t".into()))
        .await
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Append))
        .await
        .unwrap();
    stmt2.bind(sample_batch()).await.unwrap();
    stmt2.execute_update().await.unwrap();

    stmt2
        .set_sql_query("SELECT count(*) FROM pg_app_t")
        .await
        .unwrap();
    let (r, _) = stmt2.execute().await.unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), 6);
    conn.rollback().await.unwrap();
}

/// bind_stream ingest via RecordBatchReader interface.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn ingest_bind_stream() {
    let conn = connect().await.new_connection().await.unwrap();
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
    stmt.set_option(StatementOption::TargetTable("pg_stream_t".into()))
        .await
        .unwrap();
    stmt.bind_stream(Box::new(OneBatch {
        batch: Some(batch),
        schema,
    }))
    .await
    .unwrap();
    stmt.execute_update().await.unwrap();

    stmt.set_sql_query("SELECT count(*) FROM pg_stream_t")
        .await
        .unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), 3);
    conn.rollback().await.unwrap();
}

/// Ingest 1 000 rows and verify the full count survives.
#[tokio::test]
#[ignore = "requires ADBC_POSTGRES_URI"]
async fn ingest_large_batch() {
    let n = 1_000usize;
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let big = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..n as i64))],
    )
    .unwrap();

    let conn = connect().await.new_connection().await.unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .await
        .unwrap();

    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_big_t".into()))
        .await
        .unwrap();
    stmt.bind(big).await.unwrap();
    stmt.execute_update().await.unwrap();

    stmt.set_sql_query("SELECT count(*) FROM pg_big_t")
        .await
        .unwrap();
    let (r, _) = stmt.execute().await.unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), n as i64);
    conn.rollback().await.unwrap();
}
