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

fn connect() -> adbc_postgres::PostgresDatabase {
    let uri = pg_uri().expect("ADBC_POSTGRES_URI not set");
    PostgresDriver
        .new_database_with_opts([(DatabaseOption::Uri, uri.into())])
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

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn driver_new_database_with_opts() {
    connect();
}

#[test]
fn driver_bad_option() {
    let err = PostgresDriver
        .new_database_with_opts([(DatabaseOption::Other("bad".into()), "v".into())])
        .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
}

// ─────────────────────────────────────────────────────────────
// Connection — lifecycle
// ─────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn db_new_connection() {
    connect().new_connection().unwrap();
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_autocommit_toggle() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();
    conn.commit().unwrap();
    conn.rollback().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(true)).unwrap();
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_commit_in_autocommit_fails() {
    let mut conn = connect().new_connection().unwrap();
    assert_eq!(conn.commit().unwrap_err().status, Status::InvalidState);
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_rollback_in_autocommit_fails() {
    let mut conn = connect().new_connection().unwrap();
    assert_eq!(conn.rollback().unwrap_err().status, Status::InvalidState);
}

/// Changes inside a transaction are invisible to other connections and
/// disappear after rollback.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_transaction_isolation() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("CREATE TABLE pg_iso_t(x BIGINT)")
        .unwrap();
    stmt.execute_update().unwrap();
    stmt.set_sql_query("INSERT INTO pg_iso_t VALUES(42)")
        .unwrap();
    stmt.execute_update().unwrap();

    // Row is visible within same transaction.
    stmt.set_sql_query("SELECT x FROM pg_iso_t").unwrap();
    let (r, _) = stmt.execute().unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    conn.rollback().unwrap();
}

/// Unknown connection option must produce a non-Ok error.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_unknown_option() {
    let mut conn = connect().new_connection().unwrap();
    let err = conn
        .set_option(ConnectionOption::Other("bad_pg_opt".into(), "v".into()))
        .unwrap_err();
    assert_ne!(err.status, Status::Ok);
}

// ─────────────────────────────────────────────────────────────
// Metadata
// ─────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_get_table_types() {
    let conn = connect().new_connection().unwrap();
    let batch = collect(conn.get_table_types().unwrap());
    assert!(batch.num_rows() >= 2);
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_get_info_all() {
    let conn = connect().new_connection().unwrap();
    let batch = collect(conn.get_info(None).unwrap());
    assert!(batch.num_rows() > 0);
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_get_info_filtered() {
    let conn = connect().new_connection().unwrap();
    let codes = [InfoCode::VendorName, InfoCode::DriverName];
    let batch = collect(conn.get_info(Some(&codes)).unwrap());
    assert_eq!(batch.num_rows(), 2);
}

/// Single-code filter: code in column 0 must match the requested code.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_get_info_vendor_name_code() {
    let conn = connect().new_connection().unwrap();
    let batch = collect(conn.get_info(Some(&[InfoCode::VendorName])).unwrap());
    assert_eq!(batch.num_rows(), 1);
    let names = batch.column(0).as_primitive::<UInt32Type>();
    assert_eq!(names.value(0), InfoCode::VendorName as u32);
    assert!(!batch.column(1).is_null(0));
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_get_objects() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let conn = connect().new_connection().unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::All, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

/// Catalogs depth still returns valid schema.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_get_objects_catalogs_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let conn = connect().new_connection().unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
    assert!(batch.num_rows() >= 1);
}

/// Schemas depth still returns valid schema.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_get_objects_schemas_depth() {
    use adbc::schema::GET_OBJECTS_SCHEMA;
    let conn = connect().new_connection().unwrap();
    let batch = collect(
        conn.get_objects(ObjectDepth::Schemas, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(batch.schema().as_ref(), GET_OBJECTS_SCHEMA.as_ref());
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_get_table_schema_missing() {
    let conn = connect().new_connection().unwrap();
    assert_eq!(
        conn.get_table_schema(None, None, "no_such_table")
            .unwrap_err()
            .status,
        Status::NotFound,
    );
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn conn_get_table_schema_existing() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("CREATE TABLE adbc_test_schema(a BIGINT, b TEXT)")
        .unwrap();
    stmt.execute_update().unwrap();
    let schema = conn
        .get_table_schema(None, None, "adbc_test_schema")
        .unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "a");
    assert_eq!(schema.field(1).name(), "b");
    conn.rollback().unwrap();
}

// ─────────────────────────────────────────────────────────────
// Queries
// ─────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn stmt_execute_select() {
    let mut conn = connect().new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 42::BIGINT AS answer").unwrap();
    let (reader, _) = stmt.execute().unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
}

/// Verify actual cell values returned from a SELECT.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn stmt_execute_select_values() {
    let mut conn = connect().new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 7::BIGINT AS n, 'hello'::TEXT AS s")
        .unwrap();
    let (reader, _) = stmt.execute().unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
    let n_col: &Int64Array = batch.column(0).as_primitive();
    assert_eq!(n_col.value(0), 7);
    let s_col: &StringArray = batch.column(1).as_string();
    assert_eq!(s_col.value(0), "hello");
}

/// Multi-row query — verify all rows come back.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn stmt_execute_multi_row() {
    let mut conn = connect().new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT g FROM generate_series(1,5) AS g")
        .unwrap();
    let (reader, _) = stmt.execute().unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 5);
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn stmt_prepare_and_execute() {
    let mut conn = connect().new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 1::BIGINT AS n").unwrap();
    stmt.prepare().unwrap();
    let (reader, _) = stmt.execute().unwrap();
    let batch = collect(reader);
    assert_eq!(batch.num_rows(), 1);
}

/// Reuse the same statement object with a new SQL query.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn stmt_reuse_with_new_query() {
    let mut conn = connect().new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT 1::BIGINT AS n").unwrap();
    let (r, _) = stmt.execute().unwrap();
    assert_eq!(collect(r).num_rows(), 1);

    stmt.set_sql_query("SELECT 2::BIGINT AS n, 3::BIGINT AS m")
        .unwrap();
    let (r2, _) = stmt.execute().unwrap();
    let b2 = collect(r2);
    assert_eq!(b2.num_rows(), 1);
    assert_eq!(b2.num_columns(), 2);
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn stmt_execute_update() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("CREATE TABLE adbc_upd_test(x BIGINT)")
        .unwrap();
    stmt.execute_update().unwrap();
    stmt.set_sql_query("INSERT INTO adbc_upd_test VALUES(1)")
        .unwrap();
    let n = stmt.execute_update().unwrap();
    assert_eq!(n, 1);
    conn.rollback().unwrap();
}

/// execute() with no SQL set → InvalidState.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn stmt_execute_no_query() {
    let mut conn = connect().new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    assert_eq!(stmt.execute().err().unwrap().status, Status::InvalidState);
}

/// Unknown statement option → InvalidArguments.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn stmt_bad_option() {
    let mut conn = connect().new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    let err = stmt
        .set_option(StatementOption::Other("x".into(), "v".into()))
        .unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
}

// ─────────────────────────────────────────────────────────────
// NULL values roundtrip
// ─────────────────────────────────────────────────────────────

/// NULL values survive write + read roundtrip.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn null_roundtrip() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
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

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_null_t".into()))
        .unwrap();
    stmt.bind(batch).unwrap();
    stmt.execute_update().unwrap();

    stmt.set_sql_query("SELECT id, val FROM pg_null_t ORDER BY ctid")
        .unwrap();
    let (r, _) = stmt.execute().unwrap();
    let got = collect(r);

    let ids: &Int64Array = got.column(0).as_primitive();
    assert!(Array::is_null(ids, 1));
    assert_eq!(ids.value(0), 1);

    let vals: &StringArray = got.column(1).as_string();
    assert!(Array::is_null(vals, 2));
    assert_eq!(vals.value(0), "a");

    conn.rollback().unwrap();
}

// ─────────────────────────────────────────────────────────────
// Bulk ingest
// ─────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn ingest_roundtrip() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    let input = sample_batch();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_ingest_t".into()))
        .unwrap();
    stmt.bind(input.clone()).unwrap();
    stmt.execute_update().unwrap();

    stmt.set_sql_query("SELECT * FROM pg_ingest_t").unwrap();
    let (reader, _) = stmt.execute().unwrap();
    let got = collect(reader);
    assert_eq!(got.num_rows(), input.num_rows());
    assert_eq!(got.num_columns(), input.num_columns());
    // Column names preserved.
    for (i, f) in input.schema().fields().iter().enumerate() {
        assert_eq!(got.schema().field(i).name(), f.name());
    }
    conn.rollback().unwrap();
}

/// Create mode on an existing table → AlreadyExists.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn ingest_create_already_exists() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    // First ingest creates the table.
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_dup_t".into()))
        .unwrap();
    stmt.bind(sample_batch()).unwrap();
    stmt.execute_update().unwrap();

    // Second Create on same table → AlreadyExists.
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("pg_dup_t".into()))
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Create))
        .unwrap();
    stmt2.bind(sample_batch()).unwrap();
    assert_eq!(
        stmt2.execute_update().err().unwrap().status,
        Status::AlreadyExists,
    );
    conn.rollback().unwrap();
}

#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn ingest_replace() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_rep_t".into()))
        .unwrap();
    stmt.bind(sample_batch()).unwrap();
    stmt.execute_update().unwrap();

    let mut stmt2 = conn.new_statement().unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("pg_rep_t".into()))
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Replace))
        .unwrap();
    stmt2.bind(sample_batch()).unwrap();
    stmt2.execute_update().unwrap();
    conn.rollback().unwrap();
}

/// Append adds rows without truncating.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn ingest_append() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_app_t".into()))
        .unwrap();
    stmt.bind(sample_batch()).unwrap();
    stmt.execute_update().unwrap();

    let mut stmt2 = conn.new_statement().unwrap();
    stmt2
        .set_option(StatementOption::TargetTable("pg_app_t".into()))
        .unwrap();
    stmt2
        .set_option(StatementOption::IngestMode(IngestMode::Append))
        .unwrap();
    stmt2.bind(sample_batch()).unwrap();
    stmt2.execute_update().unwrap();

    stmt2
        .set_sql_query("SELECT count(*) FROM pg_app_t")
        .unwrap();
    let (r, _) = stmt2.execute().unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), 6);
    conn.rollback().unwrap();
}

/// bind_stream ingest via RecordBatchReader interface.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn ingest_bind_stream() {
    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
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

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_stream_t".into()))
        .unwrap();
    stmt.bind_stream(Box::new(OneBatch {
        batch: Some(batch),
        schema,
    }))
    .unwrap();
    stmt.execute_update().unwrap();

    stmt.set_sql_query("SELECT count(*) FROM pg_stream_t")
        .unwrap();
    let (r, _) = stmt.execute().unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), 3);
    conn.rollback().unwrap();
}

/// Ingest 1 000 rows and verify the full count survives.
#[test]
#[ignore = "requires ADBC_POSTGRES_URI"]
fn ingest_large_batch() {
    let n = 1_000usize;
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let big = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..n as i64))],
    )
    .unwrap();

    let mut conn = connect().new_connection().unwrap();
    conn.set_option(ConnectionOption::AutoCommit(false))
        .unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("pg_big_t".into()))
        .unwrap();
    stmt.bind(big).unwrap();
    stmt.execute_update().unwrap();

    stmt.set_sql_query("SELECT count(*) FROM pg_big_t").unwrap();
    let (r, _) = stmt.execute().unwrap();
    let got = collect(r);
    let cnt: &Int64Array = got.column(0).as_primitive();
    assert_eq!(cnt.value(0), n as i64);
    conn.rollback().unwrap();
}
