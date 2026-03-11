//! Integration tests for adbc-flightsql.
//!
//! These tests require a live Arrow FlightSQL server. Set the following
//! environment variables to run them:
//!
//! - `ADBC_FLIGHTSQL_URI` — e.g. `grpc://localhost:32010`
//! - `ADBC_FLIGHTSQL_USER` — optional username
//! - `ADBC_FLIGHTSQL_PASS` — optional password
//!
//! All tests are marked `#[ignore]` so that `cargo test` skips them by default.
//! Run with: `cargo test -p adbc-flightsql -- --ignored`

use std::sync::Arc;

use arrow_array::RecordBatchReader;
use arrow_schema::{DataType, Field, Schema};

use adbc::{
    Connection, Database, DatabaseOption, Driver, InfoCode, IngestMode, ObjectDepth, OptionValue,
    Statement, StatementOption, Status,
};
use adbc_flightsql::FlightSqlDriver;

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

fn test_db() -> Option<adbc_flightsql::FlightSqlDatabase> {
    let uri = std::env::var("ADBC_FLIGHTSQL_URI").ok()?;
    let user = std::env::var("ADBC_FLIGHTSQL_USER").ok();
    let pass = std::env::var("ADBC_FLIGHTSQL_PASS").ok();

    let mut opts: Vec<(DatabaseOption, OptionValue)> = vec![(DatabaseOption::Uri, uri.into())];
    if let Some(u) = user {
        opts.push((DatabaseOption::Username, u.into()));
    }
    if let Some(p) = pass {
        opts.push((DatabaseOption::Password, p.into()));
    }

    FlightSqlDriver.new_database_with_opts(opts).ok()
}

fn connect() -> adbc_flightsql::FlightSqlDatabase {
    test_db().expect("ADBC_FLIGHTSQL_URI must be set")
}

/// Collect all batches and return total row count and first batch column count.
fn collect_counts(reader: Box<dyn RecordBatchReader + Send>) -> (usize, usize) {
    let mut rows = 0;
    let mut cols = 0;
    for batch in reader {
        let b = batch.unwrap();
        rows += b.num_rows();
        cols = b.num_columns();
    }
    (rows, cols)
}

// ─────────────────────────────────────────────────────────────
// Driver / Database / Connection
// ─────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn db_new_connection() {
    connect().new_connection().unwrap();
}

// ─────────────────────────────────────────────────────────────
// Metadata
// ─────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_select_rows() {
    let db = connect();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 1 AS one").unwrap();
    let (reader, _) = stmt.execute().expect("execute should succeed");
    let (rows, _cols) = collect_counts(reader);
    assert_eq!(rows, 1);
}

#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_get_table_types() {
    let db = connect();
    let conn = db.new_connection().unwrap();
    let (rows, _) = collect_counts(conn.get_table_types().unwrap());
    assert!(rows >= 1);
}

#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_get_info() {
    let db = connect();
    let conn = db.new_connection().unwrap();
    let (rows, _) = collect_counts(conn.get_info(None).unwrap());
    assert!(rows > 0);
}

/// Filter get_info to a single code; verify row count.
#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_get_info_vendor_name_code() {
    let db = connect();
    let conn = db.new_connection().unwrap();
    let codes = [InfoCode::VendorName];
    let (rows, _) = collect_counts(conn.get_info(Some(&codes)).unwrap());
    assert_eq!(rows, 1);
}

/// get_objects with All depth returns results.
#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_get_objects() {
    let db = connect();
    let conn = db.new_connection().unwrap();
    let (rows, _) = collect_counts(
        conn.get_objects(ObjectDepth::All, None, None, None, None, None)
            .unwrap(),
    );
    let _ = rows; // may be 0 if no tables
}

/// get_table_schema on a missing table → NotFound.
#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_get_table_schema_missing() {
    let db = connect();
    let conn = db.new_connection().unwrap();
    let err = conn
        .get_table_schema(None, None, "no_such_table_xyz")
        .unwrap_err();
    assert_eq!(err.status, Status::NotFound);
}

// ─────────────────────────────────────────────────────────────
// Queries
// ─────────────────────────────────────────────────────────────

/// prepare() with no SQL set → InvalidState.
#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_prepare_no_query() {
    let db = connect();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    assert_eq!(stmt.prepare().unwrap_err().status, Status::InvalidState);
}

/// prepare() + execute() — FlightSQL supports real server-side prepare.
#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_prepare_and_execute() {
    let db = connect();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 42 AS n").unwrap();
    stmt.prepare().unwrap();
    let (reader, _) = stmt.execute().expect("execute should succeed");
    let (rows, _) = collect_counts(reader);
    assert_eq!(rows, 1);
}

// ─────────────────────────────────────────────────────────────
// Known NotImplemented gaps — test that the error is correct
// ─────────────────────────────────────────────────────────────

/// Bulk ingest is not supported by FlightSQL — must return NotImplemented.
#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_ingest_not_implemented() {
    use arrow_array::Int64Array;
    use arrow_array::RecordBatch as AB;
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let batch = AB::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))]).unwrap();

    let db = connect();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(StatementOption::TargetTable("t".into()))
        .unwrap();
    stmt.set_option(StatementOption::IngestMode(IngestMode::Create))
        .unwrap();
    stmt.bind(batch).unwrap();

    let err = stmt.execute_update().unwrap_err();
    assert_eq!(err.status, Status::NotImplemented);
}

/// get_table_schema returns NotImplemented (documented gap).
#[test]
#[ignore = "requires ADBC_FLIGHTSQL_URI environment variable"]
fn flightsql_get_table_schema_not_impl() {
    let db = connect();
    // Note: get_table_schema is stubbed as NotImplemented in this driver.
    let conn = db.new_connection().unwrap();
    let err = conn.get_table_schema(None, None, "any_table").unwrap_err();
    assert_eq!(err.status, Status::NotImplemented);
}
