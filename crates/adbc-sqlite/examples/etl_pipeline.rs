//! ETL pipeline example.
//!
//! Demonstrates a realistic Extract-Transform-Load workflow:
//!   1. Create and populate a raw "events" table
//!   2. Query (extract) the raw data
//!   3. Transform: compute derived columns in Arrow
//!   4. Load the transformed data into a new "daily_summary" table
//!   5. Query the summary for verification
//!
//! ```bash
//! cargo run -p adbc-sqlite --example etl_pipeline
//! ```

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Float64Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;

use adbc::{
    Connection, ConnectionOption, Database, DatabaseOption, Driver, IngestMode, Statement,
    StatementOption,
};
use adbc_sqlite::SqliteDriver;

#[tokio::main]
async fn main() -> adbc::Result<()> {
    let db = SqliteDriver
        .new_database_with_opts([(DatabaseOption::Uri, ":memory:".into())])
        .await?;
    let conn = db.new_connection().await?;
    conn.set_option(ConnectionOption::AutoCommit(false)).await?;

    // ── STEP 1: Create raw events table ──────────────────────
    println!("Step 1: Creating raw events table...");
    let mut stmt = conn.new_statement().await?;
    stmt.set_sql_query(
        "CREATE TABLE events (\
            event_date TEXT NOT NULL, \
            user_id    INTEGER NOT NULL, \
            action     TEXT NOT NULL, \
            duration_s REAL NOT NULL\
        )",
    )
    .await?;
    stmt.execute_update().await?;

    // Simulate a week of user activity data.
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_date", DataType::Utf8, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("action", DataType::Utf8, false),
        Field::new("duration_s", DataType::Float64, false),
    ]));

    let raw = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "2025-01-06",
                "2025-01-06",
                "2025-01-06",
                "2025-01-06",
                "2025-01-07",
                "2025-01-07",
                "2025-01-07",
                "2025-01-08",
                "2025-01-08",
                "2025-01-08",
                "2025-01-08",
                "2025-01-08",
            ])),
            Arc::new(Int64Array::from(vec![1, 2, 1, 3, 1, 2, 4, 1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "login", "login", "purchase", "login", "login", "purchase", "login", "login",
                "login", "purchase", "login", "login",
            ])),
            Arc::new(Float64Array::from(vec![
                1.2, 0.8, 5.5, 2.1, 1.0, 3.2, 0.9, 1.5, 0.7, 4.8, 1.1, 0.6,
            ])),
        ],
    )
    .map_err(|e| adbc::Error::internal(e.to_string()))?;

    stmt.set_option(StatementOption::TargetTable("events".into()))
        .await?;
    stmt.set_option(StatementOption::IngestMode(IngestMode::Append))
        .await?;
    stmt.bind(raw).await?;
    let n = stmt.execute_update().await?;
    println!("  Ingested {n} raw events.");

    // ── STEP 2: Extract -- query raw data with SQL aggregation
    println!("\nStep 2: Extracting daily summaries via SQL...");
    stmt.set_sql_query(
        "SELECT event_date, \
                COUNT(DISTINCT user_id) AS unique_users, \
                COUNT(*) AS total_events, \
                SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchases, \
                ROUND(AVG(duration_s), 2) AS avg_duration, \
                ROUND(SUM(duration_s), 2) AS total_duration \
         FROM events \
         GROUP BY event_date \
         ORDER BY event_date",
    )
    .await?;
    let (reader, _) = stmt.execute().await?;
    let extracted = collect(reader);

    println!("  Extracted {} daily summary rows.", extracted.num_rows());

    // ── STEP 3: Transform -- compute derived metrics in Arrow
    println!("\nStep 3: Computing derived metrics in Arrow...");
    let unique_users: &Int64Array = extracted.column(1).as_primitive();
    let purchases: &Int64Array = extracted.column(3).as_primitive();

    // Conversion rate = purchases / unique_users
    let conversion_rates: Float64Array = (0..extracted.num_rows())
        .map(|i| {
            let users = unique_users.value(i) as f64;
            let purch = purchases.value(i) as f64;
            if users > 0.0 {
                Some((purch / users * 100.0 * 100.0).round() / 100.0) // round to 2 decimals
            } else {
                Some(0.0)
            }
        })
        .collect();

    // Build the enriched summary batch.
    let summary_schema = Arc::new(Schema::new(vec![
        Field::new("event_date", DataType::Utf8, false),
        Field::new("unique_users", DataType::Int64, false),
        Field::new("total_events", DataType::Int64, false),
        Field::new("purchases", DataType::Int64, false),
        Field::new("avg_duration", DataType::Float64, false),
        Field::new("total_duration", DataType::Float64, false),
        Field::new("conversion_pct", DataType::Float64, false),
    ]));

    let summary = RecordBatch::try_new(
        summary_schema,
        vec![
            Arc::clone(extracted.column(0)),
            Arc::clone(extracted.column(1)),
            Arc::clone(extracted.column(2)),
            Arc::clone(extracted.column(3)),
            Arc::clone(extracted.column(4)),
            Arc::clone(extracted.column(5)),
            Arc::new(conversion_rates),
        ],
    )
    .map_err(|e| adbc::Error::internal(e.to_string()))?;

    // ── STEP 4: Load -- ingest the summary into a new table ──
    println!("\nStep 4: Loading summary into 'daily_summary' table...");
    let mut load_stmt = conn.new_statement().await?;
    load_stmt
        .set_option(StatementOption::TargetTable("daily_summary".into()))
        .await?;
    load_stmt
        .set_option(StatementOption::IngestMode(IngestMode::Create))
        .await?;
    load_stmt.bind(summary).await?;
    let loaded = load_stmt.execute_update().await?;
    println!("  Loaded {loaded} summary rows.");

    // ── STEP 5: Verify -- read back and print ────────────────
    println!("\nStep 5: Final summary report:");
    load_stmt
        .set_sql_query("SELECT * FROM daily_summary ORDER BY event_date")
        .await?;
    let (reader, _) = load_stmt.execute().await?;
    let result = collect(reader);

    println!(
        "{:<12} {:>7} {:>8} {:>10} {:>8} {:>10} {:>10}",
        "Date", "Users", "Events", "Purchases", "Avg Dur", "Total Dur", "Conv %"
    );
    println!(
        "{:-<12} {:->7} {:->8} {:->10} {:->8} {:->10} {:->10}",
        "", "", "", "", "", "", ""
    );

    let dates: &StringArray = result.column(0).as_string();
    let users: &Int64Array = result.column(1).as_primitive();
    let events: &Int64Array = result.column(2).as_primitive();
    let purch: &Int64Array = result.column(3).as_primitive();
    let avg_d: &Float64Array = result.column(4).as_primitive();
    let tot_d: &Float64Array = result.column(5).as_primitive();
    let conv: &Float64Array = result.column(6).as_primitive();

    for i in 0..result.num_rows() {
        println!(
            "{:<12} {:>7} {:>8} {:>10} {:>8.2} {:>10.2} {:>9.1}%",
            dates.value(i),
            users.value(i),
            events.value(i),
            purch.value(i),
            avg_d.value(i),
            tot_d.value(i),
            conv.value(i),
        );
    }

    // Verify schema via metadata API.
    let table_schema = conn.get_table_schema(None, None, "daily_summary").await?;
    println!(
        "\nTable schema has {} columns:",
        table_schema.fields().len()
    );
    for f in table_schema.fields() {
        println!("  {}: {:?}", f.name(), f.data_type());
    }

    conn.rollback().await?;
    println!("\nDone.");
    Ok(())
}

fn collect(reader: Box<dyn RecordBatchReader + Send>) -> RecordBatch {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&schema, &batches).unwrap()
}
