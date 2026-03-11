//! Sales analytics example.
//!
//! Demonstrates building an Arrow dataset, bulk-ingesting it into SQLite,
//! and running aggregation queries -- a typical analytics workflow.
//!
//! ```bash
//! cargo run -p adbc-sqlite --example analytics
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

fn main() -> adbc::Result<()> {
    // ── 1. Connect ───────────────────────────────────────────
    let db = SqliteDriver.new_database_with_opts([(DatabaseOption::Uri, ":memory:".into())])?;
    let mut conn = db.new_connection()?;
    conn.set_option(ConnectionOption::AutoCommit(false))?;

    // ── 2. Build a sales dataset ─────────────────────────────
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("quantity", DataType::Int64, false),
        Field::new("unit_price", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "North", "North", "South", "South", "East", "East", "West", "West", "North",
                "South",
            ])),
            Arc::new(StringArray::from(vec![
                "Widget", "Gadget", "Widget", "Gadget", "Widget", "Gadget", "Widget", "Gadget",
                "Widget", "Widget",
            ])),
            Arc::new(Int64Array::from(vec![
                100, 50, 200, 75, 150, 60, 120, 80, 90, 300,
            ])),
            Arc::new(Float64Array::from(vec![
                9.99, 24.99, 9.99, 24.99, 9.99, 24.99, 9.99, 24.99, 9.99, 9.99,
            ])),
        ],
    )
    .map_err(|e| adbc::Error::internal(e.to_string()))?;

    // ── 3. Ingest into SQLite ────────────────────────────────
    let mut stmt = conn.new_statement()?;
    stmt.set_option(StatementOption::TargetTable("sales".into()))?;
    stmt.set_option(StatementOption::IngestMode(IngestMode::Create))?;
    stmt.bind(batch)?;
    let rows = stmt.execute_update()?;
    println!("Ingested {rows} rows into 'sales' table.\n");

    // ── 4. Total revenue by region ───────────────────────────
    stmt.set_sql_query(
        "SELECT region, \
                SUM(quantity * unit_price) AS revenue \
         FROM sales \
         GROUP BY region \
         ORDER BY revenue DESC",
    )?;
    let (reader, _) = stmt.execute()?;
    let result = collect(reader);

    println!("Revenue by region:");
    println!("{:<10} {:>12}", "Region", "Revenue");
    println!("{:-<10} {:->12}", "", "");
    let regions: &StringArray = result.column(0).as_string();
    let revenues: &Float64Array = result.column(1).as_primitive();
    for i in 0..result.num_rows() {
        println!("{:<10} {:>12.2}", regions.value(i), revenues.value(i));
    }

    // ── 5. Best-selling product ──────────────────────────────
    stmt.set_sql_query(
        "SELECT product, \
                SUM(quantity) AS total_qty, \
                AVG(unit_price) AS avg_price \
         FROM sales \
         GROUP BY product \
         ORDER BY total_qty DESC",
    )?;
    let (reader, _) = stmt.execute()?;
    let result = collect(reader);

    println!("\nProduct summary:");
    println!("{:<10} {:>10} {:>12}", "Product", "Total Qty", "Avg Price");
    println!("{:-<10} {:->10} {:->12}", "", "", "");
    let products: &StringArray = result.column(0).as_string();
    let qtys: &Int64Array = result.column(1).as_primitive();
    let prices: &Float64Array = result.column(2).as_primitive();
    for i in 0..result.num_rows() {
        println!(
            "{:<10} {:>10} {:>12.2}",
            products.value(i),
            qtys.value(i),
            prices.value(i),
        );
    }

    // ── 6. Region + product cross-tab ────────────────────────
    stmt.set_sql_query(
        "SELECT region, product, \
                SUM(quantity) AS qty, \
                ROUND(SUM(quantity * unit_price), 2) AS revenue \
         FROM sales \
         GROUP BY region, product \
         ORDER BY region, product",
    )?;
    let (reader, _) = stmt.execute()?;
    let result = collect(reader);

    println!("\nRegion x Product breakdown:");
    println!(
        "{:<10} {:<10} {:>6} {:>12}",
        "Region", "Product", "Qty", "Revenue"
    );
    println!("{:-<10} {:-<10} {:->6} {:->12}", "", "", "", "");
    let r: &StringArray = result.column(0).as_string();
    let p: &StringArray = result.column(1).as_string();
    let q: &Int64Array = result.column(2).as_primitive();
    let rev: &Float64Array = result.column(3).as_primitive();
    for i in 0..result.num_rows() {
        println!(
            "{:<10} {:<10} {:>6} {:>12.2}",
            r.value(i),
            p.value(i),
            q.value(i),
            rev.value(i),
        );
    }

    conn.rollback()?;
    println!("\nDone. (Transaction rolled back -- in-memory DB, nothing to persist.)");
    Ok(())
}

fn collect(reader: Box<dyn RecordBatchReader + Send>) -> RecordBatch {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&schema, &batches).unwrap()
}
