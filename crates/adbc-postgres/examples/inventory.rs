//! Multi-table inventory management example (PostgreSQL).
//!
//! Demonstrates:
//!   - Creating multiple related tables
//!   - Bulk ingesting data via Arrow RecordBatches
//!   - Running JOIN queries across tables
//!   - Transaction commit/rollback for data integrity
//!   - Using catalog metadata APIs to inspect tables
//!
//! Requires a running PostgreSQL server. Set the connection URI:
//!
//! ```bash
//! ADBC_POSTGRES_URI="host=localhost port=5432 user=adbc_test password=adbc_test dbname=adbc_test" \
//!   cargo run -p adbc-postgres --example inventory
//! ```

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Float64Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;

use adbc::{
    Connection, ConnectionOption, Database, DatabaseOption, Driver, IngestMode, ObjectDepth,
    Statement, StatementOption,
};
use adbc_postgres::PostgresDriver;

#[tokio::main]
async fn main() -> adbc::Result<()> {
    let uri = std::env::var("ADBC_POSTGRES_URI").unwrap_or_else(|_| {
        eprintln!(
            "Set ADBC_POSTGRES_URI to connect, e.g.:\n  \
             ADBC_POSTGRES_URI=\"host=localhost port=5432 user=adbc_test password=adbc_test dbname=adbc_test\""
        );
        std::process::exit(1);
    });

    let db = PostgresDriver
        .new_database_with_opts([(DatabaseOption::Uri, uri.into())])
        .await?;
    let conn = db.new_connection().await?;
    conn.set_option(ConnectionOption::AutoCommit(false)).await?;

    // ── Cleanup from previous runs ───────────────────────────
    let mut stmt = conn.new_statement().await?;
    stmt.set_sql_query("DROP TABLE IF EXISTS order_items, orders, products")
        .await?;
    stmt.execute_update().await?;

    // ── 1. Create tables ─────────────────────────────────────
    println!("1. Creating tables...");

    stmt.set_sql_query(
        "CREATE TABLE products (\
            id    BIGINT PRIMARY KEY, \
            name  TEXT NOT NULL, \
            price DOUBLE PRECISION NOT NULL, \
            stock BIGINT NOT NULL\
        )",
    )
    .await?;
    stmt.execute_update().await?;

    stmt.set_sql_query(
        "CREATE TABLE orders (\
            id          BIGINT PRIMARY KEY, \
            customer    TEXT NOT NULL, \
            order_date  TEXT NOT NULL\
        )",
    )
    .await?;
    stmt.execute_update().await?;

    stmt.set_sql_query(
        "CREATE TABLE order_items (\
            order_id   BIGINT NOT NULL, \
            product_id BIGINT NOT NULL, \
            quantity   BIGINT NOT NULL\
        )",
    )
    .await?;
    stmt.execute_update().await?;

    conn.commit().await?;
    println!("   Tables created.");

    // ── 2. Bulk ingest product catalog ───────────────────────
    println!("\n2. Ingesting product catalog...");
    let products = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("stock", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Laptop", "Keyboard", "Mouse", "Monitor", "Headset",
            ])),
            Arc::new(Float64Array::from(vec![
                999.99, 79.99, 29.99, 349.99, 149.99,
            ])),
            Arc::new(Int64Array::from(vec![50, 200, 500, 75, 150])),
        ],
    )
    .map_err(|e| adbc::Error::internal(e.to_string()))?;

    let mut ingest = conn.new_statement().await?;
    ingest
        .set_option(StatementOption::TargetTable("products".into()))
        .await?;
    ingest
        .set_option(StatementOption::IngestMode(IngestMode::Append))
        .await?;
    ingest.bind(products).await?;
    let n = ingest.execute_update().await?;
    println!("   Ingested {n} products.");

    // ── 3. Ingest orders ─────────────────────────────────────
    println!("\n3. Ingesting orders...");
    let orders = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("customer", DataType::Utf8, false),
            Field::new("order_date", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(StringArray::from(vec![
                "2025-03-01",
                "2025-03-02",
                "2025-03-02",
            ])),
        ],
    )
    .map_err(|e| adbc::Error::internal(e.to_string()))?;

    ingest
        .set_option(StatementOption::TargetTable("orders".into()))
        .await?;
    ingest
        .set_option(StatementOption::IngestMode(IngestMode::Append))
        .await?;
    ingest.bind(orders).await?;
    ingest.execute_update().await?;

    let items = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("product_id", DataType::Int64, false),
            Field::new("quantity", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![101, 101, 102, 102, 103])),
            Arc::new(Int64Array::from(vec![1, 3, 2, 4, 1])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2])),
        ],
    )
    .map_err(|e| adbc::Error::internal(e.to_string()))?;

    ingest
        .set_option(StatementOption::TargetTable("order_items".into()))
        .await?;
    ingest
        .set_option(StatementOption::IngestMode(IngestMode::Append))
        .await?;
    ingest.bind(items).await?;
    ingest.execute_update().await?;

    conn.commit().await?;
    println!("   Orders ingested.");

    // ── 4. JOIN query: order details with product info ───────
    println!("\n4. Order details report (JOIN query):");
    stmt.set_sql_query(
        "SELECT o.id AS order_id, \
                o.customer, \
                p.name AS product, \
                oi.quantity, \
                ROUND(CAST(p.price * oi.quantity AS NUMERIC), 2) AS line_total \
         FROM orders o \
         JOIN order_items oi ON oi.order_id = o.id \
         JOIN products p ON p.id = oi.product_id \
         ORDER BY o.id, p.name",
    )
    .await?;
    let (reader, _) = stmt.execute().await?;
    let result = collect(reader);

    println!(
        "{:>8} {:<10} {:<12} {:>4} {:>12}",
        "Order", "Customer", "Product", "Qty", "Line Total"
    );
    println!("{:->8} {:-<10} {:-<12} {:->4} {:->12}", "", "", "", "", "");
    let oid: &Int64Array = result.column(0).as_primitive();
    let cust: &StringArray = result.column(1).as_string();
    let prod: &StringArray = result.column(2).as_string();
    let qty: &Int64Array = result.column(3).as_primitive();
    let total: &Float64Array = result.column(4).as_primitive();
    for i in 0..result.num_rows() {
        println!(
            "{:>8} {:<10} {:<12} {:>4} {:>12.2}",
            oid.value(i),
            cust.value(i),
            prod.value(i),
            qty.value(i),
            total.value(i),
        );
    }

    // ── 5. Revenue per customer ──────────────────────────────
    println!("\n5. Revenue per customer:");
    stmt.set_sql_query(
        "SELECT o.customer, \
                ROUND(CAST(SUM(p.price * oi.quantity) AS NUMERIC), 2) AS total_spent \
         FROM orders o \
         JOIN order_items oi ON oi.order_id = o.id \
         JOIN products p ON p.id = oi.product_id \
         GROUP BY o.customer \
         ORDER BY total_spent DESC",
    )
    .await?;
    let (reader, _) = stmt.execute().await?;
    let result = collect(reader);

    let names: &StringArray = result.column(0).as_string();
    let spent: &Float64Array = result.column(1).as_primitive();
    for i in 0..result.num_rows() {
        println!("  {:<10} ${:>10.2}", names.value(i), spent.value(i));
    }

    // ── 6. Catalog introspection ─────────────────────────────
    println!("\n6. Inspecting catalog metadata...");
    let schema = conn
        .get_table_schema(None, Some("public"), "products")
        .await?;
    println!("   'products' table schema:");
    for f in schema.fields() {
        println!(
            "     {}: {:?} (nullable: {})",
            f.name(),
            f.data_type(),
            f.is_nullable()
        );
    }

    let table_types = collect(conn.get_table_types().await?);
    println!(
        "   Database supports {} table type(s).",
        table_types.num_rows()
    );

    let objects = collect(
        conn.get_objects(ObjectDepth::Tables, None, Some("public"), None, None, None)
            .await?,
    );
    println!(
        "   Found {} catalog entries at table depth.",
        objects.num_rows()
    );

    // ── Cleanup ──────────────────────────────────────────────
    stmt.set_sql_query("DROP TABLE order_items, orders, products")
        .await?;
    stmt.execute_update().await?;
    conn.commit().await?;
    println!("\n   Cleaned up tables. Done.");

    Ok(())
}

fn collect(reader: Box<dyn RecordBatchReader + Send>) -> RecordBatch {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&schema, &batches).unwrap()
}
