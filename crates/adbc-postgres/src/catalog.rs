//! PostgreSQL catalog metadata methods.
//!
//! Mirrors the SQLite catalog implementation's approach for full schema
//! compliance with the ADBC spec (6-variant Union, proper list nullability).

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::DataType;
use tokio_postgres::Client;

use adbc::{helpers, schema as sch, Error, InfoCode, ObjectDepth, Result};

// ─────────────────────────────────────────────────────────────
// get_table_types
// ─────────────────────────────────────────────────────────────

pub fn get_table_types_batch() -> Result<RecordBatch> {
    RecordBatch::try_new(
        sch::TABLE_TYPES_SCHEMA.clone(),
        vec![Arc::new(StringArray::from(vec![
            "BASE TABLE",
            "VIEW",
            "FOREIGN",
            "MATERIALIZED VIEW",
        ]))],
    )
    .map_err(|e| Error::internal(e.to_string()))
}

// ─────────────────────────────────────────────────────────────
// get_info
// ─────────────────────────────────────────────────────────────

const INFO_ITEMS: &[helpers::InfoItem] = &[
    (InfoCode::VendorName, 0, 0, 0),
    (InfoCode::VendorVersion, 1, 0, 1),
    (InfoCode::DriverName, 100, 0, 2),
    (InfoCode::DriverVersion, 101, 0, 3),
    (InfoCode::DriverArrowVersion, 102, 0, 4),
    (InfoCode::DriverAdbcVersion, 103, 2, 0),
];
const INT_VALS: &[i64] = &[1_001_000];

pub async fn get_info_batch(client: &Client, codes: Option<&[InfoCode]>) -> Result<RecordBatch> {
    let server_version: String = client
        .query_one("SELECT current_setting('server_version')", &[])
        .await
        .map(|r| r.get::<_, String>(0))
        .unwrap_or_else(|_| "unknown".into());

    let sv = [
        "PostgreSQL",
        &server_version,
        "adbc-postgres",
        env!("CARGO_PKG_VERSION"),
        ">=53,<59",
    ];

    helpers::build_get_info_batch(INFO_ITEMS, &sv, &[], INT_VALS, codes)
}

// ─────────────────────────────────────────────────────────────
// get_table_schema
// ─────────────────────────────────────────────────────────────

pub async fn get_table_schema_impl(
    client: &Client,
    _catalog: Option<&str>,
    db_schema: Option<&str>,
    name: &str,
) -> Result<arrow_schema::Schema> {
    let schema_filter = db_schema.unwrap_or("public");
    let rows = client
        .query(
            "SELECT column_name, data_type, is_nullable
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&schema_filter, &name],
        )
        .await
        .map_err(|e| Error::io(e.to_string()))?;

    if rows.is_empty() {
        return Err(Error::not_found(format!("Table '{name}' not found")));
    }

    let fields: Vec<arrow_schema::Field> = rows
        .iter()
        .map(|r| {
            let col_name: String = r.get(0);
            let data_type_str: String = r.get(1);
            let nullable: String = r.get(2);
            let dt = pg_type_str_to_arrow(&data_type_str);
            arrow_schema::Field::new(&col_name, dt, nullable == "YES")
        })
        .collect();

    Ok(arrow_schema::Schema::new(fields))
}

fn pg_type_str_to_arrow(s: &str) -> DataType {
    match s {
        "boolean" => DataType::Boolean,
        "smallint" => DataType::Int16,
        "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "double precision" => DataType::Float64,
        // Map numeric/decimal to Utf8 to match runtime conversion in convert.rs
        // which preserves full precision by returning string representation.
        "numeric" | "decimal" => DataType::Utf8,
        "bytea" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

// ─────────────────────────────────────────────────────────────
// get_objects
// ─────────────────────────────────────────────────────────────

pub async fn get_objects_batch(
    client: &Client,
    depth: ObjectDepth,
    _catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<&[&str]>,
    _column_name: Option<&str>,
) -> Result<RecordBatch> {
    let include_schemas = !matches!(depth, ObjectDepth::Catalogs);
    let include_tables = !matches!(depth, ObjectDepth::Catalogs | ObjectDepth::Schemas);

    let type_filter: std::collections::HashSet<&str> = table_type
        .unwrap_or(&["BASE TABLE", "VIEW"])
        .iter()
        .copied()
        .collect();

    let schema_filter = db_schema.unwrap_or("public");

    // Fetch tables using parameterized queries to prevent SQL injection.
    let rows = if let Some(pat) = table_name {
        client
            .query(
                "SELECT table_name, table_type FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name LIKE $2",
                &[&schema_filter, &pat],
            )
            .await
            .map_err(|e| Error::internal(e.to_string()))?
    } else {
        client
            .query(
                "SELECT table_name, table_type FROM information_schema.tables \
                 WHERE table_schema = $1",
                &[&schema_filter],
            )
            .await
            .map_err(|e| Error::internal(e.to_string()))?
    };
    let tables: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .filter(|(_, t)| type_filter.contains(t.as_str()))
        .collect();

    // Build table struct arrays and assemble the final batch.
    let table_arrays = helpers::build_table_arrays_simple(&tables, include_tables)?;
    helpers::build_get_objects_batch(table_arrays, schema_filter, include_schemas, include_tables)
}
