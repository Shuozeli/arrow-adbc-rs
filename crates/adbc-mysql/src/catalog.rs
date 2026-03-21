//! MySQL catalog metadata methods.
//!
//! Mirrors the SQLite catalog implementation's approach for full schema
//! compliance with the ADBC spec (6-variant Union, proper list nullability).

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::DataType;
use mysql_async::prelude::Queryable;
use mysql_async::Conn;

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
            "SYSTEM VIEW",
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

pub async fn get_info_batch(conn: &mut Conn, codes: Option<&[InfoCode]>) -> Result<RecordBatch> {
    let version: Option<String> = conn
        .query_first("SELECT VERSION()")
        .await
        .map_err(|e| Error::io(e.to_string()))?;
    let version = version.unwrap_or_else(|| "unknown".into());

    let sv = [
        "MySQL",
        &version,
        "adbc-mysql",
        env!("CARGO_PKG_VERSION"),
        ">=53,<59",
    ];

    helpers::build_get_info_batch(INFO_ITEMS, &sv, &[], INT_VALS, codes)
}

// ─────────────────────────────────────────────────────────────
// get_table_schema
// ─────────────────────────────────────────────────────────────

pub async fn get_table_schema_impl(
    conn: &mut Conn,
    _catalog: Option<&str>,
    db_schema: Option<&str>,
    name: &str,
) -> Result<arrow_schema::Schema> {
    let current_db: Option<Option<String>> = conn
        .query_first("SELECT DATABASE()")
        .await
        .map_err(|e| Error::io(e.to_string()))?;
    let current_db = current_db.flatten().unwrap_or_default();
    let schema_filter = db_schema.unwrap_or(&current_db).to_owned();

    let rows: Vec<(String, String, String)> = conn
        .exec(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
             FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
             ORDER BY ORDINAL_POSITION",
            (&schema_filter, name),
        )
        .await
        .map_err(|e| Error::io(e.to_string()))?;

    if rows.is_empty() {
        return Err(Error::not_found(format!("Table '{name}' not found")));
    }

    let fields: Vec<arrow_schema::Field> = rows
        .iter()
        .map(|(col_name, data_type, nullable)| {
            let dt = mysql_type_str_to_arrow(data_type);
            arrow_schema::Field::new(col_name, dt, nullable == "YES")
        })
        .collect();

    Ok(arrow_schema::Schema::new(fields))
}

fn mysql_type_str_to_arrow(s: &str) -> DataType {
    match s {
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "mediumint" | "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" | "decimal" | "numeric" => DataType::Float64,
        "tinyblob" | "blob" | "mediumblob" | "longblob" | "binary" | "varbinary" => {
            DataType::Binary
        }
        "bit" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

// ─────────────────────────────────────────────────────────────
// get_objects
// ─────────────────────────────────────────────────────────────

pub async fn get_objects_batch(
    conn: &mut Conn,
    depth: ObjectDepth,
    _catalog: Option<&str>,
    _db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<&[&str]>,
    _col_name: Option<&str>,
) -> Result<RecordBatch> {
    let include_schemas = !matches!(depth, ObjectDepth::Catalogs);
    let include_tables = !matches!(depth, ObjectDepth::Catalogs | ObjectDepth::Schemas);

    let current_db: Option<Option<String>> = conn
        .query_first("SELECT DATABASE()")
        .await
        .map_err(|e| Error::io(e.to_string()))?;
    let current_db = current_db.flatten().ok_or_else(|| {
        Error::invalid_state(
            "no database selected; use USE <db> or specify a database in the connection URI",
        )
    })?;

    let type_filter: std::collections::HashSet<&str> = table_type
        .unwrap_or(&["BASE TABLE", "VIEW"])
        .iter()
        .copied()
        .collect();

    // Fetch tables using parameterized queries.
    let tables: Vec<(String, String)> = if let Some(pat) = table_name {
        conn.exec(
            "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE ?",
            (&current_db, pat),
        )
        .await
        .map_err(|e| Error::internal(e.to_string()))?
    } else {
        conn.exec(
            "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            (&current_db,),
        )
        .await
        .map_err(|e| Error::internal(e.to_string()))?
    };
    let tables: Vec<_> = tables
        .into_iter()
        .filter(|(_, t)| type_filter.contains(t.as_str()))
        .collect();

    // Build table struct arrays and assemble the final batch.
    let table_arrays = helpers::build_table_arrays_simple(&tables, include_tables)?;
    helpers::build_get_objects_batch(table_arrays, &current_db, include_schemas, include_tables)
}
