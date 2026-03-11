//! SQLite ↔ Arrow type conversions and query execution.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, RecordBatch, StringArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

use rusqlite::Connection;

use adbc::sql::{QuotedIdent, SqlColumnDef, SqlJoined, SqlLiteral, SqlPlaceholders, TrustedSql};
use adbc::{trusted_sql, Error, IngestMode, Result, Status};

// ─────────────────────────────────────────────────────────────
// SqliteReader — collects all rows into one RecordBatch
// ─────────────────────────────────────────────────────────────

pub struct SqliteReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SqliteReader {
    /// Execute `sql` and return a [`SqliteReader`] containing all result rows.
    pub fn execute(conn: &Connection, sql: &str) -> Result<Self> {
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| Error::new(e.to_string(), Status::InvalidArguments))?;

        let col_count = stmt.column_count();
        let col_names: Vec<String> = (0..col_count)
            .map(|i| stmt.column_name(i).unwrap_or("").to_owned())
            .collect();

        // Gather all rows into a flat Vec<Vec<Value>>.
        let rows: Vec<Vec<rusqlite::types::Value>> = stmt
            .query_map([], |row| {
                (0..col_count)
                    .map(|i| row.get::<_, rusqlite::types::Value>(i))
                    .collect::<std::result::Result<Vec<_>, _>>()
            })
            .map_err(|e| Error::internal(e.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::internal(e.to_string()))?;

        // Infer schema from the first row's values (rusqlite doesn't expose
        // declared types from a Statement object).
        let fields: Vec<Field> = (0..col_count)
            .map(|i| {
                let dt = rows
                    .first()
                    .map(|r| value_to_dt(&r[i]))
                    .unwrap_or(DataType::Utf8);
                Field::new(&col_names[i], dt, true)
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));
        let batch = build_batch(schema.clone(), &rows)?;

        Ok(Self {
            batch: Some(batch),
            schema,
        })
    }
}

impl Iterator for SqliteReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl arrow_array::RecordBatchReader for SqliteReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ─────────────────────────────────────────────────────────────
// Bulk ingest (Arrow → SQLite)
// ─────────────────────────────────────────────────────────────

/// Ingest `batches` into `table_name`, respecting `mode`.
///
/// Returns the total number of rows inserted.
pub fn ingest_batches(
    conn: &Connection,
    table: &str,
    mode: IngestMode,
    batches: &[RecordBatch],
) -> Result<i64> {
    if batches.is_empty() {
        return Ok(0);
    }

    let schema = batches[0].schema();
    let quoted = QuotedIdent::ansi(table);

    match mode {
        IngestMode::Create => {
            let ddl = create_table_sql(&quoted, &schema);
            conn.execute_batch(ddl.as_str()).map_err(|e| {
                // SQLite error code 1 = SQLITE_ERROR (table exists → "already exists")
                if e.to_string().contains("already exists") {
                    Error::new(
                        format!("Table '{table}' already exists"),
                        Status::AlreadyExists,
                    )
                } else {
                    Error::internal(e.to_string())
                }
            })?;
        }
        IngestMode::Replace => {
            let drop_sql = trusted_sql!("DROP TABLE IF EXISTS {}", quoted);
            conn.execute_batch(drop_sql.as_str())
                .map_err(|e| Error::internal(e.to_string()))?;
            let ddl = create_table_sql(&quoted, &schema);
            conn.execute_batch(ddl.as_str())
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        IngestMode::CreateAppend => {
            let ddl = create_table_if_not_exists_sql(&quoted, &schema);
            conn.execute_batch(ddl.as_str())
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        IngestMode::Append => {
            // Table must already exist — no DDL.
        }
    }

    let col_names = SqlJoined::new(
        schema.fields().iter().map(|f| QuotedIdent::ansi(f.name())),
        ", ",
    );
    let placeholders = SqlPlaceholders::indexed(schema.fields().len());
    let insert_sql = trusted_sql!(
        "INSERT INTO {} ({}) VALUES ({})",
        quoted,
        col_names,
        placeholders,
    );

    let mut total: i64 = 0;
    for batch in batches {
        let mut stmt = conn
            .prepare(insert_sql.as_str())
            .map_err(|e| Error::internal(e.to_string()))?;

        for row in 0..batch.num_rows() {
            let params: Vec<rusqlite::types::Value> = (0..batch.num_columns())
                .map(|col| array_value(batch.column(col).as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            let params_slice: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
            stmt.execute(params_slice.as_slice())
                .map_err(|e| Error::internal(e.to_string()))?;
            total += 1;
        }
    }
    Ok(total)
}

// ─────────────────────────────────────────────────────────────
// Arrow → SQLite helpers
// ─────────────────────────────────────────────────────────────

fn create_table_sql(quoted_name: &QuotedIdent, schema: &Schema) -> TrustedSql {
    let cols = SqlJoined::new(
        schema
            .fields()
            .iter()
            .map(|f| SqlColumnDef::new(&QuotedIdent::ansi(f.name()), dt_to_sqlite(f.data_type()))),
        ", ",
    );
    trusted_sql!("CREATE TABLE {} ({})", quoted_name, cols)
}

fn create_table_if_not_exists_sql(quoted_name: &QuotedIdent, schema: &Schema) -> TrustedSql {
    let cols = SqlJoined::new(
        schema
            .fields()
            .iter()
            .map(|f| SqlColumnDef::new(&QuotedIdent::ansi(f.name()), dt_to_sqlite(f.data_type()))),
        ", ",
    );
    trusted_sql!("CREATE TABLE IF NOT EXISTS {} ({})", quoted_name, cols)
}

fn dt_to_sqlite(dt: &DataType) -> SqlLiteral {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Boolean => SqlLiteral("INTEGER"),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => SqlLiteral("REAL"),
        DataType::LargeBinary | DataType::Binary => SqlLiteral("BLOB"),
        _ => SqlLiteral("TEXT"),
    }
}

fn array_value(col: &dyn Array, row: usize) -> Result<rusqlite::types::Value> {
    use rusqlite::types::Value;
    if col.is_null(row) {
        return Ok(Value::Null);
    }
    match col.data_type() {
        DataType::Boolean => {
            let a = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::internal("unexpected array type: expected BooleanArray"))?;
            Ok(Value::Integer(if a.value(row) { 1 } else { 0 }))
        }
        DataType::Int64 => {
            let a = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::internal("unexpected array type: expected Int64Array"))?;
            Ok(Value::Integer(a.value(row)))
        }
        DataType::Float64 => {
            let a = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::internal("unexpected array type: expected Float64Array"))?;
            Ok(Value::Real(a.value(row)))
        }
        DataType::Utf8 => {
            let a = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::internal("unexpected array type: expected StringArray"))?;
            Ok(Value::Text(a.value(row).to_owned()))
        }
        dt => Err(Error::not_impl(format!("unsupported Arrow type: {:?}", dt))),
    }
}

// ─────────────────────────────────────────────────────────────
// SQLite → Arrow helpers
// ─────────────────────────────────────────────────────────────

pub fn value_to_dt(v: &rusqlite::types::Value) -> DataType {
    match v {
        rusqlite::types::Value::Null => DataType::Null,
        rusqlite::types::Value::Integer(_) => DataType::Int64,
        rusqlite::types::Value::Real(_) => DataType::Float64,
        rusqlite::types::Value::Text(_) => DataType::Utf8,
        rusqlite::types::Value::Blob(_) => DataType::Binary,
    }
}

fn build_batch(schema: SchemaRef, rows: &[Vec<rusqlite::types::Value>]) -> Result<RecordBatch> {
    let n = rows.len();
    let cols: Vec<ArrayRef> = (0..schema.fields().len())
        .map(|ci| col_to_array(schema.field(ci).data_type(), rows, ci, n))
        .collect();
    RecordBatch::try_new(schema, cols).map_err(|e| Error::internal(e.to_string()))
}

fn col_to_array(
    dt: &DataType,
    rows: &[Vec<rusqlite::types::Value>],
    ci: usize,
    n: usize,
) -> ArrayRef {
    use rusqlite::types::Value;
    match dt {
        DataType::Null => Arc::new(NullArray::new(n)),
        DataType::Int64 => Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| match &r[ci] {
                    Value::Integer(i) => Some(*i),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| match &r[ci] {
                    Value::Real(f) => Some(*f),
                    Value::Integer(i) => Some(*i as f64),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Binary => {
            // Build a BinaryArray from blobs.
            use arrow_array::BinaryArray;
            Arc::new(BinaryArray::from_opt_vec(
                rows.iter()
                    .map(|r| match &r[ci] {
                        Value::Blob(b) => Some(b.as_slice()),
                        _ => None,
                    })
                    .collect(),
            ))
        }
        _ => {
            // UTF-8 and everything else → stringify.
            let strings: Vec<Option<String>> = rows
                .iter()
                .map(|r| match &r[ci] {
                    Value::Text(s) => Some(s.clone()),
                    Value::Null => None,
                    Value::Integer(i) => Some(i.to_string()),
                    Value::Real(f) => Some(f.to_string()),
                    Value::Blob(_) => Some("<blob>".to_owned()),
                })
                .collect();
            Arc::new(StringArray::from(
                strings.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
            ))
        }
    }
}
