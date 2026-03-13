//! PostgreSQL <-> Arrow type conversions and query execution.

use std::sync::Arc;

use arrow_array::{
    array::Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures_util::SinkExt;
use tokio_postgres::types::Type as PgType;
use tokio_postgres::{Client, Column, Row};

use adbc::sql::{QuotedIdent, SqlColumnDef, SqlJoined, SqlLiteral, TrustedSql};
use adbc::{trusted_sql, Error, IngestMode, Result, Status};

// -----------------------------------------------------------------
// PG column type -> Arrow DataType
// -----------------------------------------------------------------

pub fn pg_type_to_arrow(t: &PgType) -> DataType {
    match *t {
        PgType::BOOL => DataType::Boolean,
        PgType::INT2 => DataType::Int16,
        PgType::INT4 | PgType::OID => DataType::Int32,
        PgType::INT8 => DataType::Int64,
        PgType::FLOAT4 => DataType::Float32,
        PgType::FLOAT8 => DataType::Float64,
        // NUMERIC is arbitrary-precision; represent as string to avoid precision loss.
        PgType::NUMERIC => DataType::Utf8,
        PgType::BYTEA => DataType::Binary,
        // text, varchar, char, name, json, jsonb, xml, uuid, ...
        _ => DataType::Utf8,
    }
}

/// Build an Arrow [`Schema`] from a slice of tokio_postgres [`Column`]s.
pub fn pg_columns_to_schema(cols: &[Column]) -> SchemaRef {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| Field::new(c.name(), pg_type_to_arrow(c.type_()), true))
        .collect();
    Arc::new(Schema::new(fields))
}

// -----------------------------------------------------------------
// Rows -> RecordBatch
// -----------------------------------------------------------------

pub fn rows_to_batch(rows: &[Row], schema: SchemaRef) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (ci, field) in schema.fields().iter().enumerate() {
        let col: ArrayRef = match field.data_type() {
            DataType::Boolean => {
                let vals: Vec<Option<bool>> =
                    rows.iter().map(|r| r.get::<_, Option<bool>>(ci)).collect();
                Arc::new(BooleanArray::from(vals))
            }
            DataType::Int16 => {
                let vals: Vec<Option<i16>> =
                    rows.iter().map(|r| r.get::<_, Option<i16>>(ci)).collect();
                Arc::new(Int16Array::from(vals))
            }
            DataType::Int32 => {
                let vals: Vec<Option<i32>> =
                    rows.iter().map(|r| r.get::<_, Option<i32>>(ci)).collect();
                Arc::new(Int32Array::from(vals))
            }
            DataType::Int64 => {
                let vals: Vec<Option<i64>> =
                    rows.iter().map(|r| r.get::<_, Option<i64>>(ci)).collect();
                Arc::new(Int64Array::from(vals))
            }
            DataType::Float32 => {
                let vals: Vec<Option<f32>> =
                    rows.iter().map(|r| r.get::<_, Option<f32>>(ci)).collect();
                Arc::new(Float32Array::from(vals))
            }
            DataType::Float64 => {
                let vals: Vec<Option<f64>> =
                    rows.iter().map(|r| r.get::<_, Option<f64>>(ci)).collect();
                Arc::new(Float64Array::from(vals))
            }
            DataType::Binary => {
                let vals: Vec<Option<Vec<u8>>> = rows
                    .iter()
                    .map(|r| r.get::<_, Option<Vec<u8>>>(ci))
                    .collect();
                let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
                Arc::new(BinaryArray::from_opt_vec(refs))
            }
            _ => {
                // Utf8 and everything else -- stringify via the text protocol.
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| r.get::<_, Option<String>>(ci))
                    .collect();
                Arc::new(StringArray::from(
                    vals.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
                ))
            }
        };
        cols.push(col);
    }

    // Handle zero-column result (e.g. DML returning nothing).
    if cols.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    RecordBatch::try_new(schema, cols).map_err(|e| Error::internal(e.to_string()))
}

// -----------------------------------------------------------------
// Bulk ingest (Arrow -> PostgreSQL)
// -----------------------------------------------------------------

/// Ingest `batches` into `table` using the PostgreSQL COPY protocol.
///
/// Returns the total rows inserted.
pub async fn ingest_batches(
    client: &Client,
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
            let sql = trusted_sql!("CREATE TABLE {}", ddl);
            client.execute(sql.as_str(), &[]).await.map_err(|e| {
                let is_dup = e
                    .as_db_error()
                    .map(|db| *db.code() == tokio_postgres::error::SqlState::DUPLICATE_TABLE)
                    .unwrap_or(false)
                    || e.to_string().to_lowercase().contains("already exists");
                if is_dup {
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
            client
                .execute(drop_sql.as_str(), &[])
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
            let ddl = create_table_sql(&quoted, &schema);
            let sql = trusted_sql!("CREATE TABLE {}", ddl);
            client
                .execute(sql.as_str(), &[])
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        IngestMode::CreateAppend => {
            let ddl = create_table_sql(&quoted, &schema);
            let sql = trusted_sql!("CREATE TABLE IF NOT EXISTS {}", ddl);
            client
                .execute(sql.as_str(), &[])
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        IngestMode::Append => {}
    }

    let col_names = SqlJoined::new(
        schema.fields().iter().map(|f| QuotedIdent::ansi(f.name())),
        ", ",
    );
    let copy_sql = trusted_sql!("COPY {} ({}) FROM STDIN (FORMAT text)", quoted, col_names);

    // Wrap in a transaction for atomicity.
    client
        .batch_execute("BEGIN")
        .await
        .map_err(|e| Error::internal(e.to_string()))?;

    let result = copy_batches(client, copy_sql.as_str(), batches).await;

    match &result {
        Ok(_) => {
            client
                .batch_execute("COMMIT")
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        Err(_) => {
            let _ = client.batch_execute("ROLLBACK").await;
        }
    }

    result
}

async fn copy_batches(client: &Client, copy_sql: &str, batches: &[RecordBatch]) -> Result<i64> {
    let sink: tokio_postgres::CopyInSink<bytes::Bytes> = client
        .copy_in(copy_sql)
        .await
        .map_err(|e| Error::internal(e.to_string()))?;
    futures_util::pin_mut!(sink);

    let mut total: i64 = 0;

    for batch in batches {
        let mut buf = String::new();
        for row in 0..batch.num_rows() {
            for ci in 0..batch.num_columns() {
                if ci > 0 {
                    buf.push('\t');
                }
                let col = batch.column(ci).as_ref();
                if col.is_null(row) {
                    buf.push_str("\\N");
                } else {
                    let val = col_to_copy_text(col, row)?;
                    buf.push_str(&val);
                }
            }
            buf.push('\n');
            total += 1;
        }
        sink.send(bytes::Bytes::from(buf))
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
    }

    sink.close()
        .await
        .map_err(|e| Error::internal(e.to_string()))?;
    Ok(total)
}

/// Format a non-null cell value as a COPY text-format string.
///
/// Special characters (backslash, tab, newline) are escaped per the PostgreSQL
/// COPY text format specification.
fn col_to_copy_text(col: &dyn Array, row: usize) -> Result<String> {
    use arrow_array::*;
    match col.data_type() {
        DataType::Boolean => {
            let v = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::internal("unexpected array type for Boolean"))?
                .value(row);
            Ok(if v { "t".into() } else { "f".into() })
        }
        DataType::Int16 => Ok(col
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| Error::internal("unexpected array type for Int16"))?
            .value(row)
            .to_string()),
        DataType::Int32 => Ok(col
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| Error::internal("unexpected array type for Int32"))?
            .value(row)
            .to_string()),
        DataType::Int64 => Ok(col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::internal("unexpected array type for Int64"))?
            .value(row)
            .to_string()),
        DataType::Float32 => Ok(col
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| Error::internal("unexpected array type for Float32"))?
            .value(row)
            .to_string()),
        DataType::Float64 => Ok(col
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Error::internal("unexpected array type for Float64"))?
            .value(row)
            .to_string()),
        DataType::Utf8 => {
            let v = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::internal("unexpected array type for Utf8"))?
                .value(row);
            Ok(escape_copy_text(v))
        }
        dt => Err(Error::not_impl(format!("unsupported Arrow type: {:?}", dt))),
    }
}

/// Escape special characters for PostgreSQL COPY text format.
fn escape_copy_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out
}

// -----------------------------------------------------------------
// helpers
// -----------------------------------------------------------------

fn create_table_sql(quoted_name: &QuotedIdent, schema: &Schema) -> TrustedSql {
    let col_defs = SqlJoined::new(
        schema
            .fields()
            .iter()
            .map(|f| SqlColumnDef::new(&QuotedIdent::ansi(f.name()), dt_to_pg(f.data_type()))),
        ", ",
    );
    trusted_sql!("{} ({})", quoted_name, col_defs)
}

fn dt_to_pg(dt: &DataType) -> SqlLiteral {
    match dt {
        DataType::Boolean => SqlLiteral("BOOLEAN"),
        DataType::Int8 | DataType::Int16 => SqlLiteral("SMALLINT"),
        DataType::Int32 => SqlLiteral("INTEGER"),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => SqlLiteral("BIGINT"),
        DataType::Float32 => SqlLiteral("REAL"),
        DataType::Float64 => SqlLiteral("DOUBLE PRECISION"),
        DataType::Binary | DataType::LargeBinary => SqlLiteral("BYTEA"),
        _ => SqlLiteral("TEXT"),
    }
}
