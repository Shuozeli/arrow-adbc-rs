//! PostgreSQL <-> Arrow type conversions and query execution.

use std::sync::Arc;

use arrow_array::{
    array::Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use postgres::types::Type as PgType;
use postgres::{Column, Row};

use adbc::sql::{QuotedIdent, SqlColumnDef, SqlJoined, SqlLiteral, SqlPlaceholders, TrustedSql};
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
        PgType::FLOAT8 | PgType::NUMERIC => DataType::Float64,
        PgType::BYTEA => DataType::Binary,
        // text, varchar, char, name, json, jsonb, xml, uuid, ...
        _ => DataType::Utf8,
    }
}

/// Build an Arrow [`Schema`] from a slice of postgres [`Column`]s.
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

/// Ingest `batches` into `table` using parameterised INSERT statements.
///
/// Returns the total rows inserted.
pub fn ingest_batches(
    client: &mut postgres::Client,
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
            client.execute(sql.as_str(), &[]).map_err(|e| {
                // SQL state 42P07 = duplicate_table
                let is_dup = e
                    .as_db_error()
                    .map(|db| db.code() == &postgres::error::SqlState::DUPLICATE_TABLE)
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
                .map_err(|e| Error::internal(e.to_string()))?;
            let ddl = create_table_sql(&quoted, &schema);
            let sql = trusted_sql!("CREATE TABLE {}", ddl);
            client
                .execute(sql.as_str(), &[])
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        IngestMode::CreateAppend => {
            let ddl = create_table_sql(&quoted, &schema);
            let sql = trusted_sql!("CREATE TABLE IF NOT EXISTS {}", ddl);
            client
                .execute(sql.as_str(), &[])
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        IngestMode::Append => {}
    }

    let col_names = SqlJoined::new(
        schema.fields().iter().map(|f| QuotedIdent::ansi(f.name())),
        ", ",
    );
    let placeholders = SqlPlaceholders::dollar(schema.fields().len());
    let insert_sql = trusted_sql!(
        "INSERT INTO {} ({}) VALUES ({})",
        quoted,
        col_names,
        placeholders
    );

    let mut total: i64 = 0;
    for batch in batches {
        for row in 0..batch.num_rows() {
            let params: Vec<Box<dyn postgres::types::ToSql + Sync>> = (0..batch.num_columns())
                .map(|ci| col_to_sql(batch.column(ci).as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            let refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
                params.iter().map(|p| p.as_ref()).collect();
            client
                .execute(insert_sql.as_str(), refs.as_slice())
                .map_err(|e| Error::internal(e.to_string()))?;
            total += 1;
        }
    }
    Ok(total)
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

fn col_to_sql(col: &dyn Array, row: usize) -> Result<Box<dyn postgres::types::ToSql + Sync>> {
    use arrow_array::*;
    if col.is_null(row) {
        // Return a typed NULL so postgres can pick the correct OID for the column.
        return Ok(match col.data_type() {
            DataType::Boolean => Box::new(Option::<bool>::None),
            DataType::Int8 | DataType::Int16 => Box::new(Option::<i16>::None),
            DataType::Int32 => Box::new(Option::<i32>::None),
            DataType::Int64 => Box::new(Option::<i64>::None),
            DataType::Float32 => Box::new(Option::<f32>::None),
            DataType::Float64 => Box::new(Option::<f64>::None),
            DataType::Binary => Box::new(Option::<Vec<u8>>::None),
            _ => Box::new(Option::<String>::None),
        });
    }
    match col.data_type() {
        DataType::Boolean => {
            let v = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::internal("unexpected array type for Boolean"))?
                .value(row);
            Ok(Box::new(v))
        }
        DataType::Int16 => {
            let v = col
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| Error::internal("unexpected array type for Int16"))?
                .value(row);
            Ok(Box::new(v))
        }
        DataType::Int32 => {
            let v = col
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::internal("unexpected array type for Int32"))?
                .value(row);
            Ok(Box::new(v))
        }
        DataType::Int64 => {
            let v = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::internal("unexpected array type for Int64"))?
                .value(row);
            Ok(Box::new(v))
        }
        DataType::Float32 => {
            let v = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| Error::internal("unexpected array type for Float32"))?
                .value(row);
            Ok(Box::new(v))
        }
        DataType::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::internal("unexpected array type for Float64"))?
                .value(row);
            Ok(Box::new(v))
        }
        DataType::Utf8 => {
            let v = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::internal("unexpected array type for Utf8"))?
                .value(row)
                .to_owned();
            Ok(Box::new(v))
        }
        dt => Err(Error::not_impl(format!("unsupported Arrow type: {:?}", dt))),
    }
}
