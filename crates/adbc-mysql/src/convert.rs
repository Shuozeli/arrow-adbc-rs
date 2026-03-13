//! MySQL <-> Arrow type conversions and query execution.

use std::sync::Arc;

use arrow_array::{
    array::Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Value};

use adbc::sql::{QuotedIdent, SqlColumnDef, SqlJoined, SqlLiteral, SqlPlaceholders, TrustedSql};
use adbc::{trusted_sql, Error, IngestMode, Result, Status};

// ─────────────────────────────────────────────────────────────
// ColumnType -> Arrow DataType
// ─────────────────────────────────────────────────────────────

pub fn mysql_type_to_arrow(col: &mysql_async::Column) -> DataType {
    use mysql_async::consts::ColumnType::*;
    match col.column_type() {
        MYSQL_TYPE_TINY
            if col
                .flags()
                .contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG) =>
        {
            DataType::UInt8
        }
        MYSQL_TYPE_TINY => DataType::Int8,
        MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR
            if col
                .flags()
                .contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG) =>
        {
            DataType::UInt16
        }
        MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR => DataType::Int16,
        MYSQL_TYPE_LONG | MYSQL_TYPE_INT24
            if col
                .flags()
                .contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG) =>
        {
            DataType::UInt32
        }
        MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => DataType::Int32,
        MYSQL_TYPE_LONGLONG
            if col
                .flags()
                .contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG) =>
        {
            DataType::UInt64
        }
        MYSQL_TYPE_LONGLONG => DataType::Int64,
        MYSQL_TYPE_FLOAT => DataType::Float32,
        MYSQL_TYPE_DOUBLE | MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => DataType::Float64,
        MYSQL_TYPE_BIT => DataType::Boolean,
        MYSQL_TYPE_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_TINY_BLOB
            if !col
                .flags()
                .contains(mysql_async::consts::ColumnFlags::BINARY_FLAG) =>
        {
            DataType::Utf8
        }
        MYSQL_TYPE_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_TINY_BLOB => {
            DataType::Binary
        }
        _ => DataType::Utf8,
    }
}

pub fn mysql_columns_to_schema(cols: &[mysql_async::Column]) -> SchemaRef {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| Field::new(c.name_str().as_ref(), mysql_type_to_arrow(c), true))
        .collect();
    Arc::new(Schema::new(fields))
}

// ─────────────────────────────────────────────────────────────
// mysql_async::Row -> RecordBatch
// ─────────────────────────────────────────────────────────────

pub fn rows_to_batch(rows: Vec<mysql_async::Row>, schema: SchemaRef) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (ci, field) in schema.fields().iter().enumerate() {
        let values: Vec<Value> = rows
            .iter()
            .map(|r| r.get::<Value, _>(ci).unwrap_or(Value::NULL))
            .collect();

        let col: ArrayRef = match field.data_type() {
            DataType::Boolean => Arc::new(BooleanArray::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Int(i) => Some(*i != 0),
                        Value::UInt(u) => Some(*u != 0),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Int8 => Arc::new(Int8Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Int(i) => i8::try_from(*i).ok(),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Int16 => Arc::new(Int16Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Int(i) => i16::try_from(*i).ok(),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Int32 => Arc::new(Int32Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Int(i) => i32::try_from(*i).ok(),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Int(i) => Some(*i),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt8 => Arc::new(UInt8Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::UInt(u) => u8::try_from(*u).ok(),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt16 => Arc::new(UInt16Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::UInt(u) => u16::try_from(*u).ok(),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt32 => Arc::new(UInt32Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::UInt(u) => u32::try_from(*u).ok(),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt64 => Arc::new(UInt64Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::UInt(u) => Some(*u),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Float32 => Arc::new(Float32Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Float(f) => Some(*f),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Float64 => Arc::new(Float64Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Double(f) => Some(*f),
                        Value::Float(f) => Some(*f as f64),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Binary => {
                let vecs: Vec<Option<Vec<u8>>> = values
                    .iter()
                    .map(|v| match v {
                        Value::Bytes(b) => Some(b.clone()),
                        Value::NULL => None,
                        _ => None,
                    })
                    .collect();
                Arc::new(BinaryArray::from_opt_vec(
                    vecs.iter().map(|v| v.as_deref()).collect(),
                ))
            }
            _ => {
                let strings: Vec<Option<String>> = values
                    .iter()
                    .map(|v| match v {
                        Value::Bytes(b) => {
                            let s = String::from_utf8(b.clone()).map_err(|e| {
                                Error::internal(format!("invalid UTF-8 in column: {e}"))
                            })?;
                            Ok(Some(s))
                        }
                        Value::NULL => Ok(None),
                        Value::Int(i) => Ok(Some(i.to_string())),
                        Value::UInt(u) => Ok(Some(u.to_string())),
                        Value::Float(f) => Ok(Some(f.to_string())),
                        Value::Double(f) => Ok(Some(f.to_string())),
                        _ => Ok(None),
                    })
                    .collect::<Result<Vec<Option<String>>>>()?;
                Arc::new(StringArray::from(
                    strings.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
                ))
            }
        };
        cols.push(col);
    }

    RecordBatch::try_new(schema, cols).map_err(|e| Error::internal(e.to_string()))
}

// ─────────────────────────────────────────────────────────────
// Bulk ingest (Arrow -> MySQL)
// ─────────────────────────────────────────────────────────────

pub async fn ingest_batches(
    conn: &mut Conn,
    table: &str,
    mode: IngestMode,
    batches: &[RecordBatch],
) -> Result<i64> {
    if batches.is_empty() {
        return Ok(0);
    }

    let schema = batches[0].schema();

    match mode {
        IngestMode::Create => {
            let ddl = create_table_sql(table, &schema);
            conn.query_drop(ddl.as_str()).await.map_err(|e| {
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
            let drop = trusted_sql!("DROP TABLE IF EXISTS {}", QuotedIdent::mysql(table));
            conn.query_drop(drop.as_str())
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
            let ddl = create_table_sql(table, &schema);
            conn.query_drop(ddl.as_str())
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        IngestMode::CreateAppend => {
            let ddl = create_table_sql_if_not_exists(table, &schema);
            conn.query_drop(ddl.as_str())
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        IngestMode::Append => {}
    }

    let quoted_table = QuotedIdent::mysql(table);
    let col_names: Vec<QuotedIdent> = schema
        .fields()
        .iter()
        .map(|f| QuotedIdent::mysql(f.name()))
        .collect();
    let cols_joined = SqlJoined::new(col_names, ", ");
    let placeholders = SqlPlaceholders::anonymous(schema.fields().len());
    let insert_sql = trusted_sql!(
        "INSERT INTO {} ({}) VALUES ({})",
        quoted_table,
        cols_joined,
        placeholders,
    );

    // Wrap all inserts in a transaction for atomicity.
    conn.query_drop("BEGIN")
        .await
        .map_err(|e| Error::internal(e.to_string()))?;

    let result = insert_rows(conn, insert_sql.as_str(), batches).await;

    match &result {
        Ok(_) => {
            conn.query_drop("COMMIT")
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
        }
        Err(_) => {
            let _ = conn.query_drop("ROLLBACK").await;
        }
    }

    result
}

async fn insert_rows(conn: &mut Conn, insert_sql: &str, batches: &[RecordBatch]) -> Result<i64> {
    let mut total: i64 = 0;
    for batch in batches {
        for row in 0..batch.num_rows() {
            let params: Vec<Value> = (0..batch.num_columns())
                .map(|ci| col_to_value(batch.column(ci).as_ref(), row))
                .collect::<Result<Vec<Value>>>()?;
            conn.exec_drop(insert_sql, mysql_async::Params::Positional(params))
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
            total += 1;
        }
    }
    Ok(total)
}

// ─────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────

fn create_table_sql(table: &str, schema: &Schema) -> TrustedSql {
    let quoted = QuotedIdent::mysql(table);
    let col_defs = schema_to_col_defs(schema);
    let cols_joined = SqlJoined::new(col_defs, ", ");
    trusted_sql!("CREATE TABLE {} ({})", quoted, cols_joined)
}

fn create_table_sql_if_not_exists(table: &str, schema: &Schema) -> TrustedSql {
    let quoted = QuotedIdent::mysql(table);
    let col_defs = schema_to_col_defs(schema);
    let cols_joined = SqlJoined::new(col_defs, ", ");
    trusted_sql!("CREATE TABLE IF NOT EXISTS {} ({})", quoted, cols_joined)
}

fn schema_to_col_defs(schema: &Schema) -> Vec<SqlColumnDef> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let ident = QuotedIdent::mysql(f.name());
            let ty = dt_to_mysql(f.data_type());
            SqlColumnDef::new(&ident, ty)
        })
        .collect()
}

fn dt_to_mysql(dt: &DataType) -> SqlLiteral {
    match dt {
        DataType::Boolean | DataType::Int8 => SqlLiteral("TINYINT"),
        DataType::Int16 | DataType::UInt8 => SqlLiteral("SMALLINT"),
        DataType::Int32 | DataType::UInt16 => SqlLiteral("INT"),
        DataType::Int64 | DataType::UInt32 => SqlLiteral("BIGINT"),
        DataType::UInt64 => SqlLiteral("BIGINT UNSIGNED"),
        DataType::Float32 => SqlLiteral("FLOAT"),
        DataType::Float64 => SqlLiteral("DOUBLE"),
        DataType::Binary | DataType::LargeBinary => SqlLiteral("BLOB"),
        _ => SqlLiteral("TEXT"),
    }
}

fn downcast_col<'a, T: 'static>(col: &'a dyn Array, type_name: &str) -> Result<&'a T> {
    col.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::internal(format!("unexpected array type for {type_name}")))
}

fn col_to_value(col: &dyn Array, row: usize) -> Result<Value> {
    use arrow_array::*;
    if col.is_null(row) {
        return Ok(Value::NULL);
    }
    match col.data_type() {
        DataType::Boolean => {
            let v = downcast_col::<BooleanArray>(col, "Boolean")?.value(row);
            Ok(Value::Int(if v { 1 } else { 0 }))
        }
        DataType::Int8 => Ok(Value::Int(
            downcast_col::<Int8Array>(col, "Int8")?.value(row) as i64,
        )),
        DataType::Int16 => Ok(Value::Int(
            downcast_col::<Int16Array>(col, "Int16")?.value(row) as i64,
        )),
        DataType::Int32 => Ok(Value::Int(
            downcast_col::<Int32Array>(col, "Int32")?.value(row) as i64,
        )),
        DataType::Int64 => Ok(Value::Int(
            downcast_col::<Int64Array>(col, "Int64")?.value(row),
        )),
        DataType::UInt8 => Ok(Value::UInt(
            downcast_col::<UInt8Array>(col, "UInt8")?.value(row) as u64,
        )),
        DataType::UInt16 => Ok(Value::UInt(
            downcast_col::<UInt16Array>(col, "UInt16")?.value(row) as u64,
        )),
        DataType::UInt32 => Ok(Value::UInt(
            downcast_col::<UInt32Array>(col, "UInt32")?.value(row) as u64,
        )),
        DataType::UInt64 => Ok(Value::UInt(
            downcast_col::<UInt64Array>(col, "UInt64")?.value(row),
        )),
        DataType::Float32 => Ok(Value::Float(
            downcast_col::<Float32Array>(col, "Float32")?.value(row),
        )),
        DataType::Float64 => Ok(Value::Double(
            downcast_col::<Float64Array>(col, "Float64")?.value(row),
        )),
        DataType::Utf8 => Ok(Value::Bytes(
            downcast_col::<StringArray>(col, "Utf8")?
                .value(row)
                .as_bytes()
                .to_vec(),
        )),
        dt => Err(Error::not_impl(format!("unsupported Arrow type: {dt:?}"))),
    }
}
