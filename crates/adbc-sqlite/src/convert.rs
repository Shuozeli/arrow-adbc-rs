//! SQLite ↔ Arrow type conversions and query execution.

use std::sync::Arc;

use arrow_array::{
    builder::{Float64Builder, Int64Builder, StringBuilder},
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, RecordBatch, StringArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

use rusqlite::Connection;

use adbc::sql::{QuotedIdent, SqlColumnDef, SqlJoined, SqlLiteral, SqlPlaceholders, TrustedSql};
use adbc::{trusted_sql, Error, IngestMode, Result, Status};

// ─────────────────────────────────────────────────────────────
// SqliteReader — builds Arrow arrays directly from query results
// ─────────────────────────────────────────────────────────────

pub struct SqliteReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SqliteReader {
    /// Execute `sql` and return a [`SqliteReader`] containing all result rows.
    ///
    /// Builds Arrow column arrays directly using builders, avoiding an
    /// intermediate `Vec<Vec<Value>>` allocation.
    ///
    /// If `params` is provided, they are bound as positional parameters to the
    /// prepared statement.
    pub fn execute(
        conn: &Connection,
        sql: &str,
        params: Option<&[rusqlite::types::Value]>,
    ) -> Result<Self> {
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| Error::new(e.to_string(), Status::InvalidArguments))?;

        let col_count = stmt.column_count();
        let col_names: Vec<String> = (0..col_count)
            .map(|i| stmt.column_name(i).unwrap_or("").to_owned())
            .collect();

        // Collect rows into column-oriented storage directly.
        // Each column gets its own Vec of Values.
        let mut columns: Vec<Vec<rusqlite::types::Value>> =
            (0..col_count).map(|_| Vec::new()).collect();

        let param_refs: Vec<&dyn rusqlite::ToSql> = params
            .map(|p| p.iter().map(|v| v as &dyn rusqlite::ToSql).collect())
            .unwrap_or_default();
        let mut row_iter = stmt
            .query(param_refs.as_slice())
            .map_err(|e| Error::internal(e.to_string()))?;

        while let Some(row) = row_iter
            .next()
            .map_err(|e| Error::internal(e.to_string()))?
        {
            for (i, col) in columns.iter_mut().enumerate() {
                let val: rusqlite::types::Value =
                    row.get(i).map_err(|e| Error::internal(e.to_string()))?;
                col.push(val);
            }
        }

        let n_rows = if columns.is_empty() {
            0
        } else {
            columns[0].len()
        };

        // Infer schema by scanning each column for the first non-NULL value.
        let fields: Vec<Field> = (0..col_count)
            .map(|i| {
                let dt = columns[i]
                    .iter()
                    .map(value_to_dt)
                    .find(|dt| *dt != DataType::Null)
                    .unwrap_or(DataType::Utf8);
                Field::new(&col_names[i], dt, true)
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));

        // Build Arrow arrays directly from column-oriented storage.
        let arrays: Vec<ArrayRef> = (0..col_count)
            .map(|ci| col_values_to_array(schema.field(ci).data_type(), &columns[ci], n_rows))
            .collect();

        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| Error::internal(e.to_string()))?;

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

/// Build an Arrow array from a column of SQLite values using builders.
fn col_values_to_array(dt: &DataType, values: &[rusqlite::types::Value], n: usize) -> ArrayRef {
    use rusqlite::types::Value;
    match dt {
        DataType::Null => Arc::new(NullArray::new(n)),
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(n);
            for v in values {
                match v {
                    Value::Integer(i) => builder.append_value(*i),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(n);
            for v in values {
                match v {
                    Value::Real(f) => builder.append_value(*f),
                    Value::Integer(i) => builder.append_value(*i as f64),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Binary => {
            use arrow_array::BinaryArray;
            Arc::new(BinaryArray::from_opt_vec(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Blob(b) => Some(b.as_slice()),
                        _ => None,
                    })
                    .collect(),
            ))
        }
        _ => {
            // UTF-8 and everything else -> stringify.
            let mut builder = StringBuilder::with_capacity(n, n * 16);
            for v in values {
                match v {
                    Value::Text(s) => builder.append_value(s),
                    Value::Null => builder.append_null(),
                    Value::Integer(i) => builder.append_value(i.to_string()),
                    Value::Real(f) => builder.append_value(f.to_string()),
                    Value::Blob(_) => builder.append_value("<blob>"),
                }
            }
            Arc::new(builder.finish())
        }
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

    // Wrap all inserts in a transaction for atomicity and performance.
    let in_explicit_txn = conn.is_autocommit();
    if in_explicit_txn {
        conn.execute_batch("BEGIN")
            .map_err(|e| Error::internal(e.to_string()))?;
    }

    let result = (|| -> Result<i64> {
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
    })();

    if in_explicit_txn {
        match &result {
            Ok(_) => {
                conn.execute_batch("COMMIT")
                    .map_err(|e| Error::internal(e.to_string()))?;
            }
            Err(_) => {
                let _ = conn.execute_batch("ROLLBACK");
            }
        }
    }

    result
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
        DataType::Binary => {
            use arrow_array::BinaryArray;
            let a = col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| Error::internal("unexpected array type: expected BinaryArray"))?;
            Ok(Value::Blob(a.value(row).to_vec()))
        }
        dt => Err(Error::not_impl(format!("unsupported Arrow type: {:?}", dt))),
    }
}

/// Extract a single row of rusqlite params from a bound RecordBatch.
///
/// Used by `execute` and `execute_update` to pass bound parameters to
/// parameterized SQL queries.
pub fn batch_row_to_params(batch: &RecordBatch, row: usize) -> Result<Vec<rusqlite::types::Value>> {
    (0..batch.num_columns())
        .map(|col| array_value(batch.column(col).as_ref(), row))
        .collect()
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
