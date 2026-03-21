//! ADBC catalog/metadata methods for SQLite.
//!
//! Implements `get_info`, `get_table_types`, `get_table_schema`, and
//! `get_objects` by querying SQLite's `sqlite_master` table.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, ListArray, RecordBatch, StringArray,
    StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use rusqlite::Connection;

use adbc::sql::QuotedIdent;
use adbc::{
    helpers, schema as sch, trusted_sql, Error, InfoCode, ObjectDepth, Result, TableArrays,
};

// ─────────────────────────────────────────────────────────────
// get_table_types
// ─────────────────────────────────────────────────────────────

pub fn get_table_types_batch() -> Result<RecordBatch> {
    RecordBatch::try_new(
        sch::TABLE_TYPES_SCHEMA.clone(),
        vec![Arc::new(StringArray::from(vec!["table", "view"]))],
    )
    .map_err(|e| Error::internal(e.to_string()))
}

// ─────────────────────────────────────────────────────────────
// get_info
// ─────────────────────────────────────────────────────────────

/// Static table of all supported info items:
/// `(InfoCode, numeric_name, union_type_id, index_within_child)`
const INFO_ITEMS: &[helpers::InfoItem] = &[
    (InfoCode::VendorName, 0, 0, 0),
    (InfoCode::VendorVersion, 1, 0, 1),
    (InfoCode::VendorArrowVersion, 2, 0, 2),
    (InfoCode::VendorSql, 3, 1, 0),
    (InfoCode::VendorSubstrait, 4, 1, 1),
    (InfoCode::DriverName, 100, 0, 3),
    (InfoCode::DriverVersion, 101, 0, 4),
    (InfoCode::DriverArrowVersion, 102, 0, 5),
    (InfoCode::DriverAdbcVersion, 103, 2, 0),
];

const BOOL_VALS: &[bool] = &[true, false]; // VendorSql, VendorSubstrait
const INT_VALS: &[i64] = &[1_001_000]; // DriverAdbcVersion = ADBC 1.1.0

pub fn get_info_batch(codes: Option<&[InfoCode]>) -> Result<RecordBatch> {
    let sv = [
        "SQLite",                  // VendorName  (index 0)
        rusqlite::version(),       // VendorVersion (1)
        "",                        // VendorArrowVersion (2)
        "adbc-sqlite",             // DriverName (3)
        env!("CARGO_PKG_VERSION"), // DriverVersion (4)
        "",                        // DriverArrowVersion (5)
    ];
    helpers::build_get_info_batch(INFO_ITEMS, &sv, BOOL_VALS, INT_VALS, codes)
}

// ─────────────────────────────────────────────────────────────
// get_table_schema
// ─────────────────────────────────────────────────────────────

/// Returns an Arrow Schema for `table_name` by running `PRAGMA table_info`.
///
/// SQLite doesn't support bind parameters in PRAGMA statements, so the
/// table name is inlined (with double-quote escaping).
pub fn get_table_schema_impl(conn: &Connection, name: &str) -> Result<arrow_schema::Schema> {
    let quoted = QuotedIdent::ansi(name);
    let sql = trusted_sql!("PRAGMA table_info({})", quoted);
    let mut stmt = conn
        .prepare(sql.as_str())
        .map_err(|e| Error::internal(e.to_string()))?;

    let cols: Vec<(String, String)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })
        .map_err(|e| Error::internal(e.to_string()))?
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| Error::internal(e.to_string()))?;

    if cols.is_empty() {
        return Err(Error::not_found(format!("Table '{name}' not found")));
    }

    let fields: Vec<Field> = cols
        .into_iter()
        .map(|(col_name, col_type)| {
            let dt = sqlite_decl_to_dt(&col_type);
            Field::new(col_name, dt, true)
        })
        .collect();

    Ok(arrow_schema::Schema::new(fields))
}

fn sqlite_decl_to_dt(declared: &str) -> DataType {
    let u = declared.to_uppercase();
    if u.contains("INT") {
        DataType::Int64
    } else if u.contains("REAL") || u.contains("FLOA") || u.contains("DOUB") {
        DataType::Float64
    } else if u.contains("BOOL") {
        DataType::Boolean
    } else {
        DataType::Utf8
    }
}

// ─────────────────────────────────────────────────────────────
// get_objects
// ─────────────────────────────────────────────────────────────

pub fn get_objects_batch(
    conn: &Connection,
    depth: ObjectDepth,
    _catalog: Option<&str>,
    _db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<&[&str]>,
    _col_name: Option<&str>,
) -> Result<RecordBatch> {
    // SQLite has one implicit catalog (the filename or ":memory:") and one
    // schema "main".  We model catalog_name = NULL, db_schema = "main".

    // ── Fetch tables/views ───────────────────────────────────
    let include_schemas = !matches!(depth, ObjectDepth::Catalogs);
    let include_tables = !matches!(depth, ObjectDepth::Catalogs | ObjectDepth::Schemas);
    let include_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);

    let type_filter: std::collections::HashSet<&str> = table_type
        .unwrap_or(&["table", "view"])
        .iter()
        .copied()
        .collect();

    let mut sql =
        String::from("SELECT name, type FROM sqlite_master WHERE type IN ('table','view')");
    if table_name.is_some() {
        sql.push_str(" AND name LIKE ?1");
    }

    let tables: Vec<(String, String)> = if let Some(pat) = table_name {
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| Error::internal(e.to_string()))?;
        let result = stmt
            .query_map(rusqlite::params![pat], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| Error::internal(e.to_string()))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| Error::internal(e.to_string()))?;
        result
    } else {
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| Error::internal(e.to_string()))?;
        let result = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| Error::internal(e.to_string()))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| Error::internal(e.to_string()))?;
        result
    };

    let tables: Vec<_> = tables
        .into_iter()
        .filter(|(_, t)| type_filter.contains(t.as_str()))
        .collect();

    // ── Build table struct arrays ────────────────────────────
    let (tname_arr, ttype_arr, tcols_arr, tcons_arr) =
        build_table_arrays(conn, &tables, include_tables, include_columns)?;

    let table_arrays = (tname_arr, ttype_arr, tcols_arr, tcons_arr);
    helpers::build_get_objects_batch(table_arrays, "main", include_schemas, include_tables)
}

fn build_table_arrays(
    conn: &Connection,
    tables: &[(String, String)],
    include_tables: bool,
    include_columns: bool,
) -> Result<TableArrays> {
    if !include_tables || tables.is_empty() {
        return Ok((
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(helpers::make_empty_col_list()),
            Arc::new(helpers::make_empty_cons_list()),
        ));
    }

    let mut tnames: Vec<&str> = Vec::new();
    let mut ttypes: Vec<&str> = Vec::new();
    let mut col_offsets: Vec<i32> = vec![0];
    let mut cons_offsets: Vec<i32> = vec![0];

    let mut all_col_names: Vec<String> = Vec::new();
    let mut all_col_pos: Vec<Option<i32>> = Vec::new();

    for (name, typ) in tables {
        tnames.push(name);
        ttypes.push(typ);

        if include_columns {
            let cols = fetch_column_names(conn, name)?;
            let n = cols.len() as i32;
            for (i, cname) in cols.into_iter().enumerate() {
                all_col_names.push(cname);
                all_col_pos.push(Some(i as i32 + 1));
            }
            col_offsets.push(col_offsets.last().unwrap() + n);
        } else {
            col_offsets.push(*col_offsets.last().unwrap());
        }
        cons_offsets.push(*cons_offsets.last().unwrap());
    }

    // Build the flattened column struct array.
    let n_cols = all_col_names.len();
    let null_str = |n: usize| -> ArrayRef { Arc::new(StringArray::from(vec![None::<&str>; n])) };
    let null_i16 = |n: usize| -> ArrayRef { Arc::new(Int16Array::from(vec![None::<i16>; n])) };
    let null_i32 = |n: usize| -> ArrayRef { Arc::new(Int32Array::from(vec![None::<i32>; n])) };
    let null_bool = |n: usize| -> ArrayRef { Arc::new(BooleanArray::from(vec![None::<bool>; n])) };

    let col_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("column_name", DataType::Utf8, false)),
            Arc::new(StringArray::from(
                all_col_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            Arc::new(Field::new("ordinal_position", DataType::Int32, true)),
            Arc::new(Int32Array::from(all_col_pos)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("remarks", DataType::Utf8, true)),
            null_str(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_data_type", DataType::Int16, true)),
            null_i16(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_type_name", DataType::Utf8, true)),
            null_str(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_column_size", DataType::Int32, true)),
            null_i32(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_decimal_digits", DataType::Int16, true)),
            null_i16(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_num_prec_radix", DataType::Int16, true)),
            null_i16(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_nullable", DataType::Int16, true)),
            null_i16(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_column_def", DataType::Utf8, true)),
            null_str(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_sql_data_type", DataType::Int16, true)),
            null_i16(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_datetime_sub", DataType::Int16, true)),
            null_i16(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_char_octet_length", DataType::Int32, true)),
            null_i32(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_is_nullable", DataType::Utf8, true)),
            null_str(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_scope_catalog", DataType::Utf8, true)),
            null_str(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_scope_schema", DataType::Utf8, true)),
            null_str(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_scope_table", DataType::Utf8, true)),
            null_str(n_cols),
        ),
        (
            Arc::new(Field::new("xdbc_is_autoincrement", DataType::Boolean, true)),
            null_bool(n_cols),
        ),
        (
            Arc::new(Field::new(
                "xdbc_is_generatedcolumn",
                DataType::Boolean,
                true,
            )),
            null_bool(n_cols),
        ),
    ]);

    let cols_list = ListArray::new(
        Arc::new(Field::new("item", sch::COLUMN_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(col_offsets)),
        Arc::new(col_struct),
        None,
    );

    let cons_list = ListArray::new(
        Arc::new(Field::new("item", sch::CONSTRAINT_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(cons_offsets)),
        Arc::new(helpers::make_empty_cons_struct()),
        None,
    );

    Ok((
        Arc::new(StringArray::from(tnames)),
        Arc::new(StringArray::from(ttypes)),
        Arc::new(cols_list),
        Arc::new(cons_list),
    ))
}

fn fetch_column_names(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let quoted = QuotedIdent::ansi(table);
    let sql = trusted_sql!("PRAGMA table_info({})", quoted);
    let mut stmt = conn
        .prepare(sql.as_str())
        .map_err(|e| Error::internal(e.to_string()))?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|e| Error::internal(e.to_string()))?
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| Error::internal(e.to_string()))?;
    Ok(names)
}
