//! ADBC catalog/metadata methods for SQLite.
//!
//! Implements `get_info`, `get_table_types`, `get_table_schema`, and
//! `get_objects` by querying SQLite's `sqlite_master` table.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray, UInt32Array, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use rusqlite::Connection;

use adbc::sql::QuotedIdent;
use adbc::{schema as sch, trusted_sql, Error, InfoCode, ObjectDepth, Result};

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
const INFO_ITEMS: &[(InfoCode, u32, i8, usize)] = &[
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

/// Returns the string values for get_info, built at runtime because
/// `rusqlite::version()` is not a const fn.
fn string_vals() -> Vec<&'static str> {
    vec![
        "SQLite",                  // VendorName  (index 0)
        rusqlite::version(),       // VendorVersion (1)
        "",                        // VendorArrowVersion (2)
        "adbc-sqlite",             // DriverName (3)
        env!("CARGO_PKG_VERSION"), // DriverVersion (4)
        "",                        // DriverArrowVersion (5)
    ]
}
const BOOL_VALS: &[bool] = &[true, false]; // VendorSql, VendorSubstrait
const INT_VALS: &[i64] = &[1_001_000]; // DriverAdbcVersion = ADBC 1.1.0

pub fn get_info_batch(codes: Option<&[InfoCode]>) -> Result<RecordBatch> {
    // Filter by requested codes.
    let items: Vec<_> = INFO_ITEMS
        .iter()
        .filter(|(code, ..)| codes.is_none_or(|cs| cs.contains(code)))
        .collect();

    // Build per-row arrays for name + union type/offset.
    let mut names: Vec<u32> = Vec::new();
    let mut type_ids: Vec<i8> = Vec::new();

    let mut s_idx: i32 = 0;
    let mut b_idx: i32 = 0;
    let mut i_idx: i32 = 0;
    let mut value_offsets: Vec<i32> = Vec::new();

    for (_, name, tid, _) in &items {
        names.push(*name);
        type_ids.push(*tid);
        let offset = match tid {
            0 => {
                let o = s_idx;
                s_idx += 1;
                o
            }
            1 => {
                let o = b_idx;
                b_idx += 1;
                o
            }
            2 => {
                let o = i_idx;
                i_idx += 1;
                o
            }
            _ => return Err(Error::internal(format!("unexpected union type_id: {tid}"))),
        };
        value_offsets.push(offset);
    }

    // Build child arrays (only include filtered items' values).
    let sv = string_vals();
    let string_child = Arc::new(StringArray::from(
        items
            .iter()
            .filter(|(.., tid, _)| *tid == 0)
            .map(|(.., idx)| sv[*idx])
            .collect::<Vec<_>>(),
    )) as Arc<dyn Array>;
    let bool_child = Arc::new(BooleanArray::from(
        items
            .iter()
            .filter(|(.., tid, _)| *tid == 1)
            .map(|(.., idx)| BOOL_VALS[*idx])
            .collect::<Vec<_>>(),
    )) as Arc<dyn Array>;
    let int_child = Arc::new(Int64Array::from(
        items
            .iter()
            .filter(|(.., tid, _)| *tid == 2)
            .map(|(.., idx)| INT_VALS[*idx])
            .collect::<Vec<_>>(),
    )) as Arc<dyn Array>;

    // Empty children for union types we don't use.
    let int32_child = Arc::new(Int32Array::from(Vec::<i32>::new())) as Arc<dyn Array>;
    let str_list_child = Arc::new(make_empty_str_list()) as Arc<dyn Array>;
    let map_child = Arc::new(make_empty_i32_map()?) as Arc<dyn Array>;

    let union_fields = match sch::GET_INFO_SCHEMA.field(1).data_type() {
        DataType::Union(uf, _) => uf.clone(),
        dt => {
            return Err(Error::internal(format!(
                "expected Union type in GET_INFO_SCHEMA, got {dt:?}"
            )))
        }
    };

    let value_arr = UnionArray::try_new(
        union_fields,
        type_ids.into_iter().collect::<ScalarBuffer<i8>>(),
        Some(value_offsets.into_iter().collect::<ScalarBuffer<i32>>()),
        vec![
            string_child,
            bool_child,
            int_child,
            int32_child,
            str_list_child,
            map_child,
        ],
    )
    .map_err(|e| Error::internal(e.to_string()))?;

    RecordBatch::try_new(
        sch::GET_INFO_SCHEMA.clone(),
        vec![
            Arc::new(UInt32Array::from(names)) as ArrayRef,
            Arc::new(value_arr) as ArrayRef,
        ],
    )
    .map_err(|e| Error::internal(e.to_string()))
}

fn make_empty_str_list() -> ListArray {
    ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        None,
    )
}

fn make_empty_i32_map() -> Result<arrow_array::MapArray> {
    let key_field = Field::new("key", DataType::Int32, false);
    let val_field = Field::new_list("value", Field::new_list_field(DataType::Int32, true), true);
    let struct_arr = StructArray::new(
        vec![key_field, val_field].into(),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                None,
            )) as ArrayRef,
        ],
        None,
    );
    let entries_field = Field::new_struct(
        "entries",
        vec![
            Field::new("key", DataType::Int32, false),
            Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
        ],
        false,
    );
    arrow_array::MapArray::try_new(
        Arc::new(entries_field),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
        struct_arr,
        None,
        false,
    )
    .map_err(|e| Error::internal(e.to_string()))
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

    let num_tables = if include_tables { tname_arr.len() } else { 0 };
    let table_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("table_name", DataType::Utf8, false)),
            tname_arr as ArrayRef,
        ),
        (
            Arc::new(Field::new("table_type", DataType::Utf8, false)),
            ttype_arr as ArrayRef,
        ),
        (
            Arc::new(Field::new_list(
                "table_columns",
                Arc::new(Field::new("item", sch::COLUMN_SCHEMA.clone(), true)),
                true,
            )),
            tcols_arr as ArrayRef,
        ),
        (
            Arc::new(Field::new_list(
                "table_constraints",
                Arc::new(Field::new("item", sch::CONSTRAINT_SCHEMA.clone(), true)),
                true,
            )),
            tcons_arr as ArrayRef,
        ),
    ]);

    let tables_list = ListArray::new(
        Arc::new(Field::new("item", sch::TABLE_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, num_tables as i32])),
        Arc::new(table_struct),
        None,
    );

    // ── db_schema struct ─────────────────────────────────────
    let schema_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("db_schema_name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("main")])) as ArrayRef,
        ),
        (
            Arc::new(Field::new_list(
                "db_schema_tables",
                Arc::new(Field::new("item", sch::TABLE_SCHEMA.clone(), true)),
                true,
            )),
            Arc::new(tables_list) as ArrayRef,
        ),
    ]);

    let n_schemas: i32 = if include_schemas { 1 } else { 0 };
    let schemas_list = ListArray::new(
        Arc::new(Field::new("item", sch::DB_SCHEMA_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, n_schemas])),
        Arc::new(schema_struct),
        None,
    );

    // ── top-level batch ──────────────────────────────────────
    RecordBatch::try_new(
        sch::GET_OBJECTS_SCHEMA.clone(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef, // catalog_name = NULL
            Arc::new(schemas_list) as ArrayRef,
        ],
    )
    .map_err(|e| Error::internal(e.to_string()))
}

#[allow(clippy::type_complexity)]
fn build_table_arrays(
    conn: &Connection,
    tables: &[(String, String)],
    include_tables: bool,
    include_columns: bool,
) -> Result<(
    Arc<dyn Array>,
    Arc<dyn Array>,
    Arc<dyn Array>,
    Arc<dyn Array>,
)> {
    if !include_tables || tables.is_empty() {
        return Ok((
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(make_empty_col_list()),
            Arc::new(make_empty_cons_list()),
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
        Arc::new(make_empty_cons_struct()),
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

fn make_empty_col_list() -> ListArray {
    ListArray::new(
        Arc::new(Field::new("item", sch::COLUMN_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
        Arc::new(make_empty_col_struct()),
        None,
    )
}

fn make_empty_cons_list() -> ListArray {
    ListArray::new(
        Arc::new(Field::new("item", sch::CONSTRAINT_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
        Arc::new(make_empty_cons_struct()),
        None,
    )
}

fn make_empty_col_struct() -> StructArray {
    let ns = || -> ArrayRef { Arc::new(StringArray::from(Vec::<&str>::new())) };
    let ni16 = || -> ArrayRef { Arc::new(Int16Array::from(Vec::<i16>::new())) };
    let ni32 = || -> ArrayRef { Arc::new(Int32Array::from(Vec::<i32>::new())) };
    let nbool = || -> ArrayRef { Arc::new(BooleanArray::from(Vec::<bool>::new())) };
    StructArray::from(vec![
        (
            Arc::new(Field::new("column_name", DataType::Utf8, false)),
            ns(),
        ),
        (
            Arc::new(Field::new("ordinal_position", DataType::Int32, true)),
            ni32(),
        ),
        (Arc::new(Field::new("remarks", DataType::Utf8, true)), ns()),
        (
            Arc::new(Field::new("xdbc_data_type", DataType::Int16, true)),
            ni16(),
        ),
        (
            Arc::new(Field::new("xdbc_type_name", DataType::Utf8, true)),
            ns(),
        ),
        (
            Arc::new(Field::new("xdbc_column_size", DataType::Int32, true)),
            ni32(),
        ),
        (
            Arc::new(Field::new("xdbc_decimal_digits", DataType::Int16, true)),
            ni16(),
        ),
        (
            Arc::new(Field::new("xdbc_num_prec_radix", DataType::Int16, true)),
            ni16(),
        ),
        (
            Arc::new(Field::new("xdbc_nullable", DataType::Int16, true)),
            ni16(),
        ),
        (
            Arc::new(Field::new("xdbc_column_def", DataType::Utf8, true)),
            ns(),
        ),
        (
            Arc::new(Field::new("xdbc_sql_data_type", DataType::Int16, true)),
            ni16(),
        ),
        (
            Arc::new(Field::new("xdbc_datetime_sub", DataType::Int16, true)),
            ni16(),
        ),
        (
            Arc::new(Field::new("xdbc_char_octet_length", DataType::Int32, true)),
            ni32(),
        ),
        (
            Arc::new(Field::new("xdbc_is_nullable", DataType::Utf8, true)),
            ns(),
        ),
        (
            Arc::new(Field::new("xdbc_scope_catalog", DataType::Utf8, true)),
            ns(),
        ),
        (
            Arc::new(Field::new("xdbc_scope_schema", DataType::Utf8, true)),
            ns(),
        ),
        (
            Arc::new(Field::new("xdbc_scope_table", DataType::Utf8, true)),
            ns(),
        ),
        (
            Arc::new(Field::new("xdbc_is_autoincrement", DataType::Boolean, true)),
            nbool(),
        ),
        (
            Arc::new(Field::new(
                "xdbc_is_generatedcolumn",
                DataType::Boolean,
                true,
            )),
            nbool(),
        ),
    ])
}

fn make_empty_cons_struct() -> StructArray {
    let ns = || -> ArrayRef { Arc::new(StringArray::from(Vec::<&str>::new())) };
    let empty_str_list = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, false)), // non-null items, matching schema
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        None,
    );
    let usage_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("fk_catalog", DataType::Utf8, true)),
            ns(),
        ),
        (
            Arc::new(Field::new("fk_db_schema", DataType::Utf8, true)),
            ns(),
        ),
        (
            Arc::new(Field::new("fk_table", DataType::Utf8, false)),
            ns(),
        ),
        (
            Arc::new(Field::new("fk_column_name", DataType::Utf8, false)),
            ns(),
        ),
    ]);
    let usage_list = ListArray::new(
        Arc::new(Field::new("item", sch::USAGE_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
        Arc::new(usage_struct),
        None,
    );
    StructArray::from(vec![
        (
            Arc::new(Field::new("constraint_name", DataType::Utf8, true)),
            ns(),
        ),
        (
            Arc::new(Field::new("constraint_type", DataType::Utf8, false)),
            ns(),
        ),
        (
            Arc::new(Field::new_list(
                "constraint_column_names",
                Field::new_list_field(DataType::Utf8, false),
                false,
            )),
            Arc::new(empty_str_list) as ArrayRef,
        ),
        (
            Arc::new(Field::new_list(
                "constraint_column_usage",
                Arc::new(Field::new("item", sch::USAGE_SCHEMA.clone(), true)),
                true,
            )),
            Arc::new(usage_list) as ArrayRef,
        ),
    ])
}
