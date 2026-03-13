//! PostgreSQL catalog metadata methods.
//!
//! Mirrors the SQLite catalog implementation's approach for full schema
//! compliance with the ADBC spec (6-variant Union, proper list nullability).

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray, UInt32Array, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use tokio_postgres::Client;

use adbc::{schema as sch, Error, InfoCode, ObjectDepth, Result};

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

const INFO_ITEMS: &[(InfoCode, u32, i8, usize)] = &[
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
        "PostgreSQL".to_owned(),
        server_version,
        "adbc-postgres".to_owned(),
        env!("CARGO_PKG_VERSION").to_owned(),
        ">=53,<59".to_owned(),
    ];

    let items: Vec<_> = INFO_ITEMS
        .iter()
        .filter(|(code, ..)| codes.is_none_or(|cs| cs.contains(code)))
        .collect();

    let mut names: Vec<u32> = Vec::new();
    let mut type_ids: Vec<i8> = Vec::new();
    let mut value_offsets: Vec<i32> = Vec::new();
    let mut s_idx: i32 = 0;
    let mut i_idx: i32 = 0;

    for (_, name, tid, _) in &items {
        names.push(*name);
        type_ids.push(*tid);
        let offset = match tid {
            0 => {
                let o = s_idx;
                s_idx += 1;
                o
            }
            2 => {
                let o = i_idx;
                i_idx += 1;
                o
            }
            _ => 0,
        };
        value_offsets.push(offset);
    }

    let string_child = Arc::new(StringArray::from(
        items
            .iter()
            .filter(|(.., tid, _)| *tid == 0)
            .map(|(.., idx)| sv[*idx].as_str())
            .collect::<Vec<_>>(),
    )) as Arc<dyn Array>;
    let bool_child = Arc::new(BooleanArray::from(Vec::<bool>::new())) as Arc<dyn Array>;
    let int_child = Arc::new(Int64Array::from(
        items
            .iter()
            .filter(|(.., tid, _)| *tid == 2)
            .map(|(.., idx)| INT_VALS[*idx])
            .collect::<Vec<_>>(),
    )) as Arc<dyn Array>;
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
        "double precision" | "numeric" | "decimal" => DataType::Float64,
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

    // Build table struct arrays.
    let (tname_arr, ttype_arr, tcols_arr, tcons_arr) = build_table_arrays(&tables, include_tables)?;
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

    let schema_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("db_schema_name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some(schema_filter)])) as ArrayRef,
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

    RecordBatch::try_new(
        sch::GET_OBJECTS_SCHEMA.clone(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(schemas_list) as ArrayRef,
        ],
    )
    .map_err(|e| Error::internal(e.to_string()))
}

#[allow(clippy::type_complexity)]
fn build_table_arrays(
    tables: &[(String, String)],
    include_tables: bool,
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

    let tnames: Vec<&str> = tables.iter().map(|(n, _)| n.as_str()).collect();
    let ttypes: Vec<&str> = tables.iter().map(|(_, t)| t.as_str()).collect();
    let cols_list = make_empty_col_list_for(tables.len());
    let cons_list = make_empty_cons_list_for(tables.len());

    Ok((
        Arc::new(StringArray::from(tnames)),
        Arc::new(StringArray::from(ttypes)),
        Arc::new(cols_list),
        Arc::new(cons_list),
    ))
}

// ─────────────────────────────────────────────────────────────
// Arrow helper builders (same as SQLite / DuckDB catalog)
// ─────────────────────────────────────────────────────────────

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
        Arc::new(Field::new("item", DataType::Utf8, false)),
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

fn make_empty_col_list_for(n: usize) -> ListArray {
    let offsets: Vec<i32> = vec![0i32; n + 1];
    ListArray::new(
        Arc::new(Field::new("item", sch::COLUMN_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(make_empty_col_struct()),
        None,
    )
}

fn make_empty_cons_list_for(n: usize) -> ListArray {
    let offsets: Vec<i32> = vec![0i32; n + 1];
    ListArray::new(
        Arc::new(Field::new("item", sch::CONSTRAINT_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(make_empty_cons_struct()),
        None,
    )
}
