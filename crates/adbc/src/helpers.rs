//! Shared utility types used across ADBC drivers.
//!
//! These are extracted here to avoid duplicating the same implementations
//! in every driver crate.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray, UInt32Array, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, Schema};

use crate::driver::{IngestMode, OptionValue, StatementMode};
use crate::error::{Error, Result};
use crate::schema as sch;
use crate::StatementOption;

// ─────────────────────────────────────────────────────────────
// OneBatch — single-batch RecordBatchReader
// ─────────────────────────────────────────────────────────────

/// A [`RecordBatchReader`](arrow_array::RecordBatchReader) that yields exactly one batch.
pub struct OneBatch {
    batch: Option<RecordBatch>,
    schema: Arc<Schema>,
}

impl OneBatch {
    /// Wrap a single [`RecordBatch`] into a reader.
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for OneBatch {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl arrow_array::RecordBatchReader for OneBatch {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

// ─────────────────────────────────────────────────────────────
// VecReader — RecordBatchReader over a Vec<RecordBatch>
// ─────────────────────────────────────────────────────────────

/// A [`RecordBatchReader`](arrow_array::RecordBatchReader) that yields batches from a `Vec`.
pub struct VecReader {
    batches: std::vec::IntoIter<RecordBatch>,
    schema: Arc<Schema>,
}

impl VecReader {
    /// Create a reader from a vector of batches.
    ///
    /// If `batches` is empty the schema will be [`Schema::empty()`].
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        Self {
            batches: batches.into_iter(),
            schema,
        }
    }
}

impl Iterator for VecReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

impl arrow_array::RecordBatchReader for VecReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

// ─────────────────────────────────────────────────────────────
// collect_reader — drain a RecordBatchReader into one RecordBatch
// ─────────────────────────────────────────────────────────────

/// Collect all batches from a [`RecordBatchReader`](arrow_array::RecordBatchReader) into a
/// single [`RecordBatch`].
///
/// Returns an error if any individual batch fails or if concatenation fails.
pub fn collect_reader(
    reader: Box<dyn arrow_array::RecordBatchReader + Send>,
) -> Result<RecordBatch> {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::io(e.to_string()))?;
    arrow_select::concat::concat_batches(&schema, &batches).map_err(|e| Error::io(e.to_string()))
}

// ─────────────────────────────────────────────────────────────
// extract_first_row — shared bound-parameter extraction
// ─────────────────────────────────────────────────────────────

/// Extract parameters from the first row of the first bound batch using a
/// driver-specific conversion function `f`.
///
/// Returns `None` when there are no batches, no rows, or the conversion fails.
pub fn extract_first_row<T>(
    bound_data: &Option<Vec<RecordBatch>>,
    f: impl FnOnce(&RecordBatch, usize) -> Result<T>,
) -> Option<T> {
    let batches = bound_data.as_ref()?;
    let batch = batches.first()?;
    if batch.num_rows() == 0 {
        return None;
    }
    f(batch, 0).ok()
}

// ─────────────────────────────────────────────────────────────
// require_string — extract a String from OptionValue
// ─────────────────────────────────────────────────────────────

/// Extract a [`String`] from an [`OptionValue`], returning an error if it is
/// not the `String` variant.
pub fn require_string(v: OptionValue, name: &str) -> Result<String> {
    match v {
        OptionValue::String(s) => Ok(s),
        _ => Err(Error::invalid_arg(format!("{name} must be a string value"))),
    }
}

// ─────────────────────────────────────────────────────────────
// set_statement_option — shared logic for Statement::set_option
// ─────────────────────────────────────────────────────────────

/// Apply a [`StatementOption`] to a [`StatementMode`].
///
/// This encapsulates the shared `TargetTable` / `IngestMode` logic that is
/// identical across all driver implementations.
pub fn set_statement_option(mode: &mut StatementMode, opt: StatementOption) -> Result<()> {
    match opt {
        StatementOption::TargetTable(table) => {
            let mode_val = if let StatementMode::Ingest { mode, .. } = mode {
                *mode
            } else {
                IngestMode::Create
            };
            *mode = StatementMode::Ingest {
                table,
                mode: mode_val,
            };
            Ok(())
        }
        StatementOption::IngestMode(m) => {
            if let StatementMode::Ingest { table, .. } = mode {
                let table = table.clone();
                *mode = StatementMode::Ingest { table, mode: m };
            } else {
                return Err(Error::invalid_state(
                    "IngestMode can only be set after TargetTable",
                ));
            }
            Ok(())
        }
        StatementOption::Other(key, _) => Err(Error::invalid_arg(format!(
            "Unknown statement option: {key}"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────
// Shared empty Arrow struct builders for catalog metadata
// ─────────────────────────────────────────────────────────────

/// Build an empty column-metadata struct array (19 fields matching the ADBC spec).
pub fn make_empty_col_struct() -> StructArray {
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

/// Build an empty constraint struct array matching the ADBC spec.
pub fn make_empty_cons_struct() -> StructArray {
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

/// Build an empty column list array (0 elements).
pub fn make_empty_col_list() -> ListArray {
    ListArray::new(
        Arc::new(Field::new("item", sch::COLUMN_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
        Arc::new(make_empty_col_struct()),
        None,
    )
}

/// Build an empty constraint list array (0 elements).
pub fn make_empty_cons_list() -> ListArray {
    ListArray::new(
        Arc::new(Field::new("item", sch::CONSTRAINT_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
        Arc::new(make_empty_cons_struct()),
        None,
    )
}

/// Build an empty column list array with `n` offset entries (all pointing to offset 0).
pub fn make_empty_col_list_for(n: usize) -> ListArray {
    let offsets: Vec<i32> = vec![0i32; n + 1];
    ListArray::new(
        Arc::new(Field::new("item", sch::COLUMN_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(make_empty_col_struct()),
        None,
    )
}

/// Build an empty constraint list array with `n` offset entries (all pointing to offset 0).
pub fn make_empty_cons_list_for(n: usize) -> ListArray {
    let offsets: Vec<i32> = vec![0i32; n + 1];
    ListArray::new(
        Arc::new(Field::new("item", sch::CONSTRAINT_SCHEMA.clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(make_empty_cons_struct()),
        None,
    )
}

/// Build an empty string list array (0 elements).
pub fn make_empty_str_list() -> ListArray {
    ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32])),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        None,
    )
}

/// Build an empty int32-to-int32-list map array (0 elements).
pub fn make_empty_i32_map() -> Result<arrow_array::MapArray> {
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
// build_get_info_batch — shared get_info union construction
// ─────────────────────────────────────────────────────────────

/// A single entry in a driver's get_info table.
///
/// Fields: `(InfoCode, numeric_name, union_type_id, index_within_child)`
pub type InfoItem = (crate::InfoCode, u32, i8, usize);

/// Build a `get_info` RecordBatch from driver-specific data values.
///
/// This encapsulates the union array construction logic that is shared
/// across all three driver catalog implementations.
///
/// - `info_items`: static table of (InfoCode, numeric_name, union_type_id, child_index)
/// - `string_vals`: string values for union type_id 0
/// - `bool_vals`: boolean values for union type_id 1
/// - `int_vals`: i64 values for union type_id 2
/// - `codes`: optional filter on which InfoCodes to include
pub fn build_get_info_batch(
    info_items: &[InfoItem],
    string_vals: &[&str],
    bool_vals: &[bool],
    int_vals: &[i64],
    codes: Option<&[crate::InfoCode]>,
) -> Result<RecordBatch> {
    let items: Vec<_> = info_items
        .iter()
        .filter(|(code, ..)| codes.is_none_or(|cs| cs.contains(code)))
        .collect();

    let mut names: Vec<u32> = Vec::new();
    let mut type_ids: Vec<i8> = Vec::new();
    let mut value_offsets: Vec<i32> = Vec::new();
    let mut s_idx: i32 = 0;
    let mut b_idx: i32 = 0;
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

    let string_child = Arc::new(StringArray::from(
        items
            .iter()
            .filter(|(.., tid, _)| *tid == 0)
            .map(|(.., idx)| string_vals[*idx])
            .collect::<Vec<_>>(),
    )) as Arc<dyn Array>;
    let bool_child = Arc::new(BooleanArray::from(
        items
            .iter()
            .filter(|(.., tid, _)| *tid == 1)
            .map(|(.., idx)| bool_vals[*idx])
            .collect::<Vec<_>>(),
    )) as Arc<dyn Array>;
    let int_child = Arc::new(Int64Array::from(
        items
            .iter()
            .filter(|(.., tid, _)| *tid == 2)
            .map(|(.., idx)| int_vals[*idx])
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
// build_table_arrays — shared table struct construction
// ─────────────────────────────────────────────────────────────

/// The return type from [`build_table_arrays_simple`].
pub type TableArrays = (
    Arc<dyn Array>,
    Arc<dyn Array>,
    Arc<dyn Array>,
    Arc<dyn Array>,
);

/// Build the table struct arrays for `get_objects`, returning empty lists for
/// columns and constraints.
///
/// This is the common case used by Postgres and MySQL which don't yet populate
/// per-table column details in `get_objects`.
pub fn build_table_arrays_simple(
    tables: &[(String, String)],
    include_tables: bool,
) -> Result<TableArrays> {
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

/// Assemble a complete `get_objects` RecordBatch from pre-built table arrays.
///
/// This encapsulates the boilerplate of building the table struct, wrapping it
/// in a tables list, nesting inside a db_schema struct, and producing the
/// final catalog-level batch. Used by Postgres and MySQL drivers.
pub fn build_get_objects_batch(
    table_arrays: TableArrays,
    schema_name: &str,
    include_schemas: bool,
    include_tables: bool,
) -> Result<RecordBatch> {
    let (tname_arr, ttype_arr, tcols_arr, tcons_arr) = table_arrays;
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
            Arc::new(StringArray::from(vec![Some(schema_name)])) as ArrayRef,
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
