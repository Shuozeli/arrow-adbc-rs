//! Static Arrow schemas for ADBC metadata results.
//!
//! These schemas match the ADBC v1.1.0 specification exactly.

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, UnionFields, UnionMode};
use std::sync::Arc;
use std::sync::LazyLock;

// ─────────────────────────────────────────────────────────────
// get_table_types
// ─────────────────────────────────────────────────────────────

pub static TABLE_TYPES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

// ─────────────────────────────────────────────────────────────
// get_info
// ─────────────────────────────────────────────────────────────

pub static GET_INFO_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    // info_value is a dense union of the six value types
    let union_fields = UnionFields::try_new(
        [0i8, 1, 2, 3, 4, 5],
        [
            Field::new("string_value", DataType::Utf8, true),
            Field::new("bool_value", DataType::Boolean, true),
            Field::new("int64_value", DataType::Int64, true),
            Field::new("int32_bitmask", DataType::Int32, true),
            Field::new_list(
                "string_list",
                Field::new_list_field(DataType::Utf8, true),
                true,
            ),
            Field::new_map(
                "int32_to_int32_list_map",
                "entries",
                Field::new("key", DataType::Int32, false),
                Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
                false,
                true,
            ),
        ],
    )
    .unwrap();
    Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new(
            "info_value",
            DataType::Union(union_fields, UnionMode::Dense),
            true,
        ),
    ]))
});

// ─────────────────────────────────────────────────────────────
// get_objects
// ─────────────────────────────────────────────────────────────

/// Schema for the `constraint_column_usage` list element.
pub static USAGE_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
    DataType::Struct(Fields::from(vec![
        Field::new("fk_catalog", DataType::Utf8, true),
        Field::new("fk_db_schema", DataType::Utf8, true),
        Field::new("fk_table", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
    ]))
});

/// Schema for a single constraint.
pub static CONSTRAINT_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
    DataType::Struct(Fields::from(vec![
        Field::new("constraint_name", DataType::Utf8, true),
        Field::new("constraint_type", DataType::Utf8, false),
        Field::new_list(
            "constraint_column_names",
            Field::new_list_field(DataType::Utf8, false),
            false,
        ),
        Field::new_list(
            "constraint_column_usage",
            Field::new("item", USAGE_SCHEMA.clone(), true),
            true,
        ),
    ]))
});

/// Schema for a single column.
pub static COLUMN_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
    DataType::Struct(Fields::from(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, true),
        Field::new("remarks", DataType::Utf8, true),
        Field::new("xdbc_data_type", DataType::Int16, true),
        Field::new("xdbc_type_name", DataType::Utf8, true),
        Field::new("xdbc_column_size", DataType::Int32, true),
        Field::new("xdbc_decimal_digits", DataType::Int16, true),
        Field::new("xdbc_num_prec_radix", DataType::Int16, true),
        Field::new("xdbc_nullable", DataType::Int16, true),
        Field::new("xdbc_column_def", DataType::Utf8, true),
        Field::new("xdbc_sql_data_type", DataType::Int16, true),
        Field::new("xdbc_datetime_sub", DataType::Int16, true),
        Field::new("xdbc_char_octet_length", DataType::Int32, true),
        Field::new("xdbc_is_nullable", DataType::Utf8, true),
        Field::new("xdbc_scope_catalog", DataType::Utf8, true),
        Field::new("xdbc_scope_schema", DataType::Utf8, true),
        Field::new("xdbc_scope_table", DataType::Utf8, true),
        Field::new("xdbc_is_autoincrement", DataType::Boolean, true),
        Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
    ]))
});

/// Schema for a single table.
pub static TABLE_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
    DataType::Struct(Fields::from(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
        Field::new_list(
            "table_columns",
            Field::new("item", COLUMN_SCHEMA.clone(), true),
            true,
        ),
        Field::new_list(
            "table_constraints",
            Field::new("item", CONSTRAINT_SCHEMA.clone(), true),
            true,
        ),
    ]))
});

/// Schema for a single db_schema.
pub static DB_SCHEMA_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
    DataType::Struct(Fields::from(vec![
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new_list(
            "db_schema_tables",
            Field::new("item", TABLE_SCHEMA.clone(), true),
            true,
        ),
    ]))
});

/// Top-level schema returned by `get_objects`.
pub static GET_OBJECTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new_list(
            "catalog_db_schemas",
            Field::new("item", DB_SCHEMA_SCHEMA.clone(), true),
            false,
        ),
    ]))
});
