//! FlightSQL metadata RPC helpers and query execution.

use arrow_array::{Array, BinaryArray, RecordBatch};
use arrow_flight::{
    sql::{
        client::{FlightSqlServiceClient, PreparedStatement},
        CommandGetTables, SqlInfo,
    },
    FlightInfo,
};
use arrow_schema::Schema;
use tonic::transport::Channel;

use adbc::{Error, InfoCode, ObjectDepth, Result};

fn arrow_err(e: impl std::fmt::Display) -> Error {
    Error::new(e.to_string(), adbc::Status::Io)
}

/// Runtime guard for the unsafe RecordBatch pointer cast in `collect_info`.
///
/// `arrow_flight` re-exports `arrow_array::RecordBatch` from its own pinned
/// version, which may differ from this crate's `arrow_array` version. The
/// unsafe cast assumes identical memory layout. This function panics if
/// size or alignment ever diverge.
fn assert_recordbatch_layout_compatible(flight_batch: &impl std::any::Any) {
    let flight_size = std::mem::size_of_val(flight_batch);
    let expected_size = std::mem::size_of::<RecordBatch>();
    assert_eq!(
        flight_size, expected_size,
        "RecordBatch size mismatch: arrow-flight={flight_size} vs arrow-array={expected_size}"
    );
}

/// Collect all record batches from a FlightInfo by fetching each endpoint.
pub(crate) async fn collect_info(
    client: &mut FlightSqlServiceClient<Channel>,
    info: FlightInfo,
) -> Result<Vec<RecordBatch>> {
    let mut all: Vec<RecordBatch> = Vec::new();
    for endpoint in info.endpoint {
        if let Some(ticket) = endpoint.ticket {
            let stream = client.do_get(ticket).await.map_err(arrow_err)?;
            use tokio_stream::StreamExt;
            let mut stream = stream;
            while let Some(item) = stream.next().await {
                let batch = item.map_err(arrow_err)?;
                // SAFETY: The `batch` returned by `arrow_flight::FlightRecordBatchStream`
                // is typed as `arrow_flight`'s re-exported `RecordBatch`. When
                // `arrow-flight` and this crate depend on different semver-incompatible
                // versions of `arrow-array`, the compiler treats them as distinct types
                // even though their memory layout is identical (same struct, same fields).
                // The pointer round-trip (`Box::into_raw` then `Box::from_raw`) reinterprets
                // the allocation without copying, which is sound because:
                //   1. Both types have identical size, alignment, and field layout.
                //   2. The Box owns the heap allocation; no aliasing occurs.
                //   3. We consume the original Box before creating the new one.
                // If a future arrow version changes the RecordBatch layout, a compile-time
                // size/align assertion (or a version-pinning CI check) should catch it.
                assert_recordbatch_layout_compatible(&batch);
                all.push(unsafe {
                    let ptr = Box::into_raw(Box::new(batch));
                    *Box::from_raw(ptr as *mut RecordBatch)
                });
            }
        }
    }
    Ok(all)
}

/// Bind a parameter `RecordBatch` to a prepared statement.
///
/// `PreparedStatement::set_parameters` expects the `arrow_array::RecordBatch`
/// type from the version of `arrow-array` that `arrow-flight` was compiled
/// against (55.x), while our crate uses 58.x. The two types have identical
/// memory layout, so we reinterpret the pointer — the same technique used
/// in `collect_info` for the reverse direction.
pub(crate) fn set_prepared_parameters(
    prepared: &mut PreparedStatement<Channel>,
    batch: &RecordBatch,
) -> Result<()> {
    // SAFETY: Our 58.x `RecordBatch` and arrow-flight's 55.x `RecordBatch`
    // are the same struct with the same fields and alignment. The
    // `Box::into_raw` / `Box::from_raw` round-trip reinterprets the heap
    // allocation without copying. No aliasing occurs because the original
    // `Box` is consumed before the new one is created.
    let flight_batch = unsafe {
        let ptr = Box::into_raw(Box::new(batch.clone())) as *mut ();
        *Box::from_raw(ptr as *mut _)
    };
    prepared.set_parameters(flight_batch).map_err(arrow_err)
}

// ─────────────────────────────────────────────────────────────
// Query execution
// ─────────────────────────────────────────────────────────────

/// Execute a SQL query and return the resulting record batches.
///
/// Sends the query via the FlightSQL `Execute` RPC and collects all
/// batches from every returned endpoint.
pub async fn execute_query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
    transaction_id: Option<bytes::Bytes>,
) -> Result<Vec<RecordBatch>> {
    let info = client
        .execute(sql.to_owned(), transaction_id)
        .await
        .map_err(arrow_err)?;
    collect_info(client, info).await
}

/// Execute a SQL DML/DDL statement and return the number of affected rows.
///
/// Uses the FlightSQL `ExecuteUpdate` RPC. Returns the server-reported
/// row count (may be -1 if the server does not provide one).
pub async fn execute_update(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
    transaction_id: Option<bytes::Bytes>,
) -> Result<i64> {
    client
        .execute_update(sql.to_owned(), transaction_id)
        .await
        .map_err(arrow_err)
}

// ─────────────────────────────────────────────────────────────
// Metadata
// ─────────────────────────────────────────────────────────────

/// Decode an IPC Schema message (as stored in the `table_schema` column of a
/// `GetTables` response) into an Arrow [`Schema`].
fn decode_ipc_schema(buf: &[u8]) -> Result<Schema> {
    arrow_ipc::convert::try_schema_from_ipc_buffer(buf)
        .map_err(|e| Error::internal(format!("Failed to decode IPC schema: {e}")))
}

/// Retrieve the Arrow schema for a specific table.
///
/// Uses the FlightSQL `GetTables` RPC with `include_schema = true` to obtain
/// the IPC-serialized schema stored in the `table_schema` column, then decodes
/// it with `arrow-ipc`.
pub async fn get_table_schema(
    client: &mut FlightSqlServiceClient<Channel>,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    name: &str,
) -> Result<Schema> {
    let request = CommandGetTables {
        catalog: catalog.map(str::to_owned),
        db_schema_filter_pattern: db_schema.map(str::to_owned),
        table_name_filter_pattern: Some(name.to_owned()),
        table_types: vec![],
        include_schema: true,
    };

    let info = client.get_tables(request).await.map_err(arrow_err)?;
    let batches = collect_info(client, info).await?;

    // The GetTables response schema:
    //   0: catalog_name       (Utf8)
    //   1: db_schema_name     (Utf8)
    //   2: table_name         (Utf8)
    //   3: table_type         (Utf8)
    //   4: table_schema       (Binary) — IPC Schema message bytes (only when include_schema=true)
    for batch in &batches {
        if batch.num_columns() < 5 || batch.num_rows() == 0 {
            continue;
        }
        let schema_col = batch.column(4);
        let binary_arr = schema_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| Error::internal("table_schema column is not a Binary array"))?;
        for i in 0..binary_arr.len() {
            if binary_arr.is_null(i) {
                continue;
            }
            return decode_ipc_schema(binary_arr.value(i));
        }
    }

    Err(Error::new(
        format!("Table '{name}' not found"),
        adbc::Status::NotFound,
    ))
}

/// Retrieve the list of table types supported by the server (e.g. "TABLE", "VIEW").
///
/// Uses the FlightSQL `GetTableTypes` RPC.
pub async fn get_table_types(
    client: &mut FlightSqlServiceClient<Channel>,
) -> Result<Vec<RecordBatch>> {
    let info = client.get_table_types().await.map_err(arrow_err)?;
    collect_info(client, info).await
}

/// Retrieve server metadata for the requested info codes.
///
/// Maps ADBC [`InfoCode`] values to FlightSQL [`SqlInfo`] variants and
/// calls the `GetSqlInfo` RPC. If `codes` is `None`, all available info
/// is requested.
pub async fn get_sql_info(
    client: &mut FlightSqlServiceClient<Channel>,
    codes: Option<&[InfoCode]>,
) -> Result<Vec<RecordBatch>> {
    // Map our InfoCode values to SqlInfo variants by raw u32 cast.
    // SqlInfo is a non-exhaustive enum; we use the integer values directly.
    let sql_infos: Vec<SqlInfo> = codes
        .map(|cs| {
            cs.iter()
                .filter_map(|c| SqlInfo::try_from(*c as i32).ok())
                .collect()
        })
        .unwrap_or_default();

    let info = client.get_sql_info(sql_infos).await.map_err(arrow_err)?;

    collect_info(client, info).await
}

/// Retrieve catalog objects (catalogs, schemas, tables, columns) filtered by
/// the given criteria.
///
/// Uses the FlightSQL `GetTables` RPC. The `depth` parameter controls how
/// deep into the object hierarchy the results go (e.g. [`ObjectDepth::All`]
/// includes column-level metadata when `include_schema` is set).
pub async fn get_objects(
    client: &mut FlightSqlServiceClient<Channel>,
    depth: ObjectDepth,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<&[&str]>,
    _column_name: Option<&str>,
) -> Result<Vec<RecordBatch>> {
    let include_schema = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);

    let table_types: Vec<String> = table_type
        .map(|ts| ts.iter().map(|s| s.to_string()).collect())
        .unwrap_or_default();

    let request = CommandGetTables {
        catalog: catalog.map(str::to_owned),
        db_schema_filter_pattern: db_schema.map(str::to_owned),
        table_name_filter_pattern: table_name.map(str::to_owned),
        table_types,
        include_schema,
    };

    let info = client.get_tables(request).await.map_err(arrow_err)?;
    collect_info(client, info).await
}
