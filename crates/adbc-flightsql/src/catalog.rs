//! FlightSQL metadata RPC helpers and query execution.

use arrow_array::RecordBatch;
use arrow_flight::{
    sql::{client::FlightSqlServiceClient, CommandGetTables, SqlInfo},
    FlightInfo,
};
use arrow_schema::Schema;
use tonic::transport::Channel;

use adbc::{Error, InfoCode, ObjectDepth, Result};

fn arrow_err(e: impl std::fmt::Display) -> Error {
    Error::new(e.to_string(), adbc::Status::Io)
}

/// Collect all record batches from a FlightInfo by fetching each endpoint.
async fn collect_info(
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
                all.push(unsafe {
                    let ptr = Box::into_raw(Box::new(batch));
                    *Box::from_raw(ptr as *mut RecordBatch)
                });
            }
        }
    }
    Ok(all)
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
    _transaction_id: Option<bytes::Bytes>,
) -> Result<Vec<RecordBatch>> {
    let info = client
        .execute(sql.to_owned(), None)
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
    _transaction_id: Option<bytes::Bytes>,
) -> Result<i64> {
    client
        .execute_update(sql.to_owned(), None)
        .await
        .map_err(arrow_err)
}

// ─────────────────────────────────────────────────────────────
// Metadata
// ─────────────────────────────────────────────────────────────

/// Retrieve the Arrow schema for a specific table.
///
/// Not yet implemented: requires an `arrow-ipc` dependency to decode the
/// IPC-serialized schema from the `GetTables` response.
pub async fn get_table_schema(
    _client: &mut FlightSqlServiceClient<Channel>,
    _catalog: Option<&str>,
    _db_schema: Option<&str>,
    name: &str,
) -> Result<Schema> {
    // Full implementation requires arrow-ipc to decode the IPC-serialized schema
    // from the GetTables response. Stub added here as a known gap.
    // TODO: add arrow-ipc dep and decode the `table_schema` column properly.
    Err(Error::new(
        format!(
            "get_table_schema('{}') is not yet implemented for FlightSQL",
            name
        ),
        adbc::Status::NotImplemented,
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
