//! Encode Arrow RecordBatches into FlightData messages.
//!
//! This module manually encodes using our workspace's `arrow-ipc` version and
//! constructs `FlightData` from raw bytes. This avoids version mismatches
//! between the workspace's `arrow-array` and `arrow-flight`'s transitive
//! `arrow-array` dependency.

use arrow_array::RecordBatch;
use arrow_flight::FlightData;
use arrow_ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::Schema;

/// Convert a schema and record batches into a sequence of [`FlightData`] messages.
///
/// The first message contains the IPC-encoded schema. Subsequent messages each
/// contain one IPC-encoded record batch. Dictionary messages (if any) are
/// interleaved before the batches that reference them.
pub fn batches_to_flight_data(
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<Vec<FlightData>, arrow_schema::ArrowError> {
    let options = IpcWriteOptions::default();
    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let mut compression = CompressionContext::default();

    // Encode schema.
    let schema_encoded =
        data_gen.schema_to_bytes_with_dictionary_tracker(schema, &mut dictionary_tracker, &options);
    let schema_flight_data = encoded_data_to_flight_data(schema_encoded);

    let mut dictionaries = Vec::new();
    let mut batch_flight_data = Vec::new();

    for batch in batches {
        let (encoded_dicts, encoded_batch) =
            data_gen.encode(batch, &mut dictionary_tracker, &options, &mut compression)?;
        dictionaries.extend(encoded_dicts.into_iter().map(encoded_data_to_flight_data));
        batch_flight_data.push(encoded_data_to_flight_data(encoded_batch));
    }

    let mut result = Vec::with_capacity(1 + dictionaries.len() + batch_flight_data.len());
    result.push(schema_flight_data);
    result.extend(dictionaries);
    result.extend(batch_flight_data);
    Ok(result)
}

/// Convert IPC [`EncodedData`](arrow_ipc::writer::EncodedData) into a
/// [`FlightData`] message by moving raw bytes without version-specific
/// type dependencies.
fn encoded_data_to_flight_data(encoded: arrow_ipc::writer::EncodedData) -> FlightData {
    FlightData {
        flight_descriptor: None,
        data_header: encoded.ipc_message.into(),
        data_body: encoded.arrow_data.into(),
        app_metadata: bytes::Bytes::new(),
    }
}
