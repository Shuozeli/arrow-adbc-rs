//! Shared utility types used across ADBC drivers.
//!
//! These are extracted here to avoid duplicating the same implementations
//! in every driver crate.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema};

use crate::driver::OptionValue;
use crate::error::{Error, Result};

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
