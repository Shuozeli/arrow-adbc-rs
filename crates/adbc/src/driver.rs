//! Core ADBC traits: `Driver`, `Database`, `Connection`, `Statement`.
//!
//! All traits are async-first with `Send + Sync` bounds. Drivers that wrap
//! synchronous libraries (e.g. SQLite via `rusqlite`) use
//! `tokio::task::spawn_blocking` internally.

use std::future::Future;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

use crate::error::Result;

// ─────────────────────────────────────────────────────────────
// Option types
// ─────────────────────────────────────────────────────────────

/// A generic option value that can be passed to any ADBC object.
#[derive(Debug, Clone, PartialEq)]
pub enum OptionValue {
    String(String),
    Int(i64),
    Double(f64),
    Bytes(Vec<u8>),
}

impl From<&str> for OptionValue {
    fn from(s: &str) -> Self {
        OptionValue::String(s.to_owned())
    }
}
impl From<String> for OptionValue {
    fn from(s: String) -> Self {
        OptionValue::String(s)
    }
}
impl From<i64> for OptionValue {
    fn from(i: i64) -> Self {
        OptionValue::Int(i)
    }
}
impl From<f64> for OptionValue {
    fn from(f: f64) -> Self {
        OptionValue::Double(f)
    }
}
impl From<Vec<u8>> for OptionValue {
    fn from(b: Vec<u8>) -> Self {
        OptionValue::Bytes(b)
    }
}

/// Options that can be set on a [`Database`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatabaseOption {
    /// Database URI (e.g. `":memory:"` for SQLite, `"grpc://host:port"` for FlightSQL).
    Uri,
    /// Username for authentication.
    Username,
    /// Password for authentication.
    Password,
    /// Any driver-specific option, identified by its key string.
    Other(String),
}

/// Options that can be set on a [`Connection`].
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionOption {
    /// Enable or disable autocommit. Default: `true`.
    AutoCommit(bool),
    /// Open the connection in read-only mode.
    ReadOnly(bool),
    /// Set the transaction isolation level.
    IsolationLevel(IsolationLevel),
    /// Any driver-specific option.
    Other(String, OptionValue),
}

/// Options that can be set on a [`Statement`].
#[derive(Debug, Clone, PartialEq)]
pub enum StatementOption {
    /// Target table name for bulk ingestion.
    TargetTable(String),
    /// Behavior when the target table already exists.
    IngestMode(IngestMode),
    /// Any driver-specific option.
    Other(String, OptionValue),
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    Default,
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Linearizable,
}

/// Bulk ingestion mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestMode {
    /// Create the table; fail if it already exists.
    Create,
    /// Append rows to an existing table; fail if it doesn't exist.
    Append,
    /// Drop and recreate the table.
    Replace,
    /// Create the table if absent, then append.
    CreateAppend,
}

/// Depth of catalog information to return from [`Connection::get_objects`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectDepth {
    /// Return all levels.
    All,
    /// Return catalogs only.
    Catalogs,
    /// Return catalogs and schemas.
    Schemas,
    /// Return catalogs, schemas, and tables.
    Tables,
    /// Return catalogs, schemas, tables, and columns.
    Columns,
}

/// Info codes for [`Connection::get_info`].
///
/// These numeric values follow the ADBC specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum InfoCode {
    /// The name of the vendor/database (e.g. `"SQLite"`).
    VendorName = 0,
    /// The version of the vendor/database.
    VendorVersion = 1,
    /// The Arrow version supported by the vendor.
    VendorArrowVersion = 2,
    /// Whether the vendor supports SQL queries.
    VendorSql = 3,
    /// Whether the vendor supports Substrait plans.
    VendorSubstrait = 4,
    /// The name of the ADBC driver.
    DriverName = 100,
    /// The version of the ADBC driver.
    DriverVersion = 101,
    /// The Arrow version the driver supports.
    DriverArrowVersion = 102,
    /// The ADBC API version the driver implements.
    DriverAdbcVersion = 103,
}

// ─────────────────────────────────────────────────────────────
// Core traits
// ─────────────────────────────────────────────────────────────

/// The top-level entry point for an ADBC driver.
///
/// A [`Driver`] is stateless and cheaply constructible. Use it to create
/// [`Database`] instances.
pub trait Driver: Send + Sync {
    /// The concrete [`Database`] type produced by this driver.
    type DatabaseType: Database;

    /// Create a new database with default options.
    fn new_database(&self) -> impl Future<Output = Result<Self::DatabaseType>> + Send;

    /// Create a new database with the given options.
    fn new_database_with_opts(
        &self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)> + Send,
    ) -> impl Future<Output = Result<Self::DatabaseType>> + Send;
}

/// A handle to a database that can produce [`Connection`]s.
///
/// A [`Database`] holds configuration shared across all connections (URI,
/// credentials, connection-pool settings, etc.). For in-memory databases
/// it also owns the in-memory state itself.
pub trait Database: Send + Sync {
    /// The concrete [`Connection`] type produced by this database.
    type ConnectionType: Connection;

    /// Open a new connection.
    fn new_connection(&self) -> impl Future<Output = Result<Self::ConnectionType>> + Send;

    /// Open a new connection with the given options.
    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption> + Send,
    ) -> impl Future<Output = Result<Self::ConnectionType>> + Send;
}

/// A single logical connection to a database.
///
/// All methods take `&self` to allow sharing via `Arc<C>` across async
/// tasks. Drivers use internal synchronization (`tokio::sync::Mutex`,
/// `std::sync::Mutex`, etc.) as needed.
///
/// # Statement lifetime
///
/// A [`Statement`] created via [`new_statement`](Connection::new_statement)
/// holds an internal reference to the underlying database connection. Only
/// **one statement should be actively executing at a time** on a given
/// connection. Creating multiple statements is allowed (e.g. for reuse),
/// but concurrent execution from separate tasks will serialize on an
/// internal lock and may produce unexpected interleaving of operations.
pub trait Connection: Send + Sync {
    /// The concrete [`Statement`] type produced by this connection.
    type StatementType: Statement;

    /// Create a new [`Statement`] on this connection.
    fn new_statement(&self) -> impl Future<Output = Result<Self::StatementType>> + Send;

    /// Set a connection-level option.
    fn set_option(&self, opt: ConnectionOption) -> impl Future<Output = Result<()>> + Send;

    // ── Transaction management ────────────────────────────────

    /// Commit the current transaction.
    ///
    /// Fails if autocommit is enabled.
    fn commit(&self) -> impl Future<Output = Result<()>> + Send;

    /// Roll back the current transaction.
    ///
    /// Fails if autocommit is enabled.
    fn rollback(&self) -> impl Future<Output = Result<()>> + Send;

    // ── Metadata ─────────────────────────────────────────────

    /// Retrieve the Arrow schema of a table.
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        name: &str,
    ) -> impl Future<Output = Result<Schema>> + Send;

    /// Return the table types supported by this database.
    ///
    /// Result schema: `{ table_type: Utf8 }`.
    fn get_table_types(
        &self,
    ) -> impl Future<Output = Result<Box<dyn RecordBatchReader + Send>>> + Send;

    /// Return database/driver info.
    ///
    /// `codes` filters the result; `None` means return all supported codes.
    fn get_info(
        &self,
        codes: Option<&[InfoCode]>,
    ) -> impl Future<Output = Result<Box<dyn RecordBatchReader + Send>>> + Send;

    /// Return a hierarchical view of the catalog.
    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> impl Future<Output = Result<Box<dyn RecordBatchReader + Send>>> + Send;
}

/// A prepared or ad-hoc query statement.
///
/// A [`Statement`] holds the SQL text (or Substrait plan) and any bound
/// parameters. It can be executed multiple times, though doing so invalidates
/// any prior result set.
///
/// Statement methods take `&mut self` because statements are single-owner
/// and mutation is natural (setting SQL, binding params, executing).
pub trait Statement: Send + Sync {
    /// Set the SQL query string to execute.
    fn set_sql_query(&mut self, sql: &str) -> impl Future<Output = Result<()>> + Send;

    /// Validate and "prepare" the current SQL query on the server,
    /// enabling parameter binding.
    fn prepare(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Execute the current query and return a [`RecordBatchReader`] plus the
    /// number of rows affected (if known).
    fn execute(
        &mut self,
    ) -> impl Future<Output = Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)>> + Send;

    /// Execute the current query or DML statement without returning rows.
    ///
    /// Returns the number of rows affected (if known).
    fn execute_update(&mut self) -> impl Future<Output = Result<i64>> + Send;

    /// Bind a single [`RecordBatch`] of parameters.
    fn bind(&mut self, batch: RecordBatch) -> impl Future<Output = Result<()>> + Send;

    /// Bind a stream of [`RecordBatch`]es of parameters.
    fn bind_stream(
        &mut self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Set a statement-level option.
    fn set_option(&mut self, opt: StatementOption) -> impl Future<Output = Result<()>> + Send;
}
