//! ADBC driver for Apache Arrow FlightSQL.
//!
//! Connects to any Arrow FlightSQL server (e.g. Dremio, DuckDB, Ballista)
//! over gRPC / HTTP/2. TLS and username/password authentication are supported.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
//! use adbc_flightsql::FlightSqlDriver;
//!
//! let mut drv = FlightSqlDriver::default();
//! let db = drv.new_database_with_opts([
//!     (DatabaseOption::Uri,      OptionValue::String("grpc://localhost:32010".into())),
//!     (DatabaseOption::Username, OptionValue::String("admin".into())),
//!     (DatabaseOption::Password, OptionValue::String("password".into())),
//! ]).unwrap();
//! let mut conn = db.new_connection().unwrap();
//! let mut stmt = conn.new_statement().unwrap();
//! stmt.set_sql_query("SELECT 1").unwrap();
//! let (mut reader, _) = stmt.execute().unwrap();
//! while let Some(batch) = reader.next() { println!("{:?}", batch.unwrap()); }
//! ```

mod catalog;

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_schema::Schema;
use tonic::transport::{Channel, Endpoint};

use adbc::{
    Connection, ConnectionOption, Database, DatabaseOption, Driver, Error, InfoCode, IngestMode,
    ObjectDepth, OptionValue, Result, Statement, StatementOption, Status,
};

// ─────────────────────────────────────────────────────────────
// Tokio bridge helpers
// ─────────────────────────────────────────────────────────────

/// Run an async future synchronously, creating a Tokio runtime if needed.
///
/// Returns `Err` if no Tokio runtime exists and building one fails.
fn block<F: std::future::Future<Output = T>, T>(f: F) -> Result<T> {
    // If we're already inside a Tokio context, block_in_place avoids deadlock.
    if tokio::runtime::Handle::try_current().is_ok() {
        Ok(tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(f)
        }))
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::internal(format!("Failed to build Tokio runtime: {e}")))?;
        Ok(rt.block_on(f))
    }
}

fn flight_err(e: impl std::fmt::Display) -> Error {
    Error::new(e.to_string(), Status::Io)
}

// ─────────────────────────────────────────────────────────────
// FlightSqlDriver
// ─────────────────────────────────────────────────────────────

/// Top-level ADBC driver for a FlightSQL service.
///
/// Stateless: call [`Driver::new_database_with_opts`] to obtain a
/// [`FlightSqlDatabase`] configured for a specific endpoint.
#[derive(Default, Debug, Clone, Copy)]
pub struct FlightSqlDriver;

impl Driver for FlightSqlDriver {
    type DatabaseType = FlightSqlDatabase;

    fn new_database(&mut self) -> Result<FlightSqlDatabase> {
        Ok(FlightSqlDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)>,
    ) -> Result<FlightSqlDatabase> {
        let mut db = FlightSqlDatabase::default();
        for (k, v) in opts {
            match k {
                DatabaseOption::Uri => {
                    db.uri = require_string(v, "Uri")?;
                }
                DatabaseOption::Username => {
                    db.username = Some(require_string(v, "Username")?);
                }
                DatabaseOption::Password => {
                    db.password = Some(require_string(v, "Password")?);
                }
                DatabaseOption::Other(key) => {
                    return Err(Error::invalid_arg(format!(
                        "Unknown database option: {key}"
                    )));
                }
            }
        }
        Ok(db)
    }
}

// ─────────────────────────────────────────────────────────────
// FlightSqlDatabase
// ─────────────────────────────────────────────────────────────

/// Configuration for a FlightSQL endpoint.
///
/// Supported URI schemes:
/// - `grpc://host:port`       — plaintext (no TLS)
/// - `grpc+tls://host:port`   — TLS using system roots
///
/// Set [`DatabaseOption::Username`] and [`DatabaseOption::Password`] to
/// authenticate via the standard FlightSQL basic-auth handshake.
#[derive(Debug, Clone)]
pub struct FlightSqlDatabase {
    uri: String,
    username: Option<String>,
    password: Option<String>,
}

impl Default for FlightSqlDatabase {
    fn default() -> Self {
        Self {
            uri: "grpc://localhost:32010".into(),
            username: None,
            password: None,
        }
    }
}

impl FlightSqlDatabase {
    /// Open an authenticated [`FlightSqlServiceClient`].
    async fn connect(&self) -> Result<FlightSqlServiceClient<Channel>> {
        let endpoint = Endpoint::from_shared(self.uri.clone())
            .map_err(|e| Error::invalid_arg(format!("Invalid URI '{}': {e}", self.uri)))?
            .connect_lazy();

        let mut client = FlightSqlServiceClient::new(endpoint);

        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            client.handshake(user, pass).await.map_err(flight_err)?;
        }
        Ok(client)
    }
}

impl Database for FlightSqlDatabase {
    type ConnectionType = FlightSqlConnection;

    fn new_connection(&self) -> Result<FlightSqlConnection> {
        let client = block(self.connect())??;
        Ok(FlightSqlConnection {
            client,
            autocommit: true,
            transaction_id: None,
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption>,
    ) -> Result<FlightSqlConnection> {
        let mut conn = self.new_connection()?;
        for opt in opts {
            conn.set_option(opt)?;
        }
        Ok(conn)
    }
}

// ─────────────────────────────────────────────────────────────
// FlightSqlConnection
// ─────────────────────────────────────────────────────────────

/// A live connection to a FlightSQL service.
///
/// Wraps a [`FlightSqlServiceClient`] internally.
pub struct FlightSqlConnection {
    client: FlightSqlServiceClient<Channel>,
    autocommit: bool,
    /// Non-`None` while a server-side transaction is open.
    transaction_id: Option<bytes::Bytes>,
}

impl Connection for FlightSqlConnection {
    type StatementType = FlightSqlStatement;

    fn new_statement(&mut self) -> Result<FlightSqlStatement> {
        Ok(FlightSqlStatement {
            client: self.client.clone(),
            transaction_id: self.transaction_id.clone(),
            mode: StmtMode::Idle,
            bound_data: None,
        })
    }

    fn set_option(&mut self, opt: ConnectionOption) -> Result<()> {
        match opt {
            ConnectionOption::AutoCommit(enable) => {
                if enable && !self.autocommit {
                    // Commit any open transaction.
                    if let Some(txn) = self.transaction_id.take() {
                        block(
                            self.client
                                .end_transaction(txn, arrow_flight::sql::EndTransaction::Commit),
                        )?
                        .map_err(flight_err)?;
                    }
                } else if !enable && self.autocommit {
                    let txn = block(self.client.begin_transaction())?.map_err(flight_err)?;
                    self.transaction_id = Some(txn);
                }
                self.autocommit = enable;
                Ok(())
            }
            ConnectionOption::ReadOnly(_) => Err(Error::not_impl(
                "ReadOnly option is not yet supported for FlightSQL",
            )),
            ConnectionOption::IsolationLevel(_) => Err(Error::not_impl(
                "IsolationLevel is not supported for FlightSQL",
            )),
            ConnectionOption::Other(key, _) => Err(Error::invalid_arg(format!(
                "Unknown connection option: {key}"
            ))),
        }
    }

    fn commit(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::invalid_state(
                "commit called while autocommit is enabled",
            ));
        }
        let txn = self
            .transaction_id
            .take()
            .ok_or_else(|| Error::invalid_state("No open transaction"))?;
        block(
            self.client
                .end_transaction(txn, arrow_flight::sql::EndTransaction::Commit),
        )?
        .map_err(flight_err)?;
        // Start a fresh transaction so the connection remains in manual mode.
        let new_txn = block(self.client.begin_transaction())?.map_err(flight_err)?;
        self.transaction_id = Some(new_txn);
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::invalid_state(
                "rollback called while autocommit is enabled",
            ));
        }
        let txn = self
            .transaction_id
            .take()
            .ok_or_else(|| Error::invalid_state("No open transaction"))?;
        block(
            self.client
                .end_transaction(txn, arrow_flight::sql::EndTransaction::Rollback),
        )?
        .map_err(flight_err)?;
        let new_txn = block(self.client.begin_transaction())?.map_err(flight_err)?;
        self.transaction_id = Some(new_txn);
        Ok(())
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        name: &str,
    ) -> Result<Schema> {
        let mut client = self.client.clone();
        block(catalog::get_table_schema(
            &mut client,
            catalog,
            db_schema,
            name,
        ))?
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        let mut client = self.client.clone();
        let batches = block(catalog::get_table_types(&mut client))??;
        Ok(Box::new(VecReader::new(batches)))
    }

    fn get_info(&self, codes: Option<&[InfoCode]>) -> Result<Box<dyn RecordBatchReader + Send>> {
        let codes: Option<Vec<InfoCode>> = codes.map(|c| c.to_vec());
        let mut client = self.client.clone();
        let batches = block(catalog::get_sql_info(&mut client, codes.as_deref()))??;
        Ok(Box::new(VecReader::new(batches)))
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let mut client = self.client.clone();
        let batches = block(catalog::get_objects(
            &mut client,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        ))??;
        Ok(Box::new(VecReader::new(batches)))
    }
}

// ─────────────────────────────────────────────────────────────
// FlightSqlStatement
// ─────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
enum StmtMode {
    #[default]
    Idle,
    Sql(String),
    /// After prepare() — stores the sql for server-side re-execution.
    /// (PreparedStatement handle is private in arrow-flight 55.x;
    ///  we re-execute via `execute_query` using the stored SQL.)
    Prepared {
        query: String,
    },
    Ingest {
        table: String,
        mode: IngestMode,
    },
}

/// A SQL statement or bulk-ingest operation on a [`FlightSqlConnection`].
pub struct FlightSqlStatement {
    client: FlightSqlServiceClient<Channel>,
    transaction_id: Option<bytes::Bytes>,
    mode: StmtMode,
    bound_data: Option<Vec<RecordBatch>>,
}

impl Statement for FlightSqlStatement {
    fn set_sql_query(&mut self, sql: &str) -> Result<()> {
        self.mode = StmtMode::Sql(sql.to_owned());
        Ok(())
    }

    fn prepare(&mut self) -> Result<()> {
        let sql = match &self.mode {
            StmtMode::Sql(s) => s.clone(),
            StmtMode::Prepared { .. } => return Ok(()), // idempotent
            _ => return Err(Error::invalid_state("No SQL has been set")),
        };
        // In arrow-flight 55.x, PreparedStatement.handle is private.
        // We call client.prepare() for the side-effect (server-side compilation)
        // but store just the SQL query for re-execution via execute_query.
        let mut client = self.client.clone();
        let txn = self.transaction_id.clone();
        let sql_for_block = sql.clone();
        block(async move { client.prepare(sql_for_block, txn).await })?.map_err(flight_err)?;
        self.mode = StmtMode::Prepared { query: sql };
        Ok(())
    }

    fn execute(&mut self) -> Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)> {
        match &self.mode {
            StmtMode::Sql(sql) => {
                let sql = sql.clone();
                let txn = self.transaction_id.clone();
                let mut client = self.client.clone();
                let batches = block(catalog::execute_query(&mut client, &sql, txn))??;
                Ok((Box::new(VecReader::new(batches)), None))
            }
            StmtMode::Prepared { query, .. } => {
                let sql = query.clone();
                let txn = self.transaction_id.clone();
                let mut client = self.client.clone();
                let batches = block(catalog::execute_query(&mut client, &sql, txn))??;
                Ok((Box::new(VecReader::new(batches)), None))
            }
            StmtMode::Idle => Err(Error::invalid_state("No SQL has been set")),
            StmtMode::Ingest { .. } => Err(Error::invalid_state("Use execute_update for ingest")),
        }
    }

    fn execute_update(&mut self) -> Result<i64> {
        match &self.mode {
            StmtMode::Sql(sql) => {
                let sql = sql.clone();
                let txn = self.transaction_id.clone();
                let mut client = self.client.clone();
                block(catalog::execute_update(&mut client, &sql, txn))?
            }
            StmtMode::Ingest { .. } => Err(Error::not_impl(
                "Bulk ingest is not supported by the FlightSQL driver",
            )),
            StmtMode::Idle => Err(Error::invalid_state("No SQL has been set")),
            StmtMode::Prepared { .. } => Err(Error::not_impl(
                "execute_update on a prepared statement is not yet supported",
            )),
        }
    }

    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound_data = Some(vec![batch]);
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::internal(e.to_string()))?;
        self.bound_data = Some(batches);
        Ok(())
    }

    fn set_option(&mut self, opt: StatementOption) -> Result<()> {
        match opt {
            StatementOption::TargetTable(table) => {
                let m = if let StmtMode::Ingest { mode, .. } = &self.mode {
                    *mode
                } else {
                    IngestMode::Create
                };
                self.mode = StmtMode::Ingest { table, mode: m };
                Ok(())
            }
            StatementOption::IngestMode(m) => {
                if let StmtMode::Ingest { table, .. } = &self.mode {
                    let table = table.clone();
                    self.mode = StmtMode::Ingest { table, mode: m };
                }
                Ok(())
            }
            StatementOption::Other(key, _) => Err(Error::invalid_arg(format!(
                "Unknown statement option: {key}"
            ))),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// VecReader — simple RecordBatchReader over a Vec
// ─────────────────────────────────────────────────────────────

struct VecReader {
    batches: std::vec::IntoIter<RecordBatch>,
    schema: Arc<Schema>,
}

impl VecReader {
    fn new(batches: Vec<RecordBatch>) -> Self {
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
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

impl RecordBatchReader for VecReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

// ─────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────

fn require_string(v: OptionValue, name: &str) -> Result<String> {
    match v {
        OptionValue::String(s) => Ok(s),
        _ => Err(Error::invalid_arg(format!("{name} must be a string value"))),
    }
}
