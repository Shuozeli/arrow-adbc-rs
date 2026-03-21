//! ADBC driver for Apache Arrow FlightSQL.
//!
//! Connects to any Arrow FlightSQL server (e.g. Dremio, DuckDB, Ballista)
//! over gRPC / HTTP/2. TLS and username/password authentication are supported.
//!
//! # Quick start
//!
//! ```rust,no_run
//! # #[tokio::main] async fn main() {
//! use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
//! use adbc_flightsql::FlightSqlDriver;
//!
//! let drv = FlightSqlDriver::default();
//! let db = drv.new_database_with_opts([
//!     (DatabaseOption::Uri,      OptionValue::String("grpc://localhost:32010".into())),
//!     (DatabaseOption::Username, OptionValue::String("admin".into())),
//!     (DatabaseOption::Password, OptionValue::String("password".into())),
//! ]).await.unwrap();
//! let conn = db.new_connection().await.unwrap();
//! let mut stmt = conn.new_statement().await.unwrap();
//! stmt.set_sql_query("SELECT 1").await.unwrap();
//! let (mut reader, _) = stmt.execute().await.unwrap();
//! while let Some(batch) = reader.next() { println!("{:?}", batch.unwrap()); }
//! # }
//! ```

mod catalog;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_flight::sql::client::{FlightSqlServiceClient, PreparedStatement};
use arrow_schema::Schema;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use adbc::{
    helpers::{require_string, set_statement_option, VecReader},
    Connection, ConnectionOption, Database, DatabaseOption, Driver, Error, InfoCode, ObjectDepth,
    OptionValue, Result, Statement, StatementMode, StatementOption, Status,
};

fn flight_err(e: impl std::fmt::Display) -> Error {
    Error::new(e.to_string(), Status::Io)
}

// ─────────────────────────────────────────────────────────────
// FlightSqlDriver
// ─────────────────────────────────────────────────────────────

/// Top-level ADBC driver for a FlightSQL service.
#[derive(Default, Debug, Clone, Copy)]
pub struct FlightSqlDriver;

impl Driver for FlightSqlDriver {
    type DatabaseType = FlightSqlDatabase;

    async fn new_database(&self) -> Result<FlightSqlDatabase> {
        Ok(FlightSqlDatabase::default())
    }

    async fn new_database_with_opts(
        &self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)> + Send,
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
/// - `grpc://host:port`       -- plaintext (no TLS)
/// - `grpc+tls://host:port`   -- TLS using system roots
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
        let use_tls = self.uri.starts_with("grpc+tls://");

        let transport_uri = if use_tls {
            self.uri.replacen("grpc+tls://", "https://", 1)
        } else {
            self.uri.replacen("grpc://", "http://", 1)
        };

        #[allow(unused_mut)]
        let mut endpoint = Endpoint::from_shared(transport_uri)
            .map_err(|e| Error::invalid_arg(format!("Invalid URI '{}': {e}", self.uri)))?;

        if use_tls {
            #[cfg(feature = "tls")]
            {
                endpoint = endpoint
                    .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())
                    .map_err(|e| Error::io(format!("TLS configuration failed: {e}")))?;
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(Error::invalid_arg(
                    "grpc+tls:// requires the 'tls' feature to be enabled on adbc-flightsql",
                ));
            }
        }

        let channel = endpoint.connect_lazy();
        let mut client = FlightSqlServiceClient::new(channel);

        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            client.handshake(user, pass).await.map_err(flight_err)?;
        }
        Ok(client)
    }
}

impl Database for FlightSqlDatabase {
    type ConnectionType = FlightSqlConnection;

    async fn new_connection(&self) -> Result<FlightSqlConnection> {
        let client = self.connect().await?;
        Ok(FlightSqlConnection {
            inner: Mutex::new(FlightSqlInner {
                client,
                autocommit: true,
                transaction_id: None,
            }),
        })
    }

    async fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption> + Send,
    ) -> Result<FlightSqlConnection> {
        let conn = self.new_connection().await?;
        let opts: Vec<_> = opts.into_iter().collect();
        for opt in opts {
            conn.set_option(opt).await?;
        }
        Ok(conn)
    }
}

// ─────────────────────────────────────────────────────────────
// FlightSqlConnection
// ─────────────────────────────────────────────────────────────

struct FlightSqlInner {
    client: FlightSqlServiceClient<Channel>,
    autocommit: bool,
    transaction_id: Option<bytes::Bytes>,
}

/// A live connection to a FlightSQL service.
pub struct FlightSqlConnection {
    inner: Mutex<FlightSqlInner>,
}

impl Connection for FlightSqlConnection {
    type StatementType = FlightSqlStatement;

    async fn new_statement(&self) -> Result<FlightSqlStatement> {
        let inner = self.inner.lock().await;
        Ok(FlightSqlStatement {
            client: inner.client.clone(),
            transaction_id: inner.transaction_id.clone(),
            mode: StatementMode::Idle,
            bound_data: None,
            prepared_stmt: None,
        })
    }

    async fn set_option(&self, opt: ConnectionOption) -> Result<()> {
        let mut inner = self.inner.lock().await;
        match opt {
            ConnectionOption::AutoCommit(enable) => {
                if enable && !inner.autocommit {
                    if let Some(txn) = inner.transaction_id.take() {
                        inner
                            .client
                            .end_transaction(txn, arrow_flight::sql::EndTransaction::Commit)
                            .await
                            .map_err(flight_err)?;
                    }
                } else if !enable && inner.autocommit {
                    let txn = inner.client.begin_transaction().await.map_err(flight_err)?;
                    inner.transaction_id = Some(txn);
                }
                inner.autocommit = enable;
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

    async fn commit(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.autocommit {
            return Err(Error::invalid_state(
                "commit called while autocommit is enabled",
            ));
        }
        let txn = inner
            .transaction_id
            .take()
            .ok_or_else(|| Error::invalid_state("No open transaction"))?;
        inner
            .client
            .end_transaction(txn, arrow_flight::sql::EndTransaction::Commit)
            .await
            .map_err(flight_err)?;
        let new_txn = inner.client.begin_transaction().await.map_err(flight_err)?;
        inner.transaction_id = Some(new_txn);
        Ok(())
    }

    async fn rollback(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.autocommit {
            return Err(Error::invalid_state(
                "rollback called while autocommit is enabled",
            ));
        }
        let txn = inner
            .transaction_id
            .take()
            .ok_or_else(|| Error::invalid_state("No open transaction"))?;
        inner
            .client
            .end_transaction(txn, arrow_flight::sql::EndTransaction::Rollback)
            .await
            .map_err(flight_err)?;
        let new_txn = inner.client.begin_transaction().await.map_err(flight_err)?;
        inner.transaction_id = Some(new_txn);
        Ok(())
    }

    async fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        name: &str,
    ) -> Result<Schema> {
        let mut client = self.inner.lock().await.client.clone();
        catalog::get_table_schema(&mut client, catalog, db_schema, name).await
    }

    async fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        let mut client = self.inner.lock().await.client.clone();
        let batches = catalog::get_table_types(&mut client).await?;
        Ok(Box::new(VecReader::new(batches)))
    }

    async fn get_info(
        &self,
        codes: Option<&[InfoCode]>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let codes: Option<Vec<InfoCode>> = codes.map(|c| c.to_vec());
        let mut client = self.inner.lock().await.client.clone();
        let batches = catalog::get_sql_info(&mut client, codes.as_deref()).await?;
        Ok(Box::new(VecReader::new(batches)))
    }

    async fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let mut client = self.inner.lock().await.client.clone();
        let batches = catalog::get_objects(
            &mut client,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )
        .await?;
        Ok(Box::new(VecReader::new(batches)))
    }
}

// ─────────────────────────────────────────────────────────────
// FlightSqlStatement
// ─────────────────────────────────────────────────────────────

/// A SQL statement or bulk-ingest operation on a [`FlightSqlConnection`].
pub struct FlightSqlStatement {
    client: FlightSqlServiceClient<Channel>,
    transaction_id: Option<bytes::Bytes>,
    mode: StatementMode,
    bound_data: Option<Vec<RecordBatch>>,
    /// Server-side prepared statement handle, set by [`Statement::prepare`].
    prepared_stmt: Option<PreparedStatement<Channel>>,
}

impl Statement for FlightSqlStatement {
    async fn set_sql_query(&mut self, sql: &str) -> Result<()> {
        self.mode = StatementMode::Sql(sql.to_owned());
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        let sql = match &self.mode {
            StatementMode::Sql(s) => s.clone(),
            StatementMode::Prepared(_) => return Ok(()),
            _ => return Err(Error::invalid_state("No SQL has been set")),
        };
        let mut client = self.client.clone();
        let txn = self.transaction_id.clone();
        let handle = client.prepare(sql.clone(), txn).await.map_err(flight_err)?;
        self.prepared_stmt = Some(handle);
        self.mode = StatementMode::Prepared(sql);
        Ok(())
    }

    async fn execute(&mut self) -> Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)> {
        match &self.mode {
            StatementMode::Sql(sql) => {
                let sql = sql.clone();
                let txn = self.transaction_id.clone();
                let mut client = self.client.clone();
                let batches = catalog::execute_query(&mut client, &sql, txn).await?;
                Ok((Box::new(VecReader::new(batches)), None))
            }
            StatementMode::Prepared(_) => {
                let mut prepared = self.prepared_stmt.take().ok_or_else(|| {
                    Error::invalid_state("Prepared statement handle missing; call prepare() first")
                })?;
                if let Some(batches) = &self.bound_data {
                    if let Some(batch) = batches.first() {
                        catalog::set_prepared_parameters(&mut prepared, batch)?;
                    }
                }
                let info = prepared.execute().await.map_err(flight_err)?;
                let mut client = self.client.clone();
                let batches = catalog::collect_info(&mut client, info).await?;
                self.prepared_stmt = Some(prepared);
                Ok((Box::new(VecReader::new(batches)), None))
            }
            StatementMode::Idle => Err(Error::invalid_state("No SQL has been set")),
            StatementMode::Ingest { .. } => {
                Err(Error::invalid_state("Use execute_update for ingest"))
            }
        }
    }

    async fn execute_update(&mut self) -> Result<i64> {
        match &self.mode {
            StatementMode::Sql(sql) => {
                let sql = sql.clone();
                let txn = self.transaction_id.clone();
                let mut client = self.client.clone();
                catalog::execute_update(&mut client, &sql, txn).await
            }
            StatementMode::Prepared(_) => {
                let mut prepared = self.prepared_stmt.take().ok_or_else(|| {
                    Error::invalid_state("Prepared statement handle missing; call prepare() first")
                })?;
                if let Some(batches) = &self.bound_data {
                    if let Some(batch) = batches.first() {
                        catalog::set_prepared_parameters(&mut prepared, batch)?;
                    }
                }
                let count = prepared.execute_update().await.map_err(flight_err)?;
                self.prepared_stmt = Some(prepared);
                Ok(count)
            }
            StatementMode::Ingest { .. } => Err(Error::not_impl(
                "Bulk ingest is not supported by the FlightSQL driver",
            )),
            StatementMode::Idle => Err(Error::invalid_state("No SQL has been set")),
        }
    }

    async fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound_data = Some(vec![batch]);
        Ok(())
    }

    async fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::internal(e.to_string()))?;
        self.bound_data = Some(batches);
        Ok(())
    }

    async fn set_option(&mut self, opt: StatementOption) -> Result<()> {
        set_statement_option(&mut self.mode, opt)
    }
}
