//! Pure-Rust PostgreSQL ADBC driver.
//!
//! # Quick start
//!
//! ```rust,no_run
//! # #[tokio::main] async fn main() {
//! use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
//! use adbc_postgres::PostgresDriver;
//!
//! let drv = PostgresDriver::default();
//! let db = drv.new_database_with_opts([
//!     (DatabaseOption::Uri, OptionValue::String(
//!         "host=localhost port=5432 user=alice password=secret dbname=mydb".into(),
//!     )),
//! ]).await.unwrap();
//! let conn = db.new_connection().await.unwrap();
//! let mut stmt = conn.new_statement().await.unwrap();
//! stmt.set_sql_query("SELECT 42 AS answer").await.unwrap();
//! let (mut reader, _) = stmt.execute().await.unwrap();
//! while let Some(batch) = reader.next() {
//!     println!("{:?}", batch.unwrap());
//! }
//! # }
//! ```

mod catalog;
mod convert;

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use tokio::sync::Mutex;

use adbc::helpers::{require_string, OneBatch};
use adbc::sql::SqlLiteral;
use adbc::{
    trusted_sql, Connection, ConnectionOption, Database, DatabaseOption, Driver, Error, InfoCode,
    IngestMode, IsolationLevel, ObjectDepth, OptionValue, Result, Statement, StatementOption,
};
use catalog::{get_info_batch, get_objects_batch, get_table_schema_impl, get_table_types_batch};
use convert::{pg_columns_to_schema, rows_to_batch};

// ─────────────────────────────────────────────────────────────
// PostgresDriver
// ─────────────────────────────────────────────────────────────

/// The top-level ADBC driver for PostgreSQL.
#[derive(Default, Debug, Clone, Copy)]
pub struct PostgresDriver;

impl Driver for PostgresDriver {
    type DatabaseType = PostgresDatabase;

    async fn new_database(&self) -> Result<PostgresDatabase> {
        Ok(PostgresDatabase::default())
    }

    async fn new_database_with_opts(
        &self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)> + Send,
    ) -> Result<PostgresDatabase> {
        let mut db = PostgresDatabase::default();
        for (k, v) in opts {
            match k {
                DatabaseOption::Uri => {
                    db.conn_str = require_string(v, "Uri")?;
                }
                DatabaseOption::Username => {
                    let u = require_string(v, "Username")?;
                    db.conn_str
                        .push_str(&format!(" user='{}'", escape_libpq_value(&u)));
                }
                DatabaseOption::Password => {
                    let p = require_string(v, "Password")?;
                    db.conn_str
                        .push_str(&format!(" password='{}'", escape_libpq_value(&p)));
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
// PostgresDatabase
// ─────────────────────────────────────────────────────────────

/// Configuration for a PostgreSQL database.
///
/// The `Uri` option should be a libpq-style connection string, e.g.:
/// `"host=localhost port=5432 user=alice password=secret dbname=mydb"`
/// or a `postgres://` URL.
#[derive(Debug, Clone)]
pub struct PostgresDatabase {
    conn_str: String,
}

impl Default for PostgresDatabase {
    fn default() -> Self {
        Self {
            conn_str: "host=localhost port=5432 dbname=postgres".into(),
        }
    }
}

impl PostgresDatabase {
    #[cfg(feature = "tls")]
    async fn connect(&self) -> Result<tokio_postgres::Client> {
        let config: tokio_postgres::Config =
            self.conn_str.parse().map_err(|e: tokio_postgres::Error| {
                Error::invalid_arg(format!("Invalid connection string: {e}"))
            })?;

        let certs = rustls_native_certs::load_native_certs();
        let mut root_store = rustls::RootCertStore::empty();
        for cert in certs {
            let _ = root_store.add(cert);
        }

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

        let (client, connection) = config
            .connect(tls)
            .await
            .map_err(|e| Error::io(e.to_string()))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    #[cfg(not(feature = "tls"))]
    async fn connect(&self) -> Result<tokio_postgres::Client> {
        let config: tokio_postgres::Config =
            self.conn_str.parse().map_err(|e: tokio_postgres::Error| {
                Error::invalid_arg(format!("Invalid connection string: {e}"))
            })?;

        let (client, connection) = config
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::io(e.to_string()))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }
}

impl Database for PostgresDatabase {
    type ConnectionType = PostgresConnection;

    async fn new_connection(&self) -> Result<PostgresConnection> {
        let client = self.connect().await?;
        Ok(PostgresConnection {
            client: Arc::new(client),
            state: Mutex::new(PgState {
                autocommit: true,
                in_transaction: false,
            }),
        })
    }

    async fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption> + Send,
    ) -> Result<PostgresConnection> {
        let conn = self.new_connection().await?;
        let opts: Vec<_> = opts.into_iter().collect();
        for opt in opts {
            conn.set_option(opt).await?;
        }
        Ok(conn)
    }
}

// ─────────────────────────────────────────────────────────────
// PostgresConnection
// ─────────────────────────────────────────────────────────────

struct PgState {
    autocommit: bool,
    in_transaction: bool,
}

/// A live connection to a PostgreSQL server.
///
/// The underlying [`tokio_postgres::Client`] handles pipelining internally,
/// so no external mutex is needed around it. A separate [`Mutex`] guards
/// the transaction state.
pub struct PostgresConnection {
    client: Arc<tokio_postgres::Client>,
    state: Mutex<PgState>,
}

impl PostgresConnection {
    async fn exec(&self, sql: &str) -> Result<()> {
        self.client
            .batch_execute(sql)
            .await
            .map_err(|e| Error::internal(e.to_string()))
    }
}

impl Connection for PostgresConnection {
    type StatementType = PostgresStatement;

    async fn new_statement(&self) -> Result<PostgresStatement> {
        Ok(PostgresStatement::new(Arc::clone(&self.client)))
    }

    async fn set_option(&self, opt: ConnectionOption) -> Result<()> {
        match opt {
            ConnectionOption::AutoCommit(enable) => {
                let mut s = self.state.lock().await;
                if enable && !s.autocommit {
                    if s.in_transaction {
                        self.exec("COMMIT").await?;
                        s.in_transaction = false;
                    }
                } else if !enable && s.autocommit {
                    self.exec("BEGIN").await?;
                    s.in_transaction = true;
                }
                s.autocommit = enable;
                Ok(())
            }
            ConnectionOption::ReadOnly(ro) => {
                let mode = if ro {
                    SqlLiteral("READ ONLY")
                } else {
                    SqlLiteral("READ WRITE")
                };
                let sql = trusted_sql!("SET SESSION CHARACTERISTICS AS TRANSACTION {}", mode);
                self.exec(sql.as_str()).await
            }
            ConnectionOption::IsolationLevel(lvl) => {
                let sql_lvl = match lvl {
                    IsolationLevel::ReadUncommitted => SqlLiteral("READ UNCOMMITTED"),
                    IsolationLevel::ReadCommitted => SqlLiteral("READ COMMITTED"),
                    IsolationLevel::RepeatableRead => SqlLiteral("REPEATABLE READ"),
                    IsolationLevel::Serializable | IsolationLevel::Linearizable => {
                        SqlLiteral("SERIALIZABLE")
                    }
                    IsolationLevel::Default => return Ok(()),
                };
                let sql = trusted_sql!(
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {}",
                    sql_lvl
                );
                self.exec(sql.as_str()).await
            }
            ConnectionOption::Other(key, _) => Err(Error::invalid_arg(format!(
                "Unknown connection option: {key}"
            ))),
        }
    }

    async fn commit(&self) -> Result<()> {
        let mut s = self.state.lock().await;
        if s.autocommit {
            return Err(Error::invalid_state(
                "commit called while autocommit is enabled",
            ));
        }
        self.exec("COMMIT").await?;
        self.exec("BEGIN").await?;
        s.in_transaction = true;
        Ok(())
    }

    async fn rollback(&self) -> Result<()> {
        let mut s = self.state.lock().await;
        if s.autocommit {
            return Err(Error::invalid_state(
                "rollback called while autocommit is enabled",
            ));
        }
        self.exec("ROLLBACK").await?;
        self.exec("BEGIN").await?;
        s.in_transaction = true;
        Ok(())
    }

    async fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        name: &str,
    ) -> Result<Schema> {
        get_table_schema_impl(&self.client, catalog, db_schema, name).await
    }

    async fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        let batch = get_table_types_batch()?;
        Ok(Box::new(OneBatch::new(batch)))
    }

    async fn get_info(
        &self,
        codes: Option<&[InfoCode]>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let codes: Option<Vec<InfoCode>> = codes.map(|c| c.to_vec());
        let batch = get_info_batch(&self.client, codes.as_deref()).await?;
        Ok(Box::new(OneBatch::new(batch)))
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
        let batch = get_objects_batch(
            &self.client,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )
        .await?;
        Ok(Box::new(OneBatch::new(batch)))
    }
}

// ─────────────────────────────────────────────────────────────
// PostgresStatement
// ─────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
enum Mode {
    #[default]
    Idle,
    Sql(String),
    Prepared(String),
    Ingest {
        table: String,
        mode: IngestMode,
    },
}

/// A query or bulk-ingest statement on a [`PostgresConnection`].
pub struct PostgresStatement {
    client: Arc<tokio_postgres::Client>,
    mode: Mode,
    bound_data: Option<Vec<RecordBatch>>,
}

impl PostgresStatement {
    fn new(client: Arc<tokio_postgres::Client>) -> Self {
        Self {
            client,
            mode: Mode::Idle,
            bound_data: None,
        }
    }
}

impl Statement for PostgresStatement {
    async fn set_sql_query(&mut self, sql: &str) -> Result<()> {
        self.mode = Mode::Sql(sql.to_owned());
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        match &self.mode {
            Mode::Sql(sql) => {
                let sql = sql.clone();
                self.client
                    .prepare(&sql)
                    .await
                    .map_err(|e| Error::invalid_arg(e.to_string()))?;
                self.mode = Mode::Prepared(sql);
                Ok(())
            }
            Mode::Prepared(_) => Ok(()),
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
            Mode::Ingest { .. } => Err(Error::invalid_state("Cannot prepare an ingest statement")),
        }
    }

    async fn execute(&mut self) -> Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)> {
        match &self.mode {
            Mode::Sql(sql) | Mode::Prepared(sql) => {
                let sql = sql.clone();
                let stmt = self
                    .client
                    .prepare(&sql)
                    .await
                    .map_err(|e| Error::invalid_arg(e.to_string()))?;
                let schema = pg_columns_to_schema(stmt.columns());
                let rows = self
                    .client
                    .query(&stmt, &[])
                    .await
                    .map_err(|e| Error::io(e.to_string()))?;
                let batch = rows_to_batch(&rows, schema)?;
                Ok((Box::new(OneBatch::new(batch)), None))
            }
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
            Mode::Ingest { .. } => Err(Error::invalid_state("Use execute_update for ingest")),
        }
    }

    async fn execute_update(&mut self) -> Result<i64> {
        match &self.mode {
            Mode::Sql(sql) | Mode::Prepared(sql) => {
                let sql = sql.clone();
                let n = self
                    .client
                    .execute(sql.as_str(), &[])
                    .await
                    .map_err(|e| Error::internal(e.to_string()))?;
                Ok(n as i64)
            }
            Mode::Ingest { table, mode } => {
                let table = table.clone();
                let mode = *mode;
                let batches = self.bound_data.take().unwrap_or_default();
                if batches.is_empty() {
                    return Err(Error::invalid_state("No data bound for ingest"));
                }
                convert::ingest_batches(&self.client, &table, mode, &batches).await
            }
            Mode::Idle => Err(Error::invalid_state("No SQL or ingest target has been set")),
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
        match opt {
            StatementOption::TargetTable(table) => {
                let mode_val = if let Mode::Ingest { mode, .. } = &self.mode {
                    *mode
                } else {
                    IngestMode::Create
                };
                self.mode = Mode::Ingest {
                    table,
                    mode: mode_val,
                };
                Ok(())
            }
            StatementOption::IngestMode(m) => {
                if let Mode::Ingest { table, .. } = &self.mode {
                    let table = table.clone();
                    self.mode = Mode::Ingest { table, mode: m };
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
}

/// Escape a value for use inside a single-quoted libpq connection string parameter.
///
/// Per the libpq docs, within single quotes the only escapes are `\'` and `\\`.
fn escape_libpq_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out
}
