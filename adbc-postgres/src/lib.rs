//! Pure-Rust PostgreSQL ADBC driver.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
//! use adbc_postgres::PostgresDriver;
//!
//! let mut drv = PostgresDriver::default();
//! let db = drv.new_database_with_opts([
//!     (DatabaseOption::Uri, OptionValue::String(
//!         "host=localhost port=5432 user=alice password=secret dbname=mydb".into(),
//!     )),
//! ]).unwrap();
//! let mut conn = db.new_connection().unwrap();
//! let mut stmt = conn.new_statement().unwrap();
//! stmt.set_sql_query("SELECT 42 AS answer").unwrap();
//! let (mut reader, _) = stmt.execute().unwrap();
//! while let Some(batch) = reader.next() {
//!     println!("{:?}", batch.unwrap());
//! }
//! ```

mod catalog;
mod convert;

use std::sync::{Arc, Mutex};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

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

    fn new_database(&mut self) -> Result<PostgresDatabase> {
        Ok(PostgresDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)>,
    ) -> Result<PostgresDatabase> {
        let mut db = PostgresDatabase::default();
        for (k, v) in opts {
            match k {
                DatabaseOption::Uri => {
                    db.conn_str = require_string(v, "Uri")?;
                }
                DatabaseOption::Username => {
                    let u = require_string(v, "Username")?;
                    db.conn_str.push_str(&format!(" user={u}"));
                }
                DatabaseOption::Password => {
                    let p = require_string(v, "Password")?;
                    db.conn_str.push_str(&format!(" password={p}"));
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

impl Database for PostgresDatabase {
    type ConnectionType = PostgresConnection;

    fn new_connection(&self) -> Result<PostgresConnection> {
        PostgresConnection::open(&self.conn_str)
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption>,
    ) -> Result<PostgresConnection> {
        let mut conn = self.new_connection()?;
        for opt in opts {
            conn.set_option(opt)?;
        }
        Ok(conn)
    }
}

// ─────────────────────────────────────────────────────────────
// PostgresConnection
// ─────────────────────────────────────────────────────────────

/// A live connection to a PostgreSQL server.
///
/// Wraps a [`postgres::Client`] in `Arc<Mutex<…>>` so metadata (`&self`) methods
/// can lock it safely.
pub struct PostgresConnection {
    inner: Arc<Mutex<postgres::Client>>,
    autocommit: bool,
    in_transaction: bool,
}

impl PostgresConnection {
    fn open(conn_str: &str) -> Result<Self> {
        let client = postgres::Client::connect(conn_str, postgres::NoTls)
            .map_err(|e| Error::io(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            autocommit: true,
            in_transaction: false,
        })
    }

    fn lock_inner(&self) -> Result<std::sync::MutexGuard<'_, postgres::Client>> {
        self.inner
            .lock()
            .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))
    }

    fn exec(&self, sql: &str) -> Result<()> {
        self.lock_inner()?
            .batch_execute(sql)
            .map_err(|e| Error::internal(e.to_string()))
    }
}

impl Connection for PostgresConnection {
    type StatementType = PostgresStatement;

    fn new_statement(&mut self) -> Result<PostgresStatement> {
        Ok(PostgresStatement::new(Arc::clone(&self.inner)))
    }

    fn set_option(&mut self, opt: ConnectionOption) -> Result<()> {
        match opt {
            ConnectionOption::AutoCommit(enable) => {
                if enable && !self.autocommit {
                    if self.in_transaction {
                        self.exec("COMMIT")?;
                        self.in_transaction = false;
                    }
                } else if !enable && self.autocommit {
                    self.exec("BEGIN")?;
                    self.in_transaction = true;
                }
                self.autocommit = enable;
                Ok(())
            }
            ConnectionOption::ReadOnly(ro) => {
                let mode = if ro {
                    SqlLiteral("READ ONLY")
                } else {
                    SqlLiteral("READ WRITE")
                };
                let sql = trusted_sql!("SET SESSION CHARACTERISTICS AS TRANSACTION {}", mode);
                self.exec(sql.as_str())
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
                self.exec(sql.as_str())
            }
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
        self.exec("COMMIT")?;
        self.exec("BEGIN")
    }

    fn rollback(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::invalid_state(
                "rollback called while autocommit is enabled",
            ));
        }
        self.exec("ROLLBACK")?;
        self.exec("BEGIN")
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        name: &str,
    ) -> Result<Schema> {
        let mut client = self.lock_inner()?;
        get_table_schema_impl(&mut client, catalog, db_schema, name)
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        let batch = get_table_types_batch()?;
        Ok(Box::new(OneBatch::new(batch)))
    }

    fn get_info(&self, codes: Option<&[InfoCode]>) -> Result<Box<dyn RecordBatchReader + Send>> {
        let mut client = self.lock_inner()?;
        let batch = get_info_batch(&mut client, codes)?;
        Ok(Box::new(OneBatch::new(batch)))
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
        let mut client = self.lock_inner()?;
        let batch = get_objects_batch(
            &mut client,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )?;
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
    conn: Arc<Mutex<postgres::Client>>,
    mode: Mode,
    bound_data: Option<Vec<RecordBatch>>,
}

impl PostgresStatement {
    fn new(conn: Arc<Mutex<postgres::Client>>) -> Self {
        Self {
            conn,
            mode: Mode::Idle,
            bound_data: None,
        }
    }

    fn lock_conn(&self) -> Result<std::sync::MutexGuard<'_, postgres::Client>> {
        self.conn
            .lock()
            .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))
    }
}

impl Statement for PostgresStatement {
    fn set_sql_query(&mut self, sql: &str) -> Result<()> {
        self.mode = Mode::Sql(sql.to_owned());
        Ok(())
    }

    fn prepare(&mut self) -> Result<()> {
        match &self.mode {
            Mode::Sql(sql) => {
                // Validate by preparing — postgres::Client::prepare checks syntax.
                let sql = sql.clone();
                self.lock_conn()?
                    .prepare(&sql)
                    .map_err(|e| Error::invalid_arg(e.to_string()))?;
                self.mode = Mode::Prepared(sql);
                Ok(())
            }
            Mode::Prepared(_) => Ok(()),
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
            Mode::Ingest { .. } => Err(Error::invalid_state("Cannot prepare an ingest statement")),
        }
    }

    fn execute(&mut self) -> Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)> {
        match &self.mode {
            Mode::Sql(sql) | Mode::Prepared(sql) => {
                let sql = sql.clone();
                let mut client = self.lock_conn()?;
                let stmt = client
                    .prepare(&sql)
                    .map_err(|e| Error::invalid_arg(e.to_string()))?;
                let schema = pg_columns_to_schema(stmt.columns());
                let rows = client
                    .query(&stmt, &[])
                    .map_err(|e| Error::io(e.to_string()))?;
                let batch = rows_to_batch(&rows, schema)?;
                Ok((Box::new(OneBatch::new(batch)), None))
            }
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
            Mode::Ingest { .. } => Err(Error::invalid_state("Use execute_update for ingest")),
        }
    }

    fn execute_update(&mut self) -> Result<i64> {
        match &self.mode {
            Mode::Sql(sql) | Mode::Prepared(sql) => {
                let sql = sql.clone();
                let n = self
                    .lock_conn()?
                    .execute(sql.as_str(), &[])
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
                let mut client = self.lock_conn()?;
                convert::ingest_batches(&mut client, &table, mode, &batches)
            }
            Mode::Idle => Err(Error::invalid_state("No SQL or ingest target has been set")),
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
// OneBatch — single-batch RecordBatchReader
// ─────────────────────────────────────────────────────────────

pub(crate) struct OneBatch {
    batch: Option<RecordBatch>,
    schema: Arc<Schema>,
}

impl OneBatch {
    pub(crate) fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for OneBatch {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for OneBatch {
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
