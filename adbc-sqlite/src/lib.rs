//! Pure-Rust SQLite ADBC driver.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
//! use adbc_sqlite::SqliteDriver;
//!
//! let mut drv = SqliteDriver::default();
//! let db = drv.new_database_with_opts([
//!     (DatabaseOption::Uri, OptionValue::String(":memory:".into())),
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

use adbc::{
    Connection, ConnectionOption, Database, DatabaseOption, Driver, Error, InfoCode, IngestMode,
    IsolationLevel, ObjectDepth, OptionValue, Result, Statement, StatementOption,
};

use catalog::{get_info_batch, get_objects_batch, get_table_schema_impl, get_table_types_batch};
use convert::SqliteReader;

// ─────────────────────────────────────────────────────────────
// SqliteDriver
// ─────────────────────────────────────────────────────────────

/// The top-level ADBC driver for SQLite.
///
/// This driver is stateless — create one via `SqliteDriver::default()` and
/// call [`Driver::new_database`] or [`Driver::new_database_with_opts`].
#[derive(Default, Debug, Clone, Copy)]
pub struct SqliteDriver;

impl Driver for SqliteDriver {
    type DatabaseType = SqliteDatabase;

    fn new_database(&mut self) -> Result<SqliteDatabase> {
        Ok(SqliteDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)>,
    ) -> Result<SqliteDatabase> {
        let mut db = SqliteDatabase::default();
        for (k, v) in opts {
            match k {
                DatabaseOption::Uri => {
                    db.uri = require_string(v, "Uri")?;
                }
                DatabaseOption::Username | DatabaseOption::Password => {
                    // SQLite doesn't use credentials; ignore silently.
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
// SqliteDatabase
// ─────────────────────────────────────────────────────────────

/// Configuration for a SQLite database.
///
/// The primary option is the URI — use `":memory:"` for an ephemeral
/// in-process database, or a filesystem path for a persistent one.
#[derive(Debug, Clone)]
pub struct SqliteDatabase {
    uri: String,
}

impl Default for SqliteDatabase {
    fn default() -> Self {
        Self {
            uri: ":memory:".into(),
        }
    }
}

impl Database for SqliteDatabase {
    type ConnectionType = SqliteConnection;

    fn new_connection(&self) -> Result<SqliteConnection> {
        SqliteConnection::open(&self.uri)
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption>,
    ) -> Result<SqliteConnection> {
        let mut conn = self.new_connection()?;
        for opt in opts {
            conn.set_option(opt)?;
        }
        Ok(conn)
    }
}

// ─────────────────────────────────────────────────────────────
// SqliteConnection
// ─────────────────────────────────────────────────────────────

/// An open connection to a SQLite database.
///
/// Wraps a [`rusqlite::Connection`] inside an `Arc<Mutex<…>>` so that
/// metadata methods (`get_info` etc.) — which take `&self` — can still safely
/// lock and query the database.
pub struct SqliteConnection {
    inner: Arc<Mutex<rusqlite::Connection>>,
    autocommit: bool,
    in_transaction: bool,
}

impl SqliteConnection {
    fn open(uri: &str) -> Result<Self> {
        let conn = if uri == ":memory:" {
            rusqlite::Connection::open_in_memory()
        } else {
            rusqlite::Connection::open(uri)
        }
        .map_err(|e| Error::io(e.to_string()))?;

        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
            autocommit: true,
            in_transaction: false,
        })
    }

    fn exec(&self, sql: &str) -> Result<()> {
        self.inner
            .lock()
            .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?
            .execute_batch(sql)
            .map_err(|e| Error::internal(e.to_string()))
    }
}

impl Connection for SqliteConnection {
    type StatementType = SqliteStatement;

    fn new_statement(&mut self) -> Result<SqliteStatement> {
        Ok(SqliteStatement::new(Arc::clone(&self.inner)))
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
                    self.exec("BEGIN DEFERRED")?;
                    self.in_transaction = true;
                }
                self.autocommit = enable;
                Ok(())
            }
            ConnectionOption::ReadOnly(_) => Err(Error::not_impl(
                "ReadOnly option is not supported for SQLite",
            )),
            ConnectionOption::IsolationLevel(lvl) => {
                // SQLite only supports serializable semantics, but we accept
                // Serializable silently and reject others.
                match lvl {
                    IsolationLevel::Serializable | IsolationLevel::Default => Ok(()),
                    _ => Err(Error::not_impl(format!(
                        "SQLite does not support IsolationLevel {:?}",
                        lvl
                    ))),
                }
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
        self.exec("BEGIN DEFERRED")
    }

    fn rollback(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::invalid_state(
                "rollback called while autocommit is enabled",
            ));
        }
        self.exec("ROLLBACK")?;
        self.exec("BEGIN DEFERRED")
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        name: &str,
    ) -> Result<Schema> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
        get_table_schema_impl(&conn, name)
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        let batch = get_table_types_batch()?;
        Ok(Box::new(OneBatch::new(batch)))
    }

    fn get_info(&self, codes: Option<&[InfoCode]>) -> Result<Box<dyn RecordBatchReader + Send>> {
        let batch = get_info_batch(codes)?;
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
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
        let batch = get_objects_batch(
            &conn,
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
// SqliteStatement
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

/// A query or bulk-ingest statement on a [`SqliteConnection`].
pub struct SqliteStatement {
    conn: Arc<Mutex<rusqlite::Connection>>,
    mode: Mode,
    bound_data: Option<Vec<RecordBatch>>,
}

impl SqliteStatement {
    fn new(conn: Arc<Mutex<rusqlite::Connection>>) -> Self {
        Self {
            conn,
            mode: Mode::Idle,
            bound_data: None,
        }
    }
}

impl Statement for SqliteStatement {
    fn set_sql_query(&mut self, sql: &str) -> Result<()> {
        self.mode = Mode::Sql(sql.to_owned());
        Ok(())
    }

    fn prepare(&mut self) -> Result<()> {
        match &self.mode {
            Mode::Sql(sql) => {
                let sql = sql.clone();
                let conn = self
                    .conn
                    .lock()
                    .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
                conn.prepare(&sql)
                    .map_err(|e| Error::invalid_arg(e.to_string()))?;
                self.mode = Mode::Prepared(sql);
                Ok(())
            }
            Mode::Prepared(_) => Ok(()), // idempotent
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
            Mode::Ingest { .. } => Err(Error::invalid_state("Cannot prepare an ingest statement")),
        }
    }

    fn execute(&mut self) -> Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)> {
        match &self.mode {
            Mode::Sql(sql) | Mode::Prepared(sql) => {
                let sql = sql.clone();
                let conn = self
                    .conn
                    .lock()
                    .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
                let reader = SqliteReader::execute(&conn, &sql)?;
                Ok((Box::new(reader), None))
            }
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
            Mode::Ingest { .. } => Err(Error::invalid_state("Use execute_update for ingest")),
        }
    }

    fn execute_update(&mut self) -> Result<i64> {
        match &self.mode {
            Mode::Sql(sql) | Mode::Prepared(sql) => {
                let sql = sql.clone();
                let conn = self
                    .conn
                    .lock()
                    .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
                conn.execute_batch(&sql)
                    .map(|_| conn.changes() as i64)
                    .map_err(|e| Error::internal(e.to_string()))
            }
            Mode::Ingest { table, mode } => {
                let table = table.clone();
                let mode = *mode;
                let batches = self.bound_data.take().unwrap_or_default();
                if batches.is_empty() {
                    return Err(Error::invalid_state("No data bound for ingest"));
                }
                let conn = self
                    .conn
                    .lock()
                    .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
                convert::ingest_batches(&conn, &table, mode, &batches)
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

struct OneBatch {
    batch: Option<RecordBatch>,
    schema: Arc<Schema>,
}

impl OneBatch {
    fn new(batch: RecordBatch) -> Self {
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
