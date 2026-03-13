//! Pure-Rust SQLite ADBC driver.
//!
//! # Quick start
//!
//! ```rust,no_run
//! # #[tokio::main] async fn main() {
//! use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
//! use adbc_sqlite::SqliteDriver;
//!
//! let drv = SqliteDriver::default();
//! let db = drv.new_database_with_opts([
//!     (DatabaseOption::Uri, OptionValue::String(":memory:".into())),
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

use std::sync::{Arc, Mutex};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

use adbc::{
    helpers::{require_string, OneBatch},
    Connection, ConnectionOption, Database, DatabaseOption, Driver, Error, InfoCode, IngestMode,
    IsolationLevel, ObjectDepth, OptionValue, Result, Statement, StatementOption,
};

use catalog::{get_info_batch, get_objects_batch, get_table_schema_impl, get_table_types_batch};
use convert::SqliteReader;

// ─────────────────────────────────────────────────────────────
// SqliteDriver
// ─────────────────────────────────────────────────────────────

/// The top-level ADBC driver for SQLite.
#[derive(Default, Debug, Clone, Copy)]
pub struct SqliteDriver;

impl Driver for SqliteDriver {
    type DatabaseType = SqliteDatabase;

    async fn new_database(&self) -> Result<SqliteDatabase> {
        Ok(SqliteDatabase::default())
    }

    async fn new_database_with_opts(
        &self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)> + Send,
    ) -> Result<SqliteDatabase> {
        let mut db = SqliteDatabase::default();
        for (k, v) in opts {
            match k {
                DatabaseOption::Uri => {
                    db.uri = require_string(v, "Uri")?;
                }
                DatabaseOption::Username | DatabaseOption::Password => {}
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

    async fn new_connection(&self) -> Result<SqliteConnection> {
        SqliteConnection::open(&self.uri)
    }

    async fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption> + Send,
    ) -> Result<SqliteConnection> {
        let conn = self.new_connection().await?;
        let opts: Vec<_> = opts.into_iter().collect();
        for opt in opts {
            conn.set_option(opt).await?;
        }
        Ok(conn)
    }
}

// ─────────────────────────────────────────────────────────────
// SqliteConnection
// ─────────────────────────────────────────────────────────────

/// Internal state shared between Connection and Statement.
struct SqliteInner {
    conn: rusqlite::Connection,
    autocommit: bool,
    in_transaction: bool,
}

/// An open connection to a SQLite database.
///
/// Wraps a [`rusqlite::Connection`] inside an `Arc<Mutex<…>>` so that
/// async methods can safely lock and use it from `spawn_blocking`.
pub struct SqliteConnection {
    inner: Arc<Mutex<SqliteInner>>,
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
            inner: Arc::new(Mutex::new(SqliteInner {
                conn,
                autocommit: true,
                in_transaction: false,
            })),
        })
    }

    /// Run a blocking closure on the shared connection via `spawn_blocking`.
    async fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut SqliteInner) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut state = inner
                .lock()
                .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
            f(&mut state)
        })
        .await
        .map_err(|e| Error::internal(e.to_string()))?
    }
}

impl Drop for SqliteConnection {
    fn drop(&mut self) {
        if let Ok(state) = self.inner.lock() {
            if !state.autocommit && state.in_transaction {
                let _ = state.conn.execute_batch("ROLLBACK");
            }
        }
    }
}

impl Connection for SqliteConnection {
    type StatementType = SqliteStatement;

    async fn new_statement(&self) -> Result<SqliteStatement> {
        Ok(SqliteStatement::new(Arc::clone(&self.inner)))
    }

    async fn set_option(&self, opt: ConnectionOption) -> Result<()> {
        self.with_conn(move |s| match opt {
            ConnectionOption::AutoCommit(enable) => {
                if enable && !s.autocommit {
                    if s.in_transaction {
                        s.conn
                            .execute_batch("COMMIT")
                            .map_err(|e| Error::internal(e.to_string()))?;
                        s.in_transaction = false;
                    }
                } else if !enable && s.autocommit {
                    s.conn
                        .execute_batch("BEGIN DEFERRED")
                        .map_err(|e| Error::internal(e.to_string()))?;
                    s.in_transaction = true;
                }
                s.autocommit = enable;
                Ok(())
            }
            ConnectionOption::ReadOnly(_) => Err(Error::not_impl(
                "ReadOnly option is not supported for SQLite",
            )),
            ConnectionOption::IsolationLevel(lvl) => match lvl {
                IsolationLevel::Serializable | IsolationLevel::Default => Ok(()),
                _ => Err(Error::not_impl(format!(
                    "SQLite does not support IsolationLevel {:?}",
                    lvl
                ))),
            },
            ConnectionOption::Other(key, _) => Err(Error::invalid_arg(format!(
                "Unknown connection option: {key}"
            ))),
        })
        .await
    }

    async fn commit(&self) -> Result<()> {
        self.with_conn(|s| {
            if s.autocommit {
                return Err(Error::invalid_state(
                    "commit called while autocommit is enabled",
                ));
            }
            s.conn
                .execute_batch("COMMIT")
                .map_err(|e| Error::internal(e.to_string()))?;
            s.conn
                .execute_batch("BEGIN DEFERRED")
                .map_err(|e| Error::internal(e.to_string()))
        })
        .await
    }

    async fn rollback(&self) -> Result<()> {
        self.with_conn(|s| {
            if s.autocommit {
                return Err(Error::invalid_state(
                    "rollback called while autocommit is enabled",
                ));
            }
            s.conn
                .execute_batch("ROLLBACK")
                .map_err(|e| Error::internal(e.to_string()))?;
            s.conn
                .execute_batch("BEGIN DEFERRED")
                .map_err(|e| Error::internal(e.to_string()))
        })
        .await
    }

    async fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        name: &str,
    ) -> Result<Schema> {
        let name = name.to_owned();
        self.with_conn(move |s| get_table_schema_impl(&s.conn, &name))
            .await
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
        let batch = get_info_batch(codes.as_deref())?;
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
        let catalog = catalog.map(|s| s.to_owned());
        let db_schema = db_schema.map(|s| s.to_owned());
        let table_name = table_name.map(|s| s.to_owned());
        let table_type: Option<Vec<String>> =
            table_type.map(|t| t.iter().map(|s| s.to_string()).collect());
        let column_name = column_name.map(|s| s.to_owned());
        self.with_conn(move |s| {
            let table_type_refs: Option<Vec<&str>> = table_type
                .as_ref()
                .map(|v| v.iter().map(|s| s.as_str()).collect());
            let batch = get_objects_batch(
                &s.conn,
                depth,
                catalog.as_deref(),
                db_schema.as_deref(),
                table_name.as_deref(),
                table_type_refs.as_deref(),
                column_name.as_deref(),
            )?;
            Ok(Box::new(OneBatch::new(batch)) as Box<dyn RecordBatchReader + Send>)
        })
        .await
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
    conn: Arc<Mutex<SqliteInner>>,
    mode: Mode,
    bound_data: Option<Vec<RecordBatch>>,
}

impl SqliteStatement {
    fn new(conn: Arc<Mutex<SqliteInner>>) -> Self {
        Self {
            conn,
            mode: Mode::Idle,
            bound_data: None,
        }
    }

    /// Run a blocking closure on the shared connection via `spawn_blocking`.
    async fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut SqliteInner) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut state = inner
                .lock()
                .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
            f(&mut state)
        })
        .await
        .map_err(|e| Error::internal(e.to_string()))?
    }
}

impl Statement for SqliteStatement {
    async fn set_sql_query(&mut self, sql: &str) -> Result<()> {
        self.mode = Mode::Sql(sql.to_owned());
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        match &self.mode {
            Mode::Sql(sql) => {
                let sql = sql.clone();
                let sql_clone = sql.clone();
                self.with_conn(move |s| {
                    s.conn
                        .prepare(&sql_clone)
                        .map_err(|e| Error::invalid_arg(e.to_string()))?;
                    Ok(())
                })
                .await?;
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
                self.with_conn(move |s| {
                    let reader = SqliteReader::execute(&s.conn, &sql)?;
                    Ok((Box::new(reader) as Box<dyn RecordBatchReader + Send>, None))
                })
                .await
            }
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
            Mode::Ingest { .. } => Err(Error::invalid_state("Use execute_update for ingest")),
        }
    }

    async fn execute_update(&mut self) -> Result<i64> {
        match &self.mode {
            Mode::Sql(sql) | Mode::Prepared(sql) => {
                let sql = sql.clone();
                self.with_conn(move |s| {
                    let before = s.conn.total_changes();
                    s.conn
                        .execute_batch(&sql)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    Ok((s.conn.total_changes() - before) as i64)
                })
                .await
            }
            Mode::Ingest { table, mode } => {
                let table = table.clone();
                let mode = *mode;
                let batches = self.bound_data.take().unwrap_or_default();
                if batches.is_empty() {
                    return Err(Error::invalid_state("No data bound for ingest"));
                }
                self.with_conn(move |s| convert::ingest_batches(&s.conn, &table, mode, &batches))
                    .await
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
