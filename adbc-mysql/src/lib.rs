//! Pure-Rust MySQL ADBC driver.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
//! use adbc_mysql::MysqlDriver;
//!
//! let mut drv = MysqlDriver::default();
//! let db = drv.new_database_with_opts([
//!     (DatabaseOption::Uri, OptionValue::String(
//!         "mysql://alice:secret@localhost:3306/mydb".into(),
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
use mysql::prelude::Queryable;

use adbc::sql::SqlLiteral;
use adbc::{
    trusted_sql, Connection, ConnectionOption, Database, DatabaseOption, Driver, Error, InfoCode,
    IngestMode, IsolationLevel, ObjectDepth, OptionValue, Result, Statement, StatementOption,
};
use catalog::{get_info_batch, get_objects_batch, get_table_schema_impl, get_table_types_batch};
use convert::{mysql_columns_to_schema, rows_to_batch};

// ─────────────────────────────────────────────────────────────
// MysqlDriver
// ─────────────────────────────────────────────────────────────

/// The top-level ADBC driver for MySQL.
#[derive(Default, Debug, Clone, Copy)]
pub struct MysqlDriver;

impl Driver for MysqlDriver {
    type DatabaseType = MysqlDatabase;

    fn new_database(&mut self) -> Result<MysqlDatabase> {
        Ok(MysqlDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)>,
    ) -> Result<MysqlDatabase> {
        let mut db = MysqlDatabase::default();
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
// MysqlDatabase
// ─────────────────────────────────────────────────────────────

/// Configuration for a MySQL database.
///
/// The `Uri` option should be a MySQL URL:
/// `mysql://user:password@host:port/database`
#[derive(Debug, Clone)]
pub struct MysqlDatabase {
    uri: String,
    username: Option<String>,
    password: Option<String>,
}

impl Default for MysqlDatabase {
    fn default() -> Self {
        Self {
            uri: "mysql://root@localhost:3306/".into(),
            username: None,
            password: None,
        }
    }
}

impl MysqlDatabase {
    fn pool(&self) -> Result<mysql::Pool> {
        let opts =
            mysql::Opts::from_url(&self.uri).map_err(|e| Error::invalid_arg(e.to_string()))?;
        let mut builder = mysql::OptsBuilder::from_opts(opts);
        if let Some(ref u) = self.username {
            builder = builder.user(Some(u.clone()));
        }
        if let Some(ref p) = self.password {
            builder = builder.pass(Some(p.clone()));
        }
        mysql::Pool::new(builder).map_err(|e| Error::io(e.to_string()))
    }
}

impl Database for MysqlDatabase {
    type ConnectionType = MysqlConnection;

    fn new_connection(&self) -> Result<MysqlConnection> {
        let pool = self.pool()?;
        let conn = pool.get_conn().map_err(|e| Error::io(e.to_string()))?;
        Ok(MysqlConnection {
            conn: Arc::new(Mutex::new(conn)),
            autocommit: true,
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption>,
    ) -> Result<MysqlConnection> {
        let mut conn = self.new_connection()?;
        for opt in opts {
            conn.set_option(opt)?;
        }
        Ok(conn)
    }
}

// ─────────────────────────────────────────────────────────────
// MysqlConnection
// ─────────────────────────────────────────────────────────────

/// A live MySQL connection.
///
/// Wraps `mysql::PooledConn` in `Arc<Mutex<…>>` so `&self` metadata methods
/// can lock it safely.
pub struct MysqlConnection {
    conn: Arc<Mutex<mysql::PooledConn>>,
    autocommit: bool,
}

impl MysqlConnection {
    fn exec_sql(&self, sql: &str) -> Result<()> {
        self.conn
            .lock()
            .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?
            .query_drop(sql)
            .map_err(|e| Error::internal(e.to_string()))
    }
}

impl Connection for MysqlConnection {
    type StatementType = MysqlStatement;

    fn new_statement(&mut self) -> Result<MysqlStatement> {
        Ok(MysqlStatement::new(Arc::clone(&self.conn)))
    }

    fn set_option(&mut self, opt: ConnectionOption) -> Result<()> {
        match opt {
            ConnectionOption::AutoCommit(enable) => {
                let v = if enable {
                    SqlLiteral("1")
                } else {
                    SqlLiteral("0")
                };
                let sql = trusted_sql!("SET autocommit = {}", v);
                self.exec_sql(sql.as_str())?;
                if !enable && self.autocommit {
                    self.exec_sql("START TRANSACTION")?;
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
                let sql = trusted_sql!("SET TRANSACTION {}", mode);
                self.exec_sql(sql.as_str())
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
                let sql = trusted_sql!("SET SESSION TRANSACTION ISOLATION LEVEL {}", sql_lvl);
                self.exec_sql(sql.as_str())
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
        self.exec_sql("COMMIT")?;
        self.exec_sql("START TRANSACTION")
    }

    fn rollback(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::invalid_state(
                "rollback called while autocommit is enabled",
            ));
        }
        self.exec_sql("ROLLBACK")?;
        self.exec_sql("START TRANSACTION")
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        name: &str,
    ) -> Result<Schema> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
        get_table_schema_impl(&mut conn, catalog, db_schema, name)
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        Ok(Box::new(OneBatch::new(get_table_types_batch()?)))
    }

    fn get_info(&self, codes: Option<&[InfoCode]>) -> Result<Box<dyn RecordBatchReader + Send>> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
        Ok(Box::new(OneBatch::new(get_info_batch(&mut conn, codes)?)))
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
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
        Ok(Box::new(OneBatch::new(get_objects_batch(
            &mut conn,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )?)))
    }
}

// ─────────────────────────────────────────────────────────────
// MysqlStatement
// ─────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
enum Mode {
    #[default]
    Idle,
    Sql(String),
    Ingest {
        table: String,
        mode: IngestMode,
    },
}

/// A query or bulk-ingest statement on a [`MysqlConnection`].
///
/// Holds a shared reference to the underlying connection so that
/// [`Statement::execute`] and [`Statement::execute_update`] work without
/// needing to call connection-level helpers.
pub struct MysqlStatement {
    conn: Arc<Mutex<mysql::PooledConn>>,
    mode: Mode,
    bound_data: Option<Vec<RecordBatch>>,
}

impl MysqlStatement {
    fn new(conn: Arc<Mutex<mysql::PooledConn>>) -> Self {
        Self {
            conn,
            mode: Mode::Idle,
            bound_data: None,
        }
    }
}

impl Statement for MysqlStatement {
    fn set_sql_query(&mut self, sql: &str) -> Result<()> {
        self.mode = Mode::Sql(sql.to_owned());
        Ok(())
    }

    fn prepare(&mut self) -> Result<()> {
        match &self.mode {
            Mode::Sql(_) | Mode::Idle => Ok(()),
            Mode::Ingest { .. } => Err(Error::invalid_state("Cannot prepare an ingest statement")),
        }
    }

    fn execute(&mut self) -> Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)> {
        match &self.mode {
            Mode::Sql(sql) => {
                let sql = sql.clone();
                let mut conn = self
                    .conn
                    .lock()
                    .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
                let result = conn
                    .exec_iter(&sql, ())
                    .map_err(|e| Error::io(e.to_string()))?;
                let cols = result.columns().as_ref().to_vec();
                let schema = mysql_columns_to_schema(&cols);
                let rows: Vec<mysql::Row> = result
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| Error::io(e.to_string()))?;
                let batch = rows_to_batch(rows, schema)?;
                Ok((Box::new(OneBatch::new(batch)), None))
            }
            Mode::Ingest { table, mode } => {
                let table = table.clone();
                let mode = *mode;
                let batches = self.bound_data.take().unwrap_or_default();
                if batches.is_empty() {
                    return Err(Error::invalid_state("No data bound for ingest"));
                }
                let mut conn = self
                    .conn
                    .lock()
                    .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
                let rows = convert::ingest_batches(&mut conn, &table, mode, &batches)?;
                Ok((
                    Box::new(OneBatch::new(RecordBatch::new_empty(Arc::new(
                        arrow_schema::Schema::empty(),
                    )))),
                    Some(rows),
                ))
            }
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
        }
    }

    fn execute_update(&mut self) -> Result<i64> {
        match &self.mode {
            Mode::Sql(sql) => {
                let sql = sql.clone();
                let mut conn = self
                    .conn
                    .lock()
                    .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
                conn.exec_drop(&sql, ())
                    .map_err(|e| Error::internal(e.to_string()))?;
                Ok(conn.affected_rows() as i64)
            }
            Mode::Ingest { table, mode } => {
                let table = table.clone();
                let mode = *mode;
                let batches = self.bound_data.take().unwrap_or_default();
                if batches.is_empty() {
                    return Err(Error::invalid_state("No data bound for ingest"));
                }
                let mut conn = self
                    .conn
                    .lock()
                    .map_err(|e| Error::internal(format!("mutex poisoned: {e}")))?;
                convert::ingest_batches(&mut conn, &table, mode, &batches)
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
                let m = if let Mode::Ingest { mode, .. } = &self.mode {
                    *mode
                } else {
                    IngestMode::Create
                };
                self.mode = Mode::Ingest { table, mode: m };
                Ok(())
            }
            StatementOption::IngestMode(m) => {
                if let Mode::Ingest { table, .. } = &self.mode {
                    let t = table.clone();
                    self.mode = Mode::Ingest { table: t, mode: m };
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
// MysqlConnection — execute helpers (kept for backward compat)
// ─────────────────────────────────────────────────────────────

impl MysqlConnection {
    /// Execute a SQL or ingest statement; returns Arrow [`RecordBatchReader`].
    ///
    /// This is a thin wrapper around [`Statement::execute`] kept for
    /// compatibility. Prefer calling `stmt.execute()` directly.
    pub fn execute_statement(
        &self,
        stmt: &mut MysqlStatement,
    ) -> Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)> {
        stmt.execute()
    }

    /// Execute a DML/DDL statement and return rows affected.
    ///
    /// This is a thin wrapper around [`Statement::execute_update`] kept for
    /// compatibility. Prefer calling `stmt.execute_update()` directly.
    pub fn execute_update_statement(&self, stmt: &mut MysqlStatement) -> Result<i64> {
        stmt.execute_update()
    }
}

// ─────────────────────────────────────────────────────────────
// OneBatch helper
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

fn require_string(v: OptionValue, name: &str) -> Result<String> {
    match v {
        OptionValue::String(s) => Ok(s),
        _ => Err(Error::invalid_arg(format!("{name} must be a string value"))),
    }
}
