//! Pure-Rust MySQL ADBC driver.
//!
//! # Quick start
//!
//! ```rust,no_run
//! # #[tokio::main] async fn main() {
//! use adbc::{Driver, Database, Connection, Statement, DatabaseOption, OptionValue};
//! use adbc_mysql::MysqlDriver;
//!
//! let drv = MysqlDriver::default();
//! let db = drv.new_database_with_opts([
//!     (DatabaseOption::Uri, OptionValue::String(
//!         "mysql://alice:secret@localhost:3306/mydb".into(),
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
use mysql_async::prelude::Queryable;
use tokio::sync::Mutex;

use adbc::helpers::{require_string, OneBatch};
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

    async fn new_database(&self) -> Result<MysqlDatabase> {
        Ok(MysqlDatabase::default())
    }

    async fn new_database_with_opts(
        &self,
        opts: impl IntoIterator<Item = (DatabaseOption, OptionValue)> + Send,
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
///
/// A single connection pool is created when the first connection is opened
/// and reused for all subsequent connections.
pub struct MysqlDatabase {
    uri: String,
    username: Option<String>,
    password: Option<String>,
    pool: tokio::sync::OnceCell<mysql_async::Pool>,
}

impl std::fmt::Debug for MysqlDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlDatabase")
            .field("uri", &self.uri)
            .field("username", &self.username)
            .finish()
    }
}

impl Default for MysqlDatabase {
    fn default() -> Self {
        Self {
            uri: "mysql://root@localhost:3306/".into(),
            username: None,
            password: None,
            pool: tokio::sync::OnceCell::new(),
        }
    }
}

impl MysqlDatabase {
    async fn get_or_init_pool(&self) -> Result<&mysql_async::Pool> {
        self.pool
            .get_or_try_init(|| async {
                let opts = mysql_async::Opts::from_url(&self.uri)
                    .map_err(|e| Error::invalid_arg(e.to_string()))?;
                let mut builder = mysql_async::OptsBuilder::from_opts(opts);
                if let Some(ref u) = self.username {
                    builder = builder.user(Some(u.clone()));
                }
                if let Some(ref p) = self.password {
                    builder = builder.pass(Some(p.clone()));
                }
                Ok(mysql_async::Pool::new(builder))
            })
            .await
    }
}

impl Database for MysqlDatabase {
    type ConnectionType = MysqlConnection;

    async fn new_connection(&self) -> Result<MysqlConnection> {
        let pool = self.get_or_init_pool().await?;
        let conn = pool
            .get_conn()
            .await
            .map_err(|e| Error::io(e.to_string()))?;
        Ok(MysqlConnection {
            inner: Arc::new(Mutex::new(MysqlInner {
                conn,
                autocommit: true,
            })),
        })
    }

    async fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = ConnectionOption> + Send,
    ) -> Result<MysqlConnection> {
        let conn = self.new_connection().await?;
        let opts: Vec<_> = opts.into_iter().collect();
        for opt in opts {
            conn.set_option(opt).await?;
        }
        Ok(conn)
    }
}

// ─────────────────────────────────────────────────────────────
// MysqlConnection
// ─────────────────────────────────────────────────────────────

struct MysqlInner {
    conn: mysql_async::Conn,
    autocommit: bool,
}

/// A live MySQL connection.
///
/// Wraps `mysql_async::Conn` in `Arc<Mutex<…>>` so `&self` methods can
/// lock and use it asynchronously.
pub struct MysqlConnection {
    inner: Arc<Mutex<MysqlInner>>,
}

impl MysqlConnection {
    async fn exec_sql(&self, sql: &str) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner
            .conn
            .query_drop(sql)
            .await
            .map_err(|e| Error::internal(e.to_string()))
    }
}

impl Connection for MysqlConnection {
    type StatementType = MysqlStatement;

    async fn new_statement(&self) -> Result<MysqlStatement> {
        Ok(MysqlStatement::new(Arc::clone(&self.inner)))
    }

    async fn set_option(&self, opt: ConnectionOption) -> Result<()> {
        match opt {
            ConnectionOption::AutoCommit(enable) => {
                let v = if enable {
                    SqlLiteral("1")
                } else {
                    SqlLiteral("0")
                };
                let sql = trusted_sql!("SET autocommit = {}", v);
                self.exec_sql(sql.as_str()).await?;
                let mut inner = self.inner.lock().await;
                if !enable && inner.autocommit {
                    inner
                        .conn
                        .query_drop("START TRANSACTION")
                        .await
                        .map_err(|e| Error::internal(e.to_string()))?;
                }
                inner.autocommit = enable;
                Ok(())
            }
            ConnectionOption::ReadOnly(ro) => {
                let mode = if ro {
                    SqlLiteral("READ ONLY")
                } else {
                    SqlLiteral("READ WRITE")
                };
                let sql = trusted_sql!("SET TRANSACTION {}", mode);
                self.exec_sql(sql.as_str()).await
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
                self.exec_sql(sql.as_str()).await
            }
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
        inner
            .conn
            .query_drop("COMMIT")
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        inner
            .conn
            .query_drop("START TRANSACTION")
            .await
            .map_err(|e| Error::internal(e.to_string()))
    }

    async fn rollback(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.autocommit {
            return Err(Error::invalid_state(
                "rollback called while autocommit is enabled",
            ));
        }
        inner
            .conn
            .query_drop("ROLLBACK")
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        inner
            .conn
            .query_drop("START TRANSACTION")
            .await
            .map_err(|e| Error::internal(e.to_string()))
    }

    async fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        name: &str,
    ) -> Result<Schema> {
        let mut inner = self.inner.lock().await;
        get_table_schema_impl(&mut inner.conn, catalog, db_schema, name).await
    }

    async fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        Ok(Box::new(OneBatch::new(get_table_types_batch()?)))
    }

    async fn get_info(
        &self,
        codes: Option<&[InfoCode]>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let codes: Option<Vec<InfoCode>> = codes.map(|c| c.to_vec());
        let mut inner = self.inner.lock().await;
        Ok(Box::new(OneBatch::new(
            get_info_batch(&mut inner.conn, codes.as_deref()).await?,
        )))
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
        let mut inner = self.inner.lock().await;
        Ok(Box::new(OneBatch::new(
            get_objects_batch(
                &mut inner.conn,
                depth,
                catalog,
                db_schema,
                table_name,
                table_type,
                column_name,
            )
            .await?,
        )))
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
pub struct MysqlStatement {
    conn: Arc<Mutex<MysqlInner>>,
    mode: Mode,
    bound_data: Option<Vec<RecordBatch>>,
}

impl MysqlStatement {
    fn new(conn: Arc<Mutex<MysqlInner>>) -> Self {
        Self {
            conn,
            mode: Mode::Idle,
            bound_data: None,
        }
    }
}

impl Statement for MysqlStatement {
    async fn set_sql_query(&mut self, sql: &str) -> Result<()> {
        self.mode = Mode::Sql(sql.to_owned());
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        match &self.mode {
            Mode::Sql(_) => {
                // MySQL prepare is a no-op at the driver level; the server-side
                // prepare happens implicitly when exec is called. We accept
                // the call for API compatibility.
                Ok(())
            }
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
            Mode::Ingest { .. } => Err(Error::invalid_state("Cannot prepare an ingest statement")),
        }
    }

    async fn execute(&mut self) -> Result<(Box<dyn RecordBatchReader + Send>, Option<i64>)> {
        match &self.mode {
            Mode::Sql(sql) => {
                let sql = sql.clone();
                let mut inner = self.conn.lock().await;
                let stmt = inner
                    .conn
                    .prep(&sql)
                    .await
                    .map_err(|e| Error::io(e.to_string()))?;
                let schema = mysql_columns_to_schema(stmt.columns());
                let rows: Vec<mysql_async::Row> = inner
                    .conn
                    .exec(&stmt, ())
                    .await
                    .map_err(|e| Error::io(e.to_string()))?;
                let batch = rows_to_batch(rows, schema)?;
                Ok((Box::new(OneBatch::new(batch)), None))
            }
            Mode::Idle => Err(Error::invalid_state("No SQL has been set")),
            Mode::Ingest { .. } => Err(Error::invalid_state("Use execute_update for ingest")),
        }
    }

    async fn execute_update(&mut self) -> Result<i64> {
        match &self.mode {
            Mode::Sql(sql) => {
                let sql = sql.clone();
                let mut inner = self.conn.lock().await;
                inner
                    .conn
                    .exec_drop(&sql, ())
                    .await
                    .map_err(|e| Error::internal(e.to_string()))?;
                Ok(inner.conn.affected_rows() as i64)
            }
            Mode::Ingest { table, mode } => {
                let table = table.clone();
                let mode = *mode;
                let batches = self.bound_data.take().unwrap_or_default();
                if batches.is_empty() {
                    return Err(Error::invalid_state("No data bound for ingest"));
                }
                let mut inner = self.conn.lock().await;
                convert::ingest_batches(&mut inner.conn, &table, mode, &batches).await
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
