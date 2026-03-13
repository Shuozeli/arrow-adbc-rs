//! Object-safe (dyn-compatible) wrappers for the core ADBC traits.
//!
//! Native `async fn` in traits is not dyn-safe. This module provides
//! [`DynConnection`] and [`DynStatement`] traits that use boxed futures,
//! plus blanket implementations so any `Connection` or `Statement` is
//! automatically a `DynConnection` or `DynStatement`.
//!
//! Use these when you need `&dyn Connection`-style polymorphism (e.g.
//! generic middleware, test harnesses).

use std::future::Future;
use std::pin::Pin;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

use crate::error::Result;
use crate::{Connection, ConnectionOption, InfoCode, ObjectDepth, Statement, StatementOption};

/// A boxed, pinned, `Send` future with a borrowed lifetime.
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The return type from [`DynStatement::execute`].
type ExecuteResult = (Box<dyn RecordBatchReader + Send>, Option<i64>);

/// The return type from metadata methods that yield a reader.
type ReaderResult = Box<dyn RecordBatchReader + Send>;

// ─────────────────────────────────────────────────────────────
// DynStatement
// ─────────────────────────────────────────────────────────────

/// Object-safe version of [`Statement`].
pub trait DynStatement: Send + Sync {
    /// See [`Statement::set_sql_query`].
    fn set_sql_query<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, Result<()>>;

    /// See [`Statement::prepare`].
    fn prepare(&mut self) -> BoxFuture<'_, Result<()>>;

    /// See [`Statement::execute`].
    fn execute(&mut self) -> BoxFuture<'_, Result<ExecuteResult>>;

    /// See [`Statement::execute_update`].
    fn execute_update(&mut self) -> BoxFuture<'_, Result<i64>>;

    /// See [`Statement::bind`].
    fn bind(&mut self, batch: RecordBatch) -> BoxFuture<'_, Result<()>>;

    /// See [`Statement::bind_stream`].
    fn bind_stream(
        &mut self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> BoxFuture<'_, Result<()>>;

    /// See [`Statement::set_option`].
    fn set_option(&mut self, opt: StatementOption) -> BoxFuture<'_, Result<()>>;
}

impl<T: Statement> DynStatement for T {
    fn set_sql_query<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(Statement::set_sql_query(self, sql))
    }

    fn prepare(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(Statement::prepare(self))
    }

    fn execute(&mut self) -> BoxFuture<'_, Result<ExecuteResult>> {
        Box::pin(Statement::execute(self))
    }

    fn execute_update(&mut self) -> BoxFuture<'_, Result<i64>> {
        Box::pin(Statement::execute_update(self))
    }

    fn bind(&mut self, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(Statement::bind(self, batch))
    }

    fn bind_stream(
        &mut self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(Statement::bind_stream(self, reader))
    }

    fn set_option(&mut self, opt: StatementOption) -> BoxFuture<'_, Result<()>> {
        Box::pin(Statement::set_option(self, opt))
    }
}

// ─────────────────────────────────────────────────────────────
// DynConnection
// ─────────────────────────────────────────────────────────────

/// Object-safe version of [`Connection`].
pub trait DynConnection: Send + Sync {
    /// See [`Connection::new_statement`].
    fn new_statement(&self) -> BoxFuture<'_, Result<Box<dyn DynStatement>>>;

    /// See [`Connection::set_option`].
    fn set_option(&self, opt: ConnectionOption) -> BoxFuture<'_, Result<()>>;

    /// See [`Connection::commit`].
    fn commit(&self) -> BoxFuture<'_, Result<()>>;

    /// See [`Connection::rollback`].
    fn rollback(&self) -> BoxFuture<'_, Result<()>>;

    /// See [`Connection::get_table_schema`].
    fn get_table_schema<'a>(
        &'a self,
        catalog: Option<&'a str>,
        db_schema: Option<&'a str>,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Schema>>;

    /// See [`Connection::get_table_types`].
    fn get_table_types(&self) -> BoxFuture<'_, Result<ReaderResult>>;

    /// See [`Connection::get_info`].
    fn get_info<'a>(&'a self, codes: Option<&'a [InfoCode]>)
        -> BoxFuture<'a, Result<ReaderResult>>;

    /// See [`Connection::get_objects`].
    fn get_objects<'a>(
        &'a self,
        depth: ObjectDepth,
        catalog: Option<&'a str>,
        db_schema: Option<&'a str>,
        table_name: Option<&'a str>,
        table_type: Option<&'a [&'a str]>,
        column_name: Option<&'a str>,
    ) -> BoxFuture<'a, Result<ReaderResult>>;
}

impl<T: Connection> DynConnection for T
where
    T::StatementType: 'static,
{
    fn new_statement(&self) -> BoxFuture<'_, Result<Box<dyn DynStatement>>> {
        Box::pin(async move {
            let stmt = Connection::new_statement(self).await?;
            Ok(Box::new(stmt) as Box<dyn DynStatement>)
        })
    }

    fn set_option(&self, opt: ConnectionOption) -> BoxFuture<'_, Result<()>> {
        Box::pin(Connection::set_option(self, opt))
    }

    fn commit(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(Connection::commit(self))
    }

    fn rollback(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(Connection::rollback(self))
    }

    fn get_table_schema<'a>(
        &'a self,
        catalog: Option<&'a str>,
        db_schema: Option<&'a str>,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Schema>> {
        Box::pin(Connection::get_table_schema(self, catalog, db_schema, name))
    }

    fn get_table_types(&self) -> BoxFuture<'_, Result<ReaderResult>> {
        Box::pin(Connection::get_table_types(self))
    }

    fn get_info<'a>(
        &'a self,
        codes: Option<&'a [InfoCode]>,
    ) -> BoxFuture<'a, Result<ReaderResult>> {
        Box::pin(Connection::get_info(self, codes))
    }

    fn get_objects<'a>(
        &'a self,
        depth: ObjectDepth,
        catalog: Option<&'a str>,
        db_schema: Option<&'a str>,
        table_name: Option<&'a str>,
        table_type: Option<&'a [&'a str]>,
        column_name: Option<&'a str>,
    ) -> BoxFuture<'a, Result<ReaderResult>> {
        Box::pin(Connection::get_objects(
            self,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        ))
    }
}
