//! # adbc -- Clean-room Arrow Database Connectivity core library
//!
//! This crate defines the fundamental traits and types for the ADBC v1.1.0
//! specification in idiomatic Rust. It has no C FFI, no dynamic loading,
//! and no dependency on the apache/arrow-adbc codebase.
//!
//! ## Architecture
//!
//! ```text
//! Driver ──creates──▶ Database ──creates──▶ Connection ──creates──▶ Statement
//! ```
//!
//! All traits are **async-first** with `Send + Sync` bounds. Drivers that
//! wrap synchronous libraries (e.g. SQLite via `rusqlite`) bridge internally
//! using `tokio::task::spawn_blocking`.
//!
//! For dyn-compatible trait objects, see the [`dyn_api`] module which provides
//! [`DynConnection`] and
//! [`DynStatement`] with blanket implementations.

pub mod driver;
pub mod dyn_api;
pub mod error;
pub mod helpers;
pub mod schema;
pub mod sql;

pub use driver::{
    Connection, ConnectionOption, Database, DatabaseOption, Driver, InfoCode, IngestMode,
    IsolationLevel, ObjectDepth, OptionValue, Statement, StatementOption,
};
pub use dyn_api::{DynConnection, DynStatement};
pub use error::{Error, Result, Status};
pub use helpers::{OneBatch, VecReader};
pub use sql::{QuotedIdent, SqlColumnDef, SqlJoined, SqlLiteral, SqlPlaceholders, TrustedSql};
