//! # adbc — Clean-room Arrow Database Connectivity core library
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
//! Every type in the chain is generic over its successor, so drivers can use
//! concrete types all the way down without heap allocation.

pub mod driver;
pub mod error;
pub mod schema;
pub mod sql;

pub use driver::{
    Connection, ConnectionOption, Database, DatabaseOption, Driver, InfoCode, IngestMode,
    IsolationLevel, ObjectDepth, OptionValue, Statement, StatementOption,
};
pub use error::{Error, Result, Status};
pub use sql::{QuotedIdent, SqlColumnDef, SqlJoined, SqlLiteral, SqlPlaceholders, TrustedSql};
