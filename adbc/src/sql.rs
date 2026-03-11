//! Compile-time-checked SQL string construction.
//!
//! This module prevents SQL injection by ensuring that SQL strings can only be
//! built from static literals and properly escaped components. The `trusted_sql!`
//! macro enforces at compile time that every interpolated value implements
//! [`SqlSafe`], a sealed marker trait.
//!
//! # Example
//!
//! ```
//! use adbc::sql::{QuotedIdent, SqlLiteral, SqlJoined, SqlColumnDef};
//! use adbc::trusted_sql;
//!
//! let table = QuotedIdent::ansi("users");
//! let col = QuotedIdent::ansi("name");
//! let ty = SqlLiteral("TEXT");
//! let col_def = SqlColumnDef::new(&col, ty);
//! let cols = SqlJoined::new([col_def], ", ");
//! let sql = trusted_sql!("CREATE TABLE {} ({})", table, cols);
//! assert_eq!(sql.as_str(), r#"CREATE TABLE "users" ("name" TEXT)"#);
//! ```

use std::fmt;

// ─────────────────────────────────────────────────────────────
// SqlSafe — sealed marker trait
// ─────────────────────────────────────────────────────────────

/// Marker trait for types that are safe to interpolate into SQL strings.
///
/// This trait is sealed: it cannot be implemented outside this crate,
/// which prevents arbitrary `String` or `&str` values from being
/// interpolated into SQL via the `trusted_sql!` macro.
pub trait SqlSafe: fmt::Display + private::Sealed {}

mod private {
    pub trait Sealed {}
    impl<T: Sealed> Sealed for &T {}
}

impl<T: SqlSafe> SqlSafe for &T {}

/// Compile-time assertion that a value implements [`SqlSafe`].
/// Used by the `trusted_sql!` macro; not intended for direct use.
#[doc(hidden)]
#[inline(always)]
pub fn assert_sql_safe<T: SqlSafe>(_: &T) {}

// ─────────────────────────────────────────────────────────────
// TrustedSql — the safe SQL string wrapper
// ─────────────────────────────────────────────────────────────

/// A SQL string that has been constructed exclusively from static literals
/// and [`SqlSafe`] values (quoted identifiers, type names, etc.).
///
/// Use the `trusted_sql!` macro to construct instances.
/// Call [`.as_str()`](TrustedSql::as_str) to pass the SQL to database APIs.
pub struct TrustedSql(String);

impl TrustedSql {
    /// Internal constructor used by the `trusted_sql!` macro.
    #[doc(hidden)]
    pub fn from_raw(s: String) -> Self {
        Self(s)
    }

    /// Returns the SQL string as a `&str` for passing to database APIs.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Appends another [`TrustedSql`] fragment.
    pub fn push(&mut self, other: &TrustedSql) {
        self.0.push_str(&other.0);
    }

    /// Appends a static SQL fragment.
    pub fn push_static(&mut self, s: &'static str) {
        self.0.push_str(s);
    }
}

impl fmt::Display for TrustedSql {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl private::Sealed for TrustedSql {}
impl SqlSafe for TrustedSql {}

// ─────────────────────────────────────────────────────────────
// QuotedIdent — properly escaped SQL identifiers
// ─────────────────────────────────────────────────────────────

/// A properly quoted SQL identifier (table name, column name).
///
/// Created via [`QuotedIdent::ansi`] (double-quote escaping for SQLite,
/// PostgreSQL, DuckDB) or [`QuotedIdent::mysql`] (backtick escaping).
pub struct QuotedIdent(String);

impl QuotedIdent {
    /// Quote using ANSI SQL double-quote escaping.
    ///
    /// Used by SQLite, PostgreSQL, and DuckDB.
    /// `my"table` becomes `"my""table"`.
    pub fn ansi(name: &str) -> Self {
        Self(format!("\"{}\"", name.replace('"', "\"\"")))
    }

    /// Quote using MySQL backtick escaping.
    ///
    /// `` my`table `` becomes `` `my``table` ``.
    pub fn mysql(name: &str) -> Self {
        Self(format!("`{}`", name.replace('`', "``")))
    }
}

impl fmt::Display for QuotedIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl private::Sealed for QuotedIdent {}
impl SqlSafe for QuotedIdent {}

// ─────────────────────────────────────────────────────────────
// SqlLiteral — static SQL keywords and type names
// ─────────────────────────────────────────────────────────────

/// A static SQL keyword or type name (e.g., `"BIGINT"`, `"READ ONLY"`).
///
/// Only accepts `&'static str` to guarantee the value is known at compile time.
pub struct SqlLiteral(pub &'static str);

impl fmt::Display for SqlLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl private::Sealed for SqlLiteral {}
impl SqlSafe for SqlLiteral {}

// ─────────────────────────────────────────────────────────────
// SqlJoined — joined list of safe fragments
// ─────────────────────────────────────────────────────────────

/// Multiple [`SqlSafe`] values joined by a static separator.
///
/// # Example
///
/// ```
/// use adbc::sql::{QuotedIdent, SqlJoined};
///
/// let cols = SqlJoined::new(
///     [QuotedIdent::ansi("a"), QuotedIdent::ansi("b")],
///     ", ",
/// );
/// assert_eq!(cols.to_string(), r#""a", "b""#);
/// ```
pub struct SqlJoined(String);

impl SqlJoined {
    /// Join an iterator of [`SqlSafe`] values with a static separator.
    pub fn new<T: SqlSafe>(items: impl IntoIterator<Item = T>, sep: &'static str) -> Self {
        let s = items
            .into_iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(sep);
        Self(s)
    }
}

impl fmt::Display for SqlJoined {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl private::Sealed for SqlJoined {}
impl SqlSafe for SqlJoined {}

// ─────────────────────────────────────────────────────────────
// SqlColumnDef — "quoted_ident TYPE" pair
// ─────────────────────────────────────────────────────────────

/// A column definition consisting of a quoted identifier and a SQL type name.
///
/// # Example
///
/// ```
/// use adbc::sql::{QuotedIdent, SqlLiteral, SqlColumnDef};
///
/// let def = SqlColumnDef::new(&QuotedIdent::ansi("age"), SqlLiteral("BIGINT"));
/// assert_eq!(def.to_string(), r#""age" BIGINT"#);
/// ```
pub struct SqlColumnDef(String);

impl SqlColumnDef {
    /// Create a column definition from a quoted identifier and type name.
    pub fn new(ident: &QuotedIdent, type_name: SqlLiteral) -> Self {
        Self(format!("{} {}", ident, type_name))
    }
}

impl fmt::Display for SqlColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl private::Sealed for SqlColumnDef {}
impl SqlSafe for SqlColumnDef {}

// ─────────────────────────────────────────────────────────────
// SqlPlaceholders — parameter placeholder sequences
// ─────────────────────────────────────────────────────────────

/// A sequence of SQL parameter placeholders.
///
/// # Example
///
/// ```
/// use adbc::sql::SqlPlaceholders;
///
/// // Anonymous: ?, ?, ?
/// assert_eq!(SqlPlaceholders::anonymous(3).to_string(), "?, ?, ?");
///
/// // Positional (PostgreSQL): $1, $2, $3
/// assert_eq!(SqlPlaceholders::dollar(3).to_string(), "$1, $2, $3");
///
/// // Indexed (SQLite): ?1, ?2, ?3
/// assert_eq!(SqlPlaceholders::indexed(3).to_string(), "?1, ?2, ?3");
/// ```
pub struct SqlPlaceholders(String);

impl SqlPlaceholders {
    /// `?` repeated `n` times, comma-separated.
    pub fn anonymous(n: usize) -> Self {
        Self(vec!["?"; n].join(", "))
    }

    /// `$1, $2, ..., $n` (PostgreSQL style).
    pub fn dollar(n: usize) -> Self {
        Self(
            (1..=n)
                .map(|i| format!("${i}"))
                .collect::<Vec<_>>()
                .join(", "),
        )
    }

    /// `?1, ?2, ..., ?n` (SQLite style).
    pub fn indexed(n: usize) -> Self {
        Self(
            (1..=n)
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

impl fmt::Display for SqlPlaceholders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl private::Sealed for SqlPlaceholders {}
impl SqlSafe for SqlPlaceholders {}

// ─────────────────────────────────────────────────────────────
// trusted_sql! macro
// ─────────────────────────────────────────────────────────────

/// Build a [`TrustedSql`] string from a static format literal and [`SqlSafe`] values.
///
/// This macro works like [`format!`] but enforces at compile time that:
/// 1. The format string is a string literal (no runtime strings).
/// 2. Every interpolated value implements [`SqlSafe`].
///
/// Arbitrary `String` or `&str` values **cannot** be interpolated — they do not
/// implement `SqlSafe`. Use [`QuotedIdent`], [`SqlLiteral`], [`SqlJoined`],
/// [`SqlColumnDef`], or [`SqlPlaceholders`] instead.
///
/// # Example
///
/// ```
/// use adbc::sql::{QuotedIdent, SqlPlaceholders};
/// use adbc::trusted_sql;
///
/// let table = QuotedIdent::ansi("users");
/// let ph = SqlPlaceholders::dollar(2);
/// let sql = trusted_sql!("INSERT INTO {} VALUES ({})", table, ph);
/// assert_eq!(sql.as_str(), r#"INSERT INTO "users" VALUES ($1, $2)"#);
/// ```
///
/// ```compile_fail
/// // This fails to compile: String does not implement SqlSafe.
/// use adbc::trusted_sql;
/// let user_input = String::from("Robert'); DROP TABLE students;--");
/// let sql = trusted_sql!("SELECT * FROM {}", user_input);
/// ```
#[macro_export]
macro_rules! trusted_sql {
    // No arguments — just a static SQL string.
    ($lit:literal) => {
        $crate::sql::TrustedSql::from_raw(format!($lit))
    };
    // One or more arguments — each must implement SqlSafe.
    ($lit:literal, $($arg:expr),+ $(,)?) => {{
        $(if false { $crate::sql::assert_sql_safe(&$arg); })+
        $crate::sql::TrustedSql::from_raw(format!($lit, $($arg),+))
    }};
}
