//! Error types for ADBC.

use std::fmt;

/// Status codes following the ADBC v1.1.0 specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Status {
    /// No error.
    Ok,
    /// An unknown/miscellaneous error.
    Unknown,
    /// The operation is not implemented or supported.
    NotImplemented,
    /// A requested resource was not found.
    NotFound,
    /// A resource already exists.
    AlreadyExists,
    /// The request is semantically invalid.
    InvalidArguments,
    /// The object is in an invalid state for the requested operation.
    InvalidState,
    /// An I/O error occurred.
    Io,
    /// An internal error occurred — this indicates a driver bug.
    Internal,
    /// Authentication failed (e.g. wrong username/password).
    Unauthenticated,
    /// The caller is not authorized to perform the operation.
    Unauthorized,
    /// The operation was cancelled.
    Cancelled,
    /// The operation timed out.
    Timeout,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Status::Ok => "Ok",
            Status::Unknown => "Unknown",
            Status::NotImplemented => "NotImplemented",
            Status::NotFound => "NotFound",
            Status::AlreadyExists => "AlreadyExists",
            Status::InvalidArguments => "InvalidArguments",
            Status::InvalidState => "InvalidState",
            Status::Io => "Io",
            Status::Internal => "Internal",
            Status::Unauthenticated => "Unauthenticated",
            Status::Unauthorized => "Unauthorized",
            Status::Cancelled => "Cancelled",
            Status::Timeout => "Timeout",
        };
        f.write_str(s)
    }
}

/// An ADBC error.
///
/// Contains a human-readable message, a [`Status`] code, and optional
/// vendor-specific codes. Vendor codes follow the SQL SQLSTATE convention.
#[derive(Debug, Clone)]
pub struct Error {
    /// Human-readable description of the error.
    pub message: String,
    /// ADBC status code.
    pub status: Status,
    /// Optional vendor-specific error code (e.g. an HTTP status or errno).
    pub vendor_code: Option<i32>,
    /// Optional 5-char SQLSTATE code (e.g. `"42P01"` for undefined table).
    pub sqlstate: Option<[u8; 5]>,
}

impl Error {
    /// Create a new error with a message and status.
    pub fn new(message: impl Into<String>, status: Status) -> Self {
        Error {
            message: message.into(),
            status,
            vendor_code: None,
            sqlstate: None,
        }
    }

    /// Short-hand for a [`Status::NotImplemented`] error.
    pub fn not_impl(message: impl Into<String>) -> Self {
        Self::new(message, Status::NotImplemented)
    }

    /// Short-hand for a [`Status::InvalidArguments`] error.
    pub fn invalid_arg(message: impl Into<String>) -> Self {
        Self::new(message, Status::InvalidArguments)
    }

    /// Short-hand for a [`Status::InvalidState`] error.
    pub fn invalid_state(message: impl Into<String>) -> Self {
        Self::new(message, Status::InvalidState)
    }

    /// Short-hand for a [`Status::NotFound`] error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(message, Status::NotFound)
    }

    /// Short-hand for a [`Status::Internal`] error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(message, Status::Internal)
    }

    /// Short-hand for a [`Status::Io`] error.
    pub fn io(message: impl Into<String>) -> Self {
        Self::new(message, Status::Io)
    }

    /// Short-hand for a [`Status::AlreadyExists`] error.
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(message, Status::AlreadyExists)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.status, self.message)
    }
}

impl std::error::Error for Error {}

/// A convenience alias for `std::result::Result<T, adbc::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
