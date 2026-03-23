//! Session management for the FlightSQL gateway.
//!
//! Each FlightSQL handshake creates a session backed by a dedicated ADBC
//! connection. The session is identified by an opaque bearer token (random
//! 128-bit hex string). Subsequent requests present the token in the
//! `authorization` gRPC metadata header to reuse the same connection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::prelude::*;
use dashmap::DashMap;
use rand::Rng;
use tonic::metadata::MetadataMap;
use tonic::Status;

use adbc::Database;

use crate::adbc_to_status;

/// Resolved connection for an RPC request -- either a session-bound connection
/// (shared via `Arc`) or a one-shot anonymous connection (owned).
pub(crate) enum ResolvedConnection<D: Database + 'static> {
    /// Session-bound: the `Arc` keeps the connection alive as long as the
    /// session exists. Multiple concurrent requests on the same session share
    /// this `Arc`.
    Session(Arc<D::ConnectionType>),
    /// Anonymous: a fresh connection created for this single request.
    Anonymous(D::ConnectionType),
}

impl<D: Database + 'static> ResolvedConnection<D> {
    pub(crate) fn conn(&self) -> &D::ConnectionType {
        match self {
            ResolvedConnection::Session(arc) => arc,
            ResolvedConnection::Anonymous(conn) => conn,
        }
    }
}

struct Session<D: Database + 'static> {
    conn: Arc<D::ConnectionType>,
    /// Epoch millis of last activity. Updated atomically (no lock needed).
    last_active: AtomicU64,
}

impl<D: Database + 'static> Session<D> {
    fn touch(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_active.store(now, Ordering::Relaxed);
    }

    fn millis_since_active(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now.saturating_sub(self.last_active.load(Ordering::Relaxed))
    }
}

pub(crate) struct SessionManager<D: Database + 'static> {
    db: Arc<D>,
    sessions: DashMap<String, Session<D>>,
    session_timeout: Duration,
}

impl<D: Database + 'static> SessionManager<D> {
    pub(crate) fn new(db: Arc<D>, session_timeout: Duration) -> Self {
        Self {
            db,
            sessions: DashMap::new(),
            session_timeout,
        }
    }

    /// Create a new session with a dedicated ADBC connection.
    /// Returns the bearer token.
    pub(crate) async fn create_session(&self) -> Result<String, Status> {
        let conn = self.db.new_connection().await.map_err(adbc_to_status)?;
        let token = generate_token();
        let session = Session {
            conn: Arc::new(conn),
            last_active: AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            ),
        };
        self.sessions.insert(token.clone(), session);
        Ok(token)
    }

    /// Look up a session by bearer token. Returns the shared connection.
    pub(crate) fn get_session_connection(
        &self,
        token: &str,
    ) -> Result<Arc<D::ConnectionType>, Status> {
        let entry = self
            .sessions
            .get(token)
            .ok_or_else(|| Status::unauthenticated("invalid or expired session token"))?;
        entry.touch();
        Ok(Arc::clone(&entry.conn))
    }

    /// Resolve a connection from request metadata.
    ///
    /// - If a `Bearer <token>` is present, look up the session.
    /// - If no authorization header, create an anonymous one-shot connection.
    pub(crate) async fn resolve_connection(
        &self,
        metadata: &MetadataMap,
    ) -> Result<ResolvedConnection<D>, Status> {
        match extract_bearer_token(metadata) {
            Some(token) => {
                let conn = self.get_session_connection(&token)?;
                Ok(ResolvedConnection::Session(conn))
            }
            None => {
                let conn = self.db.new_connection().await.map_err(adbc_to_status)?;
                Ok(ResolvedConnection::Anonymous(conn))
            }
        }
    }

    /// Remove sessions that have been idle longer than the timeout.
    pub(crate) fn reap_expired(&self) {
        let timeout_ms = self.session_timeout.as_millis() as u64;
        self.sessions
            .retain(|_, session| session.millis_since_active() < timeout_ms);
    }

}

/// Generate a random 128-bit hex token.
fn generate_token() -> String {
    let bytes: [u8; 16] = rand::rng().random();
    hex_encode(&bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Extract a bearer token from gRPC metadata.
///
/// Looks for the `authorization` header with format `Bearer <token>`.
pub(crate) fn extract_bearer_token(metadata: &MetadataMap) -> Option<String> {
    let val = metadata.get("authorization")?.to_str().ok()?;
    val.strip_prefix("Bearer ").map(|s| s.to_owned())
}

/// Parse Basic authentication credentials from gRPC metadata.
///
/// Expects the `authorization` header with format `Basic base64(user:pass)`.
pub(crate) fn parse_basic_auth(metadata: &MetadataMap) -> Result<(String, String), Status> {
    let val = metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("missing authorization header"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid authorization header encoding"))?;
    let encoded = val
        .strip_prefix("Basic ")
        .ok_or_else(|| Status::unauthenticated("expected Basic authentication scheme"))?;
    let decoded = BASE64_STANDARD
        .decode(encoded)
        .map_err(|_| Status::unauthenticated("invalid base64 in credentials"))?;
    let decoded = String::from_utf8(decoded)
        .map_err(|_| Status::unauthenticated("credentials contain invalid UTF-8"))?;
    let (user, pass) = decoded
        .split_once(':')
        .ok_or_else(|| Status::unauthenticated("credentials must be in user:password format"))?;
    Ok((user.to_owned(), pass.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata_with_auth(value: &str) -> MetadataMap {
        let mut map = MetadataMap::new();
        map.insert("authorization", value.parse().unwrap());
        map
    }

    // ── extract_bearer_token ─────────────────────────────────

    #[test]
    fn test_extract_bearer_token_valid() {
        let meta = metadata_with_auth("Bearer abc123");
        assert_eq!(extract_bearer_token(&meta), Some("abc123".to_owned()));
    }

    #[test]
    fn test_extract_bearer_token_missing_header() {
        let meta = MetadataMap::new();
        assert_eq!(extract_bearer_token(&meta), None);
    }

    #[test]
    fn test_extract_bearer_token_wrong_scheme() {
        let meta = metadata_with_auth("Basic abc123");
        assert_eq!(extract_bearer_token(&meta), None);
    }

    #[test]
    fn test_extract_bearer_token_no_space() {
        let meta = metadata_with_auth("Bearertoken");
        assert_eq!(extract_bearer_token(&meta), None);
    }

    // ── parse_basic_auth ─────────────────────────────────────

    #[test]
    fn test_parse_basic_auth_valid() {
        // "user:pass" -> base64 = "dXNlcjpwYXNz"
        let encoded = BASE64_STANDARD.encode("user:pass");
        let meta = metadata_with_auth(&format!("Basic {encoded}"));
        let (user, pass) = parse_basic_auth(&meta).unwrap();
        assert_eq!(user, "user");
        assert_eq!(pass, "pass");
    }

    #[test]
    fn test_parse_basic_auth_password_with_colon() {
        // "admin:p:a:ss" -> split_once(':') gives ("admin", "p:a:ss")
        let encoded = BASE64_STANDARD.encode("admin:p:a:ss");
        let meta = metadata_with_auth(&format!("Basic {encoded}"));
        let (user, pass) = parse_basic_auth(&meta).unwrap();
        assert_eq!(user, "admin");
        assert_eq!(pass, "p:a:ss");
    }

    #[test]
    fn test_parse_basic_auth_missing_header() {
        let meta = MetadataMap::new();
        let err = parse_basic_auth(&meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_parse_basic_auth_wrong_scheme() {
        let meta = metadata_with_auth("Bearer token123");
        let err = parse_basic_auth(&meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_parse_basic_auth_invalid_base64() {
        let meta = metadata_with_auth("Basic !!!notbase64!!!");
        let err = parse_basic_auth(&meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_parse_basic_auth_no_colon() {
        let encoded = BASE64_STANDARD.encode("nocolon");
        let meta = metadata_with_auth(&format!("Basic {encoded}"));
        let err = parse_basic_auth(&meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    // ── generate_token ───────────────────────────────────────

    #[test]
    fn test_generate_token_length_and_uniqueness() {
        let t1 = generate_token();
        let t2 = generate_token();
        assert_eq!(t1.len(), 32); // 16 bytes * 2 hex chars
        assert_ne!(t1, t2);
    }
}
