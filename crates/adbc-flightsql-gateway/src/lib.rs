//! FlightSQL server gateway backed by any ADBC driver.
//!
//! Accepts FlightSQL gRPC requests and translates them into ADBC operations
//! against any backend (PostgreSQL, MySQL, SQLite, etc.).
//!
//! # Quick start
//!
//! ```rust,no_run
//! # #[tokio::main] async fn main() {
//! use adbc::{Driver, Database, DatabaseOption, OptionValue};
//! use adbc_sqlite::SqliteDriver;
//! use adbc_flightsql_gateway::FlightSqlGateway;
//! use arrow_flight::flight_service_server::FlightServiceServer;
//!
//! let drv = SqliteDriver;
//! let db = drv.new_database_with_opts([
//!     (DatabaseOption::Uri, OptionValue::String(":memory:".into())),
//! ]).await.unwrap();
//! let gateway = FlightSqlGateway::new(db);
//!
//! tonic::transport::Server::builder()
//!     .add_service(FlightServiceServer::new(gateway))
//!     .serve("[::1]:32010".parse().unwrap())
//!     .await
//!     .unwrap();
//! # }
//! ```

mod encode;
pub(crate) mod session;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    CommandGetSqlInfo, CommandGetTableTypes, CommandStatementQuery, CommandStatementUpdate,
    ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    flight_service_server::FlightService, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, Ticket,
};
use futures_util::stream;
use futures_util::StreamExt;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use adbc::{Connection, Database, InfoCode, Statement};

use session::{parse_basic_auth, SessionManager};

const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(30 * 60); // 30 minutes

fn adbc_to_status(e: adbc::Error) -> Status {
    let code = match e.status {
        adbc::Status::Ok => tonic::Code::Internal,
        adbc::Status::NotImplemented => tonic::Code::Unimplemented,
        adbc::Status::NotFound => tonic::Code::NotFound,
        adbc::Status::AlreadyExists => tonic::Code::AlreadyExists,
        adbc::Status::InvalidArguments => tonic::Code::InvalidArgument,
        adbc::Status::InvalidState => tonic::Code::FailedPrecondition,
        adbc::Status::Unauthenticated => tonic::Code::Unauthenticated,
        adbc::Status::Unauthorized => tonic::Code::PermissionDenied,
        adbc::Status::Io => tonic::Code::Unavailable,
        adbc::Status::Internal => tonic::Code::Internal,
        adbc::Status::Cancelled => tonic::Code::Cancelled,
        adbc::Status::Timeout => tonic::Code::DeadlineExceeded,
        adbc::Status::Unknown => tonic::Code::Unknown,
    };
    let mut msg = e.message;
    if let Some(state) = e.sqlstate {
        if let Ok(s) = std::str::from_utf8(&state) {
            msg = format!("[SQLSTATE {s}] {msg}");
        }
    }
    if let Some(vc) = e.vendor_code {
        msg = format!("[vendor={vc}] {msg}");
    }
    Status::new(code, msg)
}

/// A FlightSQL server backed by any ADBC [`Database`].
///
/// Supports two modes of operation:
///
/// 1. **Authenticated sessions:** Client calls `do_handshake` with Basic auth
///    credentials. The gateway creates a dedicated ADBC connection, returns a
///    bearer token. All subsequent requests with that token reuse the same
///    connection (session affinity for temp tables, transactions, etc.).
///
/// 2. **Anonymous (no handshake):** Requests without an `authorization` header
///    create a fresh one-shot connection per request (stateless, backward-compatible).
pub struct FlightSqlGateway<D: Database + 'static> {
    sessions: Arc<SessionManager<D>>,
}

impl<D: Database + 'static> FlightSqlGateway<D> {
    /// Create a new gateway backed by the given ADBC database.
    pub fn new(db: D) -> Self {
        Self::with_session_timeout(db, DEFAULT_SESSION_TIMEOUT)
    }

    /// Create a new gateway with a custom session idle timeout.
    pub fn with_session_timeout(db: D, timeout: Duration) -> Self {
        let sessions = Arc::new(SessionManager::new(Arc::new(db), timeout));
        // Spawn background reaper for expired sessions.
        let reaper_sessions = Arc::clone(&sessions);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                reaper_sessions.reap_expired();
            }
        });
        Self { sessions }
    }
}

fn reader_to_flight_data(
    reader: Box<dyn RecordBatchReader + Send>,
) -> Result<Vec<FlightData>, Status> {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Status::internal(e.to_string()))?;
    encode::batches_to_flight_data(&schema, &batches).map_err(|e| Status::internal(e.to_string()))
}

fn flight_data_to_stream(
    data: Vec<FlightData>,
) -> Pin<Box<dyn futures_util::Stream<Item = Result<FlightData, Status>> + Send>> {
    Box::pin(stream::iter(data.into_iter().map(Ok)))
}

fn sql_info_to_adbc_codes(info: &[u32]) -> Vec<InfoCode> {
    info.iter()
        .filter_map(|&code| InfoCode::try_from(code).ok())
        .collect()
}

#[tonic::async_trait]
impl<D: Database + 'static> FlightSqlService for FlightSqlGateway<D> {
    type FlightService = FlightSqlGateway<D>;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    // ── Authentication ───────────────────────────────────────

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<
            Pin<Box<dyn futures_util::Stream<Item = Result<HandshakeResponse, Status>> + Send>>,
        >,
        Status,
    > {
        // Parse Basic auth credentials from the request metadata.
        let (_username, _password) = parse_basic_auth(request.metadata())?;

        // Create a new session (dedicated ADBC connection).
        // In the future, credentials could be forwarded to the backend via
        // DatabaseOption::Username/Password for per-user connection auth.
        let token = self.sessions.create_session().await?;

        // Consume the request stream (protocol requires reading it).
        let mut stream = request.into_inner();
        while stream.next().await.is_some() {}

        // Build response with bearer token in metadata.
        let response_stream: Pin<
            Box<dyn futures_util::Stream<Item = Result<HandshakeResponse, Status>> + Send>,
        > = Box::pin(stream::once(async {
            Ok(HandshakeResponse {
                protocol_version: 0,
                payload: bytes::Bytes::new(),
            })
        }));

        let mut response = Response::new(response_stream);
        let bearer = format!("Bearer {token}")
            .parse()
            .map_err(|_| Status::internal("failed to format bearer token"))?;
        response.metadata_mut().insert("authorization", bearer);

        Ok(response)
    }

    // ── Query execution ──────────────────────────────────────

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket_query = TicketStatementQuery {
            statement_handle: query.query.into_bytes().into(),
        };
        let ticket = Ticket::new(ticket_query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new().with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("invalid UTF-8 in handle: {e}")))?;

        let resolved = self.sessions.resolve_connection(request.metadata()).await?;
        let conn = resolved.conn();
        let mut stmt = conn.new_statement().await.map_err(adbc_to_status)?;
        stmt.set_sql_query(&sql).await.map_err(adbc_to_status)?;
        let (reader, _) = stmt.execute().await.map_err(adbc_to_status)?;

        let data = reader_to_flight_data(reader)?;
        Ok(Response::new(flight_data_to_stream(data)))
    }

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let resolved = self.sessions.resolve_connection(request.metadata()).await?;
        let conn = resolved.conn();
        let mut stmt = conn.new_statement().await.map_err(adbc_to_status)?;
        stmt.set_sql_query(&ticket.query)
            .await
            .map_err(adbc_to_status)?;
        stmt.execute_update().await.map_err(adbc_to_status)
    }

    // ── Metadata: table types ────────────────────────────────

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let fd = request.into_inner();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(fd.cmd));
        let info = FlightInfo::new().with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let resolved = self.sessions.resolve_connection(request.metadata()).await?;
        let conn = resolved.conn();
        let reader = conn.get_table_types().await.map_err(adbc_to_status)?;
        let data = reader_to_flight_data(reader)?;
        Ok(Response::new(flight_data_to_stream(data)))
    }

    // ── Metadata: SQL info ───────────────────────────────────

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let fd = request.into_inner();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(fd.cmd));
        let info = FlightInfo::new().with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let codes = if query.info.is_empty() {
            None
        } else {
            let adbc_codes = sql_info_to_adbc_codes(&query.info);
            if adbc_codes.is_empty() {
                None
            } else {
                Some(adbc_codes)
            }
        };
        let resolved = self.sessions.resolve_connection(request.metadata()).await?;
        let conn = resolved.conn();
        let reader = conn
            .get_info(codes.as_deref())
            .await
            .map_err(adbc_to_status)?;
        let data = reader_to_flight_data(reader)?;
        Ok(Response::new(flight_data_to_stream(data)))
    }
}
