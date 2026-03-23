# Design: FlightSQL Gateway (`adbc-flightsql-gateway`)

> Last updated: 2026-03-22

## Problem

The `arrow-adbc-rs` ecosystem has client-side ADBC drivers for PostgreSQL, MySQL, SQLite, and FlightSQL. The FlightSQL driver connects **to** FlightSQL servers. But there is no way to **expose** a PostgreSQL or MySQL database as a FlightSQL server.

This means any tool that speaks FlightSQL (JDBC/ODBC drivers, Python ADBC clients, BI tools like Apache Superset, DBeaver with Arrow Flight connector) cannot connect to a plain PostgreSQL or MySQL instance through a standard columnar protocol.

## Solution

A new crate `adbc-flightsql-gateway` that implements the `FlightSqlService` trait from `arrow-flight`. It accepts FlightSQL gRPC requests and translates them into ADBC operations against any backend driver (PostgreSQL, MySQL, SQLite).

```
FlightSQL Client (JDBC / ODBC / Python / adbc-flightsql)
         |  gRPC / Arrow Flight protocol
         v
+----------------------------------+
|   adbc-flightsql-gateway         |
|   impl FlightSqlService          |
|                                  |
|   Incoming RPC  -->  ADBC call   |
|   Arrow results <--  RecordBatch |
+----------------------------------+
         |  ADBC Driver trait
         v
+----------------------------------+
|  adbc-postgres / adbc-mysql /    |
|  adbc-sqlite                     |
+----------------------------------+
         |
         v
   PostgreSQL / MySQL / SQLite
```

## Goals

1. Any FlightSQL client can query a PostgreSQL/MySQL/SQLite database through this gateway.
2. The gateway is generic over `D: Driver` -- one implementation works for all ADBC backends.
3. Supports query execution, catalog metadata, prepared statements, and transactions.
4. Streaming results -- does not buffer entire result sets in memory when possible.

## Non-Goals

- Query rewriting or SQL dialect translation (the gateway passes SQL through as-is; dialect differences are the client's responsibility).
- Connection pooling (out of scope for v1; each FlightSQL session maps to one ADBC connection).
- Multi-tenancy or access control beyond what the backend database provides.
- Substrait plan execution.

## Architecture

### Crate Location

`crates/adbc-flightsql-gateway` in the `arrow-adbc-rs` workspace.

### Dependencies

```toml
[dependencies]
adbc = { path = "../adbc" }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
arrow-ipc = { workspace = true }
arrow-flight = { workspace = true, features = ["flight-sql-experimental"] }
tonic = { workspace = true }
tokio = { workspace = true }
prost = { workspace = true }
bytes = { workspace = true }
dashmap = "6"
```

### Core Types

```rust
/// A FlightSQL server backed by any ADBC driver.
///
/// `D` is the ADBC Driver type (e.g. PostgresDriver, MysqlDriver).
/// The gateway holds a `Database` instance and creates connections
/// per FlightSQL session.
pub struct FlightSqlGateway<D: Driver> {
    db: Arc<D::DatabaseType>,
    sessions: DashMap<SessionId, SessionState<D>>,
    config: GatewayConfig,
}

/// Per-session state. A session starts at Handshake and ends
/// when the client disconnects or the session expires.
struct SessionState<D: Driver> {
    conn: D::ConnectionType,
    prepared: HashMap<PreparedHandle, PreparedState>,
    transaction_id: Option<TransactionId>,
}

/// Opaque handle for prepared statements.
type PreparedHandle = Bytes;

/// Opaque handle for transactions.
type TransactionId = Bytes;

pub struct GatewayConfig {
    /// Address to bind the gRPC server.
    pub bind_addr: SocketAddr,
    /// Optional TLS config.
    pub tls: Option<TlsConfig>,
    /// Max concurrent sessions.
    pub max_sessions: usize,
    /// Session idle timeout.
    pub session_timeout: Duration,
}
```

### Session Management

FlightSQL is session-oriented. The gateway maps sessions to ADBC connections:

1. **Handshake** -- Client sends credentials. Gateway calls `db.new_connection()`, stores the connection in `sessions`, returns a bearer token (the session ID).
2. **Subsequent RPCs** -- Client sends the bearer token in gRPC metadata. Gateway looks up the session and uses its ADBC connection.
3. **Disconnect / Timeout** -- Session is removed, ADBC connection is dropped.

Session IDs are random 128-bit tokens encoded as hex strings.

### RPC-to-ADBC Mapping

#### Query Execution

| FlightSQL RPC | Gateway Action |
|---|---|
| `get_flight_info_statement(CommandStatementQuery)` | Parse SQL, create a ticket containing the SQL + session ID. Return `FlightInfo` with one endpoint pointing to self. |
| `do_get_statement(TicketStatementQuery)` | Look up session. Create ADBC `Statement`, call `set_sql_query(sql)` then `execute()`. Stream `RecordBatch`es back as `FlightData`. |
| `do_put_statement_update(CommandStatementUpdate)` | Look up session. Create ADBC `Statement`, call `set_sql_query(sql)` then `execute_update()`. Return affected row count. |

**Schema in GetFlightInfo**: For SELECT queries, the gateway needs the result schema before the client calls DoGet. Two strategies:

- **Strategy A (lazy)**: Return an empty schema in `FlightInfo` and let the client discover it from the first `FlightData` message in `DoGet`. This is spec-compliant.
- **Strategy B (eager)**: Execute a `PREPARE` to get the schema, then cancel. This adds latency.

We use **Strategy A** for Phase 1, with Strategy B as an opt-in config flag later.

#### Catalog Metadata

| FlightSQL Command | ADBC Method |
|---|---|
| `CommandGetCatalogs` | `conn.get_objects(ObjectDepth::Catalogs, ...)` + project catalog column |
| `CommandGetDbSchemas` | `conn.get_objects(ObjectDepth::Schemas, ...)` + project catalog/schema columns |
| `CommandGetTables` | `conn.get_objects(ObjectDepth::Tables or Columns, ...)` + restructure |
| `CommandGetTableTypes` | `conn.get_table_types()` |
| `CommandGetSqlInfo` | `conn.get_info(codes)` + map to FlightSQL SqlInfo schema |
| `CommandGetPrimaryKeys` | Query `information_schema` (not in ADBC; driver-specific) |
| `CommandGetImportedKeys` | Query `information_schema` |
| `CommandGetExportedKeys` | Query `information_schema` |
| `CommandGetCrossReference` | Query `information_schema` |

**Schema Translation**: ADBC `get_objects` returns a deeply nested struct (catalog > schema > table > column). FlightSQL metadata commands return flat tables. The gateway must flatten/reshape the ADBC result to match each FlightSQL command's expected schema.

#### Prepared Statements

| FlightSQL Action | Gateway Action |
|---|---|
| `CreatePreparedStatement(sql)` | Create ADBC `Statement`, call `set_sql_query(sql)` + `prepare()`. Store in session under a new handle. Return handle + parameter schema + result schema. |
| `ClosePreparedStatement(handle)` | Remove from session's prepared map. Drop the ADBC statement. |
| `get_flight_info(CommandPreparedStatementQuery)` | Look up prepared handle. Return `FlightInfo` with ticket referencing the handle. |
| `do_get(prepared ticket)` | Look up prepared statement. Call `execute()`. Stream results. |
| `do_put(CommandPreparedStatementQuery + data)` | Bind parameters via `stmt.bind(batch)`. |
| `do_put(CommandPreparedStatementUpdate + data)` | Bind parameters, call `execute_update()`. Return row count. |

Prepared handles are random 128-bit tokens. They are scoped to a session.

#### Transactions

| FlightSQL Action | Gateway Action |
|---|---|
| `BeginTransaction` | Call `conn.set_option(AutoCommit(false))`. Generate a transaction ID. Store in session. |
| `EndTransaction(Commit)` | Call `conn.commit()`. Clear transaction ID. Restore autocommit. |
| `EndTransaction(Rollback)` | Call `conn.rollback()`. Clear transaction ID. Restore autocommit. |

Transaction IDs are opaque tokens returned to the client. The gateway validates them on subsequent requests.

#### Bulk Ingest

| FlightSQL Command | Gateway Action |
|---|---|
| `CommandStatementIngest` | Create ADBC `Statement`, set `StatementOption::TargetTable` and `IngestMode`. Call `bind_stream()` with the incoming `FlightData` stream converted to a `RecordBatchReader`. Call `execute_update()`. |

### Streaming Results

The gateway must convert ADBC's `Box<dyn RecordBatchReader + Send>` into a `Stream<FlightData>`:

```rust
fn reader_to_flight_stream(
    reader: Box<dyn RecordBatchReader + Send>,
) -> Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> {
    // 1. Extract schema from reader
    // 2. Encode schema as first FlightData message (Arrow IPC)
    // 3. For each RecordBatch, encode as FlightData (Arrow IPC)
    // Use FlightDataEncoderBuilder from arrow-flight crate
}
```

`FlightDataEncoderBuilder` from the `arrow-flight` crate handles IPC encoding.

### Error Mapping

| ADBC Status | gRPC Status |
|---|---|
| `NotImplemented` | `Unimplemented` |
| `NotFound` | `NotFound` |
| `AlreadyExists` | `AlreadyExists` |
| `InvalidArgument` | `InvalidArgument` |
| `InvalidState` | `FailedPrecondition` |
| `Unauthenticated` | `Unauthenticated` |
| `Unauthorized` | `PermissionDenied` |
| `Io` | `Unavailable` |
| `Internal` | `Internal` |
| `Unknown` | `Unknown` |

## Phased Rollout

### Phase 1: Dark Launch (PostgreSQL, basic queries)

**Scope**: Enough to run a FlightSQL client, connect to PostgreSQL, and execute SELECT/INSERT/UPDATE/DELETE.

- `do_handshake` -- basic auth (username/password forwarded to ADBC)
- `get_flight_info_statement` + `do_get_statement` -- SELECT queries
- `do_put_statement_update` -- DML statements
- `get_flight_info_table_types` + `do_get_table_types` -- table type metadata
- `get_flight_info_sql_info` + `do_get_sql_info` -- server info
- Error mapping
- Session management (create on handshake, timeout cleanup)
- Binary: `adbc-flightsql-gateway-server` with CLI flags for backend URI, bind address

**Test targets**: Connect from `adbc-flightsql` client (our own crate) and run basic queries against PostgreSQL. This validates the round-trip: FlightSQL client --> gateway --> PostgreSQL --> gateway --> FlightSQL client.

**Dark launch validation**: Run against a local PostgreSQL with these queries:
```sql
SELECT 1;
SELECT * FROM information_schema.tables LIMIT 5;
CREATE TABLE test_gateway (id INT, name TEXT);
INSERT INTO test_gateway VALUES (1, 'hello');
SELECT * FROM test_gateway;
DROP TABLE test_gateway;
```

### Phase 2: Catalog Metadata + Transactions

- All `CommandGet*` metadata commands (catalogs, schemas, tables, columns, keys)
- Transaction lifecycle (begin, commit, rollback)
- Prepared statements (create, bind, execute, close)
- MySQL backend validation
- SQLite backend validation

### Phase 3: Streaming + Ingest + Polish

- `CommandStatementIngest` (bulk data upload)
- Streaming large result sets (backpressure-aware)
- TLS support
- Session timeout / cleanup task
- Metrics / event logging
- `CommandGetPrimaryKeys`, `CommandGetImportedKeys`, `CommandGetExportedKeys`, `CommandGetCrossReference`

## Module Layout

```
crates/adbc-flightsql-gateway/
  Cargo.toml
  src/
    lib.rs              -- Public API: FlightSqlGateway<D>, GatewayConfig
    service.rs          -- impl FlightSqlService for FlightSqlGateway<D>
    session.rs          -- SessionState, session lifecycle, token generation
    query.rs            -- Query execution (get_flight_info_statement, do_get_statement)
    metadata.rs         -- Catalog metadata handlers (get_tables, get_catalogs, etc.)
    prepared.rs         -- Prepared statement lifecycle
    transaction.rs      -- Transaction lifecycle
    streaming.rs        -- RecordBatchReader -> Stream<FlightData> conversion
    error.rs            -- ADBC Error -> gRPC Status mapping
    server.rs           -- Standalone server binary setup (GatewayConfig -> tonic::Server)
```

## Open Questions

1. **Connection pooling**: Should the gateway pool ADBC connections, or is 1:1 session-to-connection sufficient for v1? Leaning toward 1:1 for simplicity.

2. **Schema in GetFlightInfo**: Strategy A (lazy, empty schema) is simpler but some clients may require schema upfront. Need to test with real clients (DBeaver, Superset).

3. **Key metadata**: `CommandGetPrimaryKeys` and foreign key commands have no direct ADBC equivalent. Options:
   - (a) Execute raw `information_schema` queries (driver-specific SQL).
   - (b) Use `get_objects(ObjectDepth::Columns)` which includes constraint info in nested structs.
   - (c) Return `Unimplemented` in Phase 1.
   Leaning toward (c) for Phase 1, (b) for Phase 2.

4. **Multi-backend routing**: Should one gateway instance support multiple backends simultaneously (e.g., route by catalog name)? Out of scope for v1 but worth considering in the type design.

## Testing Strategy

- **Unit tests**: Mock ADBC connections (fake in-memory driver) to test RPC routing and session management.
- **Integration tests**: Gateway + `adbc-flightsql` client + real PostgreSQL (Docker).
- **Round-trip tests**: Verify that data survives the full path: client -> gateway -> PostgreSQL -> gateway -> client with type fidelity (especially Decimal, Timestamp, List types).
- **Existing client compatibility**: Test with the `adbc-flightsql` crate as the client since it's already in the workspace.
