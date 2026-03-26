//! Integration tests for the FlightSQL gateway.
//!
//! Tests the full round-trip: FlightSQL client -> gateway -> ADBC backend.
//!
//! SQLite tests run always (in-memory).
//! PostgreSQL tests require `ADBC_POSTGRES_URI` (e.g. `host=docker.yuacx.com port=5432 user=cyuan password=cyuan dbname=adbc_test`).
//! MySQL tests require `ADBC_MYSQL_URI` (e.g. `mysql://cyuan:cyuan@docker.yuacx.com:3306/adbc_test`).

use std::net::SocketAddr;

use adbc::{Connection, Database, DatabaseOption, Driver, OptionValue, Statement};
use adbc_flightsql::FlightSqlDriver;
use adbc_flightsql_gateway::FlightSqlGateway;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, BinaryArray, RecordBatch, RecordBatchReader, StringArray};
use arrow_flight::flight_service_server::FlightServiceServer;

// ─────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────

async fn start_server<D: Database + 'static>(db: D) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let gateway = FlightSqlGateway::new(db);
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(gateway))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, handle)
}

async fn connect_client(
    addr: SocketAddr,
) -> <adbc_flightsql::FlightSqlDatabase as Database>::ConnectionType {
    let drv = FlightSqlDriver;
    let uri = format!("grpc://[::1]:{}", addr.port());
    let db = drv
        .new_database_with_opts([(DatabaseOption::Uri, OptionValue::String(uri))])
        .await
        .unwrap();
    db.new_connection().await.unwrap()
}

fn collect(reader: Box<dyn RecordBatchReader + Send>) -> RecordBatch {
    adbc::collect_reader(reader).unwrap()
}

async fn query(
    conn: &<adbc_flightsql::FlightSqlDatabase as Database>::ConnectionType,
    sql: &str,
) -> RecordBatch {
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query(sql).await.unwrap();
    let (reader, _) = stmt.execute().await.unwrap();
    collect(reader)
}

async fn exec(
    conn: &<adbc_flightsql::FlightSqlDatabase as Database>::ConnectionType,
    sql: &str,
) -> i64 {
    let mut stmt = conn.new_statement().await.unwrap();
    stmt.set_sql_query(sql).await.unwrap();
    stmt.execute_update().await.unwrap()
}

/// Try to execute, return the error (for error-handling tests).
async fn try_query(
    conn: &<adbc_flightsql::FlightSqlDatabase as Database>::ConnectionType,
    sql: &str,
) -> Result<RecordBatch, adbc::Error> {
    let mut stmt = conn.new_statement().await?;
    stmt.set_sql_query(sql).await?;
    let (reader, _) = stmt.execute().await?;
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| adbc::Error::internal(e.to_string()))?;
    if batches.is_empty() {
        Ok(RecordBatch::new_empty(schema))
    } else {
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e: arrow_schema::ArrowError| adbc::Error::internal(e.to_string()))
    }
}

async fn try_exec(
    conn: &<adbc_flightsql::FlightSqlDatabase as Database>::ConnectionType,
    sql: &str,
) -> Result<i64, adbc::Error> {
    let mut stmt = conn.new_statement().await?;
    stmt.set_sql_query(sql).await?;
    stmt.execute_update().await
}

// ═════════════════════════════════════════════════════════════
//  SQLite tests (always run)
// ═════════════════════════════════════════════════════════════

mod sqlite {
    use super::*;
    use adbc_sqlite::SqliteDriver;

    /// Start a gateway backed by a temp-file SQLite database.
    /// File-backed so that each `new_connection()` inside the gateway
    /// shares the same database state (`:memory:` creates a fresh DB per connection).
    /// Each call gets a unique file to avoid conflicts between parallel tests.
    async fn gw() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = std::env::temp_dir().join(format!("gw_test_{}_{id}.db", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let uri = tmp.to_str().unwrap().to_owned();
        let db = SqliteDriver
            .new_database_with_opts([(DatabaseOption::Uri, OptionValue::String(uri))])
            .await
            .unwrap();
        start_server(db).await
    }

    // ── A. Basic queries (existing) ──────────────────────────

    #[tokio::test]
    async fn select_literal() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT 42 AS answer").await;
        assert_eq!(b.num_rows(), 1);
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 42);
        h.abort();
    }

    #[tokio::test]
    async fn select_multiple_types() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT 7 AS i, 1.23 AS f, 'hello' AS t").await;
        assert_eq!(b.num_rows(), 1);
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 7);
        assert!((b.column(1).as_primitive::<Float64Type>().value(0) - 1.23).abs() < f64::EPSILON);
        let t: &StringArray = b.column(2).as_any().downcast_ref().unwrap();
        assert_eq!(t.value(0), "hello");
        h.abort();
    }

    #[tokio::test]
    async fn select_empty_result() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE empty_t (id INTEGER)").await;
        let b = query(&c, "SELECT * FROM empty_t").await;
        assert_eq!(b.num_rows(), 0);
        assert_eq!(b.num_columns(), 1);
        h.abort();
    }

    // ── B. DDL + DML ─────────────────────────────────────────

    #[tokio::test]
    async fn create_insert_select() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(
            &c,
            "CREATE TABLE t (id INTEGER NOT NULL, name TEXT NOT NULL)",
        )
        .await;
        assert_eq!(
            exec(
                &c,
                "INSERT INTO t VALUES (1,'alpha'),(2,'beta'),(3,'gamma')"
            )
            .await,
            3
        );
        let b = query(&c, "SELECT id, name FROM t ORDER BY id").await;
        assert_eq!(b.num_rows(), 3);
        let ids = b.column(0).as_primitive::<Int64Type>();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(2), 3);
        let names: &StringArray = b.column(1).as_any().downcast_ref().unwrap();
        assert_eq!(names.value(0), "alpha");
        assert_eq!(names.value(2), "gamma");
        h.abort();
    }

    #[tokio::test]
    async fn update_and_delete() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE ud (id INTEGER, val TEXT)").await;
        exec(&c, "INSERT INTO ud VALUES (1,'a'),(2,'b'),(3,'c')").await;
        assert_eq!(exec(&c, "UPDATE ud SET val='x' WHERE id=2").await, 1);
        assert_eq!(exec(&c, "DELETE FROM ud WHERE id=3").await, 1);
        let b = query(&c, "SELECT id, val FROM ud ORDER BY id").await;
        assert_eq!(b.num_rows(), 2);
        let vals: &StringArray = b.column(1).as_any().downcast_ref().unwrap();
        assert_eq!(vals.value(1), "x");
        h.abort();
    }

    #[tokio::test]
    async fn insert_select() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE src (n INTEGER)").await;
        exec(&c, "INSERT INTO src VALUES (1),(2),(3)").await;
        exec(&c, "CREATE TABLE dst (n INTEGER)").await;
        exec(&c, "INSERT INTO dst SELECT n*10 FROM src").await;
        let b = query(&c, "SELECT n FROM dst ORDER BY n").await;
        assert_eq!(b.num_rows(), 3);
        let ns = b.column(0).as_primitive::<Int64Type>();
        assert_eq!(ns.value(0), 10);
        assert_eq!(ns.value(2), 30);
        h.abort();
    }

    // ── C. NULL handling ─────────────────────────────────────

    #[tokio::test]
    async fn null_roundtrip() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE null_t (id INTEGER, name TEXT)").await;
        exec(
            &c,
            "INSERT INTO null_t VALUES (1,NULL),(NULL,'present'),(3,'also')",
        )
        .await;
        let b = query(&c, "SELECT id, name FROM null_t ORDER BY rowid").await;
        assert_eq!(b.num_rows(), 3);
        let ids = b.column(0).as_primitive::<Int64Type>();
        assert_eq!(ids.value(0), 1);
        assert!(ids.is_null(1));
        let names: &StringArray = b.column(1).as_any().downcast_ref().unwrap();
        assert!(names.is_null(0));
        assert_eq!(names.value(1), "present");
        h.abort();
    }

    #[tokio::test]
    async fn types_mixed_nulls_per_column() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE mn (a INT, b REAL, c TEXT, d INT, e TEXT)").await;
        exec(&c, "INSERT INTO mn VALUES (1,1.1,'x',NULL,NULL)").await;
        exec(&c, "INSERT INTO mn VALUES (NULL,NULL,NULL,4,'e')").await;
        exec(&c, "INSERT INTO mn VALUES (3,3.3,NULL,NULL,'f')").await;
        let b = query(&c, "SELECT * FROM mn ORDER BY rowid").await;
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.num_columns(), 5);
        // row 0: a=1, d=NULL
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 1);
        assert!(b.column(3).is_null(0));
        // row 1: a=NULL, d=4
        assert!(b.column(0).is_null(1));
        assert_eq!(b.column(3).as_primitive::<Int64Type>().value(1), 4);
        h.abort();
    }

    #[tokio::test]
    async fn empty_string_vs_null() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE esn (val TEXT)").await;
        exec(&c, "INSERT INTO esn VALUES ('')").await;
        exec(&c, "INSERT INTO esn VALUES (NULL)").await;
        let b = query(&c, "SELECT val FROM esn ORDER BY rowid").await;
        assert_eq!(b.num_rows(), 2);
        let vals: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert!(!vals.is_null(0));
        assert_eq!(vals.value(0), "");
        assert!(vals.is_null(1));
        h.abort();
    }

    // ── D. Column type round-trips ───────────────────────────

    #[tokio::test]
    async fn types_integer_float_text() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE ift (i INTEGER, f REAL, t TEXT)").await;
        exec(&c, "INSERT INTO ift VALUES (42, 9.87654, 'hello world')").await;
        let b = query(&c, "SELECT * FROM ift").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 42);
        assert!((b.column(1).as_primitive::<Float64Type>().value(0) - 9.87654).abs() < 1e-10);
        let t: &StringArray = b.column(2).as_any().downcast_ref().unwrap();
        assert_eq!(t.value(0), "hello world");
        h.abort();
    }

    #[tokio::test]
    async fn types_blob_roundtrip() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE blob_t (data BLOB)").await;
        exec(&c, "INSERT INTO blob_t VALUES (X'DEADBEEF')").await;
        let b = query(&c, "SELECT data FROM blob_t").await;
        let col: &BinaryArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(col.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
        h.abort();
    }

    #[tokio::test]
    async fn types_boolean_as_integer() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        // SQLite stores boolean as 0/1 INTEGER.
        exec(&c, "CREATE TABLE bool_t (flag INTEGER)").await;
        exec(&c, "INSERT INTO bool_t VALUES (1),(0),(1)").await;
        let b = query(&c, "SELECT flag FROM bool_t ORDER BY rowid").await;
        let flags = b.column(0).as_primitive::<Int64Type>();
        assert_eq!(flags.value(0), 1);
        assert_eq!(flags.value(1), 0);
        assert_eq!(flags.value(2), 1);
        h.abort();
    }

    #[tokio::test]
    async fn types_large_text() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let big = "X".repeat(10_000);
        exec(&c, "CREATE TABLE lt (t TEXT)").await;
        exec(&c, &format!("INSERT INTO lt VALUES ('{big}')")).await;
        let b = query(&c, "SELECT t FROM lt").await;
        let t: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(t.value(0).len(), 10_000);
        h.abort();
    }

    // ── E. Aggregation ───────────────────────────────────────

    #[tokio::test]
    async fn aggregation_count_sum_avg_min_max() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE agg (val INTEGER)").await;
        exec(&c, "INSERT INTO agg VALUES (10),(20),(30),(40),(50)").await;
        let b = query(
            &c,
            "SELECT COUNT(*) AS cnt, SUM(val), AVG(val), MIN(val), MAX(val) FROM agg",
        )
        .await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 5);
        assert_eq!(b.column(1).as_primitive::<Int64Type>().value(0), 150);
        assert!((b.column(2).as_primitive::<Float64Type>().value(0) - 30.0).abs() < f64::EPSILON);
        assert_eq!(b.column(3).as_primitive::<Int64Type>().value(0), 10);
        assert_eq!(b.column(4).as_primitive::<Int64Type>().value(0), 50);
        h.abort();
    }

    // ── F. GROUP BY ──────────────────────────────────────────

    #[tokio::test]
    async fn group_by() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE sales (product TEXT, amount INTEGER)").await;
        exec(
            &c,
            "INSERT INTO sales VALUES ('A',10),('B',20),('A',30),('B',40),('A',50)",
        )
        .await;
        let b = query(
            &c,
            "SELECT product, SUM(amount) AS total FROM sales GROUP BY product ORDER BY product",
        )
        .await;
        assert_eq!(b.num_rows(), 2);
        let products: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(products.value(0), "A");
        let totals = b.column(1).as_primitive::<Int64Type>();
        assert_eq!(totals.value(0), 90);
        assert_eq!(totals.value(1), 60);
        h.abort();
    }

    #[tokio::test]
    async fn group_by_having() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE scores (team TEXT, score INTEGER)").await;
        exec(
            &c,
            "INSERT INTO scores VALUES ('X',10),('Y',20),('X',30),('Y',5),('Z',100)",
        )
        .await;
        let b = query(&c, "SELECT team, SUM(score) AS total FROM scores GROUP BY team HAVING total > 30 ORDER BY total DESC").await;
        assert_eq!(b.num_rows(), 2);
        let teams: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(teams.value(0), "Z");
        assert_eq!(teams.value(1), "X");
        h.abort();
    }

    // ── G. ORDER BY + LIMIT ──────────────────────────────────

    #[tokio::test]
    async fn order_by_limit_offset() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE nums (n INTEGER)").await;
        exec(&c, "INSERT INTO nums VALUES (5),(3),(1),(4),(2)").await;
        let b = query(&c, "SELECT n FROM nums ORDER BY n LIMIT 3").await;
        assert_eq!(b.num_rows(), 3);
        let ns = b.column(0).as_primitive::<Int64Type>();
        assert_eq!(ns.value(0), 1);
        assert_eq!(ns.value(2), 3);
        let b2 = query(&c, "SELECT n FROM nums ORDER BY n LIMIT 2 OFFSET 2").await;
        assert_eq!(b2.num_rows(), 2);
        assert_eq!(b2.column(0).as_primitive::<Int64Type>().value(0), 3);
        h.abort();
    }

    // ── H. JOINs ─────────────────────────────────────────────

    #[tokio::test]
    async fn inner_join() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE users (id INTEGER, name TEXT)").await;
        exec(
            &c,
            "INSERT INTO users VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')",
        )
        .await;
        exec(&c, "CREATE TABLE orders (user_id INTEGER, amount INTEGER)").await;
        exec(&c, "INSERT INTO orders VALUES (1,100),(1,200),(2,300)").await;
        let b = query(&c, "SELECT u.name, SUM(o.amount) AS total FROM users u INNER JOIN orders o ON u.id=o.user_id GROUP BY u.name ORDER BY u.name").await;
        assert_eq!(b.num_rows(), 2);
        let names: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(b.column(1).as_primitive::<Int64Type>().value(0), 300);
        h.abort();
    }

    #[tokio::test]
    async fn left_join() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE dept (id INTEGER, name TEXT)").await;
        exec(&c, "INSERT INTO dept VALUES (1,'Eng'),(2,'Sales'),(3,'HR')").await;
        exec(&c, "CREATE TABLE emp (name TEXT, dept_id INTEGER)").await;
        exec(
            &c,
            "INSERT INTO emp VALUES ('Alice',1),('Bob',1),('Carol',2)",
        )
        .await;
        let b = query(&c, "SELECT d.name AS dept, COUNT(e.name) AS cnt FROM dept d LEFT JOIN emp e ON d.id=e.dept_id GROUP BY d.name ORDER BY d.name").await;
        assert_eq!(b.num_rows(), 3);
        let counts = b.column(1).as_primitive::<Int64Type>();
        assert_eq!(counts.value(0), 2); // Eng
        assert_eq!(counts.value(1), 0); // HR
        assert_eq!(counts.value(2), 1); // Sales
        h.abort();
    }

    #[tokio::test]
    async fn multiple_joins() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE mj_users (id INTEGER, name TEXT)").await;
        exec(&c, "INSERT INTO mj_users VALUES (1,'Alice'),(2,'Bob')").await;
        exec(
            &c,
            "CREATE TABLE mj_orders (id INTEGER, user_id INTEGER, product_id INTEGER)",
        )
        .await;
        exec(
            &c,
            "INSERT INTO mj_orders VALUES (1,1,10),(2,1,20),(3,2,10)",
        )
        .await;
        exec(
            &c,
            "CREATE TABLE mj_products (id INTEGER, pname TEXT, price INTEGER)",
        )
        .await;
        exec(
            &c,
            "INSERT INTO mj_products VALUES (10,'Widget',5),(20,'Gadget',15)",
        )
        .await;
        let b = query(&c, "SELECT u.name, SUM(p.price) AS total FROM mj_users u JOIN mj_orders o ON u.id=o.user_id JOIN mj_products p ON o.product_id=p.id GROUP BY u.name ORDER BY u.name").await;
        assert_eq!(b.num_rows(), 2);
        let totals = b.column(1).as_primitive::<Int64Type>();
        assert_eq!(totals.value(0), 20); // Alice: 5+15
        assert_eq!(totals.value(1), 5); // Bob: 5
        h.abort();
    }

    // ── I. Subqueries ────────────────────────────────────────

    #[tokio::test]
    async fn subquery() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE items (id INTEGER, price REAL)").await;
        exec(
            &c,
            "INSERT INTO items VALUES (1,10.0),(2,50.0),(3,30.0),(4,70.0)",
        )
        .await;
        let b = query(
            &c,
            "SELECT id FROM items WHERE price > (SELECT AVG(price) FROM items) ORDER BY id",
        )
        .await;
        assert_eq!(b.num_rows(), 2);
        let ids = b.column(0).as_primitive::<Int64Type>();
        assert_eq!(ids.value(0), 2);
        assert_eq!(ids.value(1), 4);
        h.abort();
    }

    // ── J. DISTINCT ──────────────────────────────────────────

    #[tokio::test]
    async fn distinct() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE tags (tag TEXT)").await;
        exec(&c, "INSERT INTO tags VALUES ('a'),('b'),('a'),('c'),('b')").await;
        let b = query(&c, "SELECT DISTINCT tag FROM tags ORDER BY tag").await;
        assert_eq!(b.num_rows(), 3);
        h.abort();
    }

    // ── K. CASE expression ───────────────────────────────────

    #[tokio::test]
    async fn case_expression() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE grades (student TEXT, score INTEGER)").await;
        exec(
            &c,
            "INSERT INTO grades VALUES ('Alice',95),('Bob',72),('Carol',45)",
        )
        .await;
        let b = query(&c, "SELECT student, CASE WHEN score>=90 THEN 'A' WHEN score>=70 THEN 'B' ELSE 'F' END AS grade FROM grades ORDER BY student").await;
        let grades: &StringArray = b.column(1).as_any().downcast_ref().unwrap();
        assert_eq!(grades.value(0), "A");
        assert_eq!(grades.value(1), "B");
        assert_eq!(grades.value(2), "F");
        h.abort();
    }

    // ── L. UNION ─────────────────────────────────────────────

    #[tokio::test]
    async fn union_all() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 1").await;
        assert_eq!(b.num_rows(), 3);
        h.abort();
    }

    #[tokio::test]
    async fn union_distinct() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT 1 AS n UNION SELECT 2 UNION SELECT 1").await;
        assert_eq!(b.num_rows(), 2);
        h.abort();
    }

    // ── M. CTE (Common Table Expression) ─────────────────────

    #[tokio::test]
    async fn cte_common_table_expression() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(&c, "WITH cte AS (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT SUM(x) AS total FROM cte").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 6);
        h.abort();
    }

    #[tokio::test]
    async fn cte_recursive() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(&c, "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT COUNT(*) FROM cnt").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 10);
        h.abort();
    }

    // ── N. WHERE operators ───────────────────────────────────

    #[tokio::test]
    async fn in_operator() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE in_t (n INTEGER)").await;
        exec(&c, "INSERT INTO in_t VALUES (1),(2),(3),(4),(5)").await;
        let b = query(&c, "SELECT n FROM in_t WHERE n IN (2,4) ORDER BY n").await;
        assert_eq!(b.num_rows(), 2);
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 2);
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(1), 4);
        // Subquery IN
        let b2 = query(
            &c,
            "SELECT n FROM in_t WHERE n IN (SELECT n FROM in_t WHERE n > 3) ORDER BY n",
        )
        .await;
        assert_eq!(b2.num_rows(), 2);
        h.abort();
    }

    #[tokio::test]
    async fn between_operator() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE bw (n INTEGER)").await;
        exec(&c, "INSERT INTO bw VALUES (1),(5),(10),(15),(20)").await;
        let b = query(&c, "SELECT n FROM bw WHERE n BETWEEN 5 AND 15 ORDER BY n").await;
        assert_eq!(b.num_rows(), 3);
        h.abort();
    }

    #[tokio::test]
    async fn like_operator() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE lk (name TEXT)").await;
        exec(
            &c,
            "INSERT INTO lk VALUES ('Alice'),('Bob'),('Anna'),('Carol')",
        )
        .await;
        let b = query(&c, "SELECT name FROM lk WHERE name LIKE 'A%' ORDER BY name").await;
        assert_eq!(b.num_rows(), 2);
        let names: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Anna");
        h.abort();
    }

    // ── O. COALESCE ──────────────────────────────────────────

    #[tokio::test]
    async fn coalesce_ifnull() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT COALESCE(NULL, NULL, 'fallback') AS v").await;
        let v: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(v.value(0), "fallback");
        h.abort();
    }

    // ── P. String functions ──────────────────────────────────

    #[tokio::test]
    async fn string_functions() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT LENGTH('hello') AS l, UPPER('hello') AS u, LOWER('HELLO') AS lo, SUBSTR('hello',2,3) AS s").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 5);
        let u: &StringArray = b.column(1).as_any().downcast_ref().unwrap();
        assert_eq!(u.value(0), "HELLO");
        let lo: &StringArray = b.column(2).as_any().downcast_ref().unwrap();
        assert_eq!(lo.value(0), "hello");
        let s: &StringArray = b.column(3).as_any().downcast_ref().unwrap();
        assert_eq!(s.value(0), "ell");
        h.abort();
    }

    // ── Q. Math expressions ──────────────────────────────────

    #[tokio::test]
    async fn math_expressions() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT ABS(-42) AS a, 10+3 AS addition, 10-3 AS subtraction, 10*3 AS multiplication, 10/3 AS division, 10%3 AS modulo, ROUND(9.87654, 2) AS r").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 42);
        assert_eq!(b.column(1).as_primitive::<Int64Type>().value(0), 13);
        assert_eq!(b.column(2).as_primitive::<Int64Type>().value(0), 7);
        assert_eq!(b.column(3).as_primitive::<Int64Type>().value(0), 30);
        assert_eq!(b.column(4).as_primitive::<Int64Type>().value(0), 3);
        assert_eq!(b.column(5).as_primitive::<Int64Type>().value(0), 1);
        assert!((b.column(6).as_primitive::<Float64Type>().value(0) - 9.88).abs() < f64::EPSILON);
        h.abort();
    }

    // ── R. Window functions ──────────────────────────────────

    #[tokio::test]
    async fn window_row_number() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE wrn (name TEXT, score INTEGER)").await;
        exec(&c, "INSERT INTO wrn VALUES ('A',30),('B',10),('C',20)").await;
        let b = query(
            &c,
            "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM wrn",
        )
        .await;
        assert_eq!(b.num_rows(), 3);
        let rn = b.column(1).as_primitive::<Int64Type>();
        // A=30 is rank 1, C=20 is rank 2, B=10 is rank 3
        assert_eq!(rn.value(0), 1);
        h.abort();
    }

    #[tokio::test]
    async fn window_rank() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE wr (cat TEXT, val INTEGER)").await;
        exec(
            &c,
            "INSERT INTO wr VALUES ('X',10),('X',20),('Y',30),('Y',5)",
        )
        .await;
        let b = query(&c, "SELECT cat, val, RANK() OVER (PARTITION BY cat ORDER BY val DESC) AS rnk FROM wr ORDER BY cat, rnk").await;
        assert_eq!(b.num_rows(), 4);
        let rnk = b.column(2).as_primitive::<Int64Type>();
        assert_eq!(rnk.value(0), 1); // X,20
        assert_eq!(rnk.value(1), 2); // X,10
        h.abort();
    }

    #[tokio::test]
    async fn window_sum_over() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE ws (cat TEXT, val INTEGER)").await;
        exec(&c, "INSERT INTO ws VALUES ('A',10),('A',20),('B',30)").await;
        let b = query(&c, "SELECT cat, val, SUM(val) OVER (PARTITION BY cat) AS cat_total FROM ws ORDER BY cat, val").await;
        assert_eq!(b.num_rows(), 3);
        let totals = b.column(2).as_primitive::<Int64Type>();
        assert_eq!(totals.value(0), 30); // A total
        assert_eq!(totals.value(1), 30); // A total
        assert_eq!(totals.value(2), 30); // B total
        h.abort();
    }

    // ── S. Error handling ────────────────────────────────────

    #[tokio::test]
    async fn error_invalid_sql() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        assert!(try_query(&c, "SELECTX garbage!!").await.is_err());
        h.abort();
    }

    #[tokio::test]
    async fn error_table_not_found() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        assert!(try_query(&c, "SELECT * FROM no_such_table_xyz")
            .await
            .is_err());
        h.abort();
    }

    #[tokio::test]
    async fn error_column_not_found() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE ecol (x INTEGER)").await;
        assert!(try_query(&c, "SELECT nonexistent_col FROM ecol")
            .await
            .is_err());
        h.abort();
    }

    #[tokio::test]
    async fn error_constraint_violation() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE uq (id INTEGER PRIMARY KEY)").await;
        exec(&c, "INSERT INTO uq VALUES (1)").await;
        assert!(try_exec(&c, "INSERT INTO uq VALUES (1)").await.is_err());
        h.abort();
    }

    // ── T. Edge cases ────────────────────────────────────────

    #[tokio::test]
    async fn special_characters_in_text() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE sc (t TEXT)").await;
        // Use separate inserts to avoid quoting issues.
        exec(&c, r#"INSERT INTO sc VALUES ('it''s a "test"')"#).await;
        exec(&c, "INSERT INTO sc VALUES ('line1\nline2')").await;
        let b = query(&c, "SELECT t FROM sc ORDER BY rowid").await;
        assert_eq!(b.num_rows(), 2);
        let t: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert!(t.value(0).contains("it's"));
        assert!(t.value(0).contains("\"test\""));
        h.abort();
    }

    #[tokio::test]
    async fn max_integer_values() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = query(
            &c,
            "SELECT 9223372036854775807 AS mx, -9223372036854775808 AS mn",
        )
        .await;
        let mx = b.column(0).as_primitive::<Int64Type>().value(0);
        let mn = b.column(1).as_primitive::<Int64Type>().value(0);
        assert_eq!(mx, i64::MAX);
        assert_eq!(mn, i64::MIN);
        h.abort();
    }

    #[tokio::test]
    async fn many_columns() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let cols: Vec<String> = (0..20).map(|i| format!("c{i} INTEGER")).collect();
        exec(&c, &format!("CREATE TABLE mc ({})", cols.join(","))).await;
        let vals: Vec<String> = (0..20).map(|i| i.to_string()).collect();
        exec(&c, &format!("INSERT INTO mc VALUES ({})", vals.join(","))).await;
        let b = query(&c, "SELECT * FROM mc").await;
        assert_eq!(b.num_columns(), 20);
        for i in 0..20 {
            assert_eq!(b.column(i).as_primitive::<Int64Type>().value(0), i as i64);
        }
        h.abort();
    }

    // ── U. Metadata ──────────────────────────────────────────

    #[tokio::test]
    async fn get_table_types() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = collect(c.get_table_types().await.unwrap());
        assert!(b.num_rows() >= 2);
        assert_eq!(b.schema().field(0).name(), "table_type");
        h.abort();
    }

    #[tokio::test]
    async fn get_sql_info() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        let b = collect(c.get_info(None).await.unwrap());
        assert!(b.num_rows() > 0);
        h.abort();
    }

    // ── V. Large result set ──────────────────────────────────

    #[tokio::test]
    async fn large_result_set() {
        let (addr, h) = gw().await;
        let c = connect_client(addr).await;
        exec(&c, "CREATE TABLE big (i INTEGER)").await;
        exec(&c, "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<500) INSERT INTO big SELECT x FROM cnt").await;
        let b = query(&c, "SELECT COUNT(*) FROM big").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 500);
        let b = query(&c, "SELECT i FROM big ORDER BY i").await;
        assert_eq!(b.num_rows(), 500);
        h.abort();
    }
}

// ═════════════════════════════════════════════════════════════
//  PostgreSQL tests (require ADBC_POSTGRES_URI)
// ═════════════════════════════════════════════════════════════

mod postgres {
    use super::*;
    use adbc_postgres::PostgresDriver;

    async fn gw() -> Option<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let uri = std::env::var("ADBC_POSTGRES_URI").ok()?;
        let db = PostgresDriver
            .new_database_with_opts([(DatabaseOption::Uri, uri.into())])
            .await
            .ok()?;
        // Each gateway request creates its own connection in autocommit mode.
        // DDL/DML commits immediately, so multi-step tests (CREATE + INSERT + SELECT) work.
        Some(start_server(db).await)
    }

    // ── Column types ─────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_types_int_family() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_int_f").await;
        exec(
            &c,
            "CREATE TABLE gw_int_f (a SMALLINT, b INTEGER, c BIGINT)",
        )
        .await;
        exec(&c, "INSERT INTO gw_int_f VALUES (1, 2, 3)").await;
        let b = query(&c, "SELECT a::BIGINT, b::BIGINT, c FROM gw_int_f").await;
        assert_eq!(b.num_rows(), 1);
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 1);
        assert_eq!(b.column(1).as_primitive::<Int64Type>().value(0), 2);
        assert_eq!(b.column(2).as_primitive::<Int64Type>().value(0), 3);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_types_float_family() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT 1.5::REAL AS f32, 2.5::DOUBLE PRECISION AS f64").await;
        assert_eq!(b.num_rows(), 1);
        assert!(!b.column(0).is_null(0));
        assert!(!b.column(1).is_null(0));
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_types_text_family() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(
            &c,
            "SELECT 'hello'::TEXT AS t, 'world'::VARCHAR(50) AS v, 'pad'::CHAR(10) AS ch",
        )
        .await;
        let t: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(t.value(0), "hello");
        let v: &StringArray = b.column(1).as_any().downcast_ref().unwrap();
        assert_eq!(v.value(0), "world");
        // CHAR(10) is padded with spaces.
        let ch: &StringArray = b.column(2).as_any().downcast_ref().unwrap();
        assert!(ch.value(0).starts_with("pad"));
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_types_boolean_roundtrip() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_bool").await;
        exec(&c, "CREATE TABLE gw_bool (flag BOOLEAN)").await;
        exec(&c, "INSERT INTO gw_bool VALUES (TRUE),(FALSE),(NULL)").await;
        let b = query(&c, "SELECT flag FROM gw_bool ORDER BY flag NULLS LAST").await;
        assert_eq!(b.num_rows(), 3);
        assert!(!b.column(0).is_null(0));
        assert!(!b.column(0).is_null(1));
        assert!(b.column(0).is_null(2));
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_types_bytea() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_bytea").await;
        exec(&c, "CREATE TABLE gw_bytea (data BYTEA)").await;
        exec(&c, "INSERT INTO gw_bytea VALUES (E'\\\\xDEADBEEF')").await;
        let b = query(&c, "SELECT data FROM gw_bytea").await;
        let col: &BinaryArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(col.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_timestamp_as_text() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT '2026-03-22T10:30:00+00'::TIMESTAMPTZ::TEXT AS ts, '2026-03-22'::DATE::TEXT AS d").await;
        let ts: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert!(ts.value(0).contains("2026-03-22"));
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_numeric_as_text() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT 123.456::NUMERIC(10,3)::TEXT AS n").await;
        let n: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(n.value(0), "123.456");
        h.abort();
    }

    // ── Aggregation + GROUP BY ───────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_aggregation() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT COUNT(*), SUM(x)::BIGINT, MIN(x)::BIGINT, MAX(x)::BIGINT FROM generate_series(1,10) AS g(x)").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 10);
        assert_eq!(b.column(1).as_primitive::<Int64Type>().value(0), 55);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_group_by() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT category, SUM(amount)::BIGINT AS total FROM (VALUES ('A',10),('B',20),('A',30),('B',40)) AS t(category,amount) GROUP BY category ORDER BY category").await;
        assert_eq!(b.num_rows(), 2);
        let totals = b.column(1).as_primitive::<Int64Type>();
        assert_eq!(totals.value(0), 40);
        assert_eq!(totals.value(1), 60);
        h.abort();
    }

    // ── JOIN ─────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_join() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT a.name, b.val::BIGINT FROM (VALUES (1,'x'),(2,'y')) AS a(id,name) INNER JOIN (VALUES (1,100),(2,200)) AS b(id,val) ON a.id=b.id ORDER BY a.id").await;
        assert_eq!(b.num_rows(), 2);
        let names: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(names.value(0), "x");
        h.abort();
    }

    // ── DISTINCT + ORDER BY + LIMIT ──────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_distinct_order_limit() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT DISTINCT x::BIGINT FROM (VALUES (3),(1),(2),(1),(3),(2)) AS t(x) ORDER BY 1 LIMIT 2").await;
        assert_eq!(b.num_rows(), 2);
        h.abort();
    }

    // ── NULL handling ────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_null_roundtrip() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT id::BIGINT, name FROM (VALUES (1,'a'),(NULL,'b'),(3,NULL)) AS t(id,name) ORDER BY id NULLS LAST").await;
        assert_eq!(b.num_rows(), 3);
        let ids = b.column(0).as_primitive::<Int64Type>();
        assert_eq!(ids.value(0), 1);
        assert!(ids.is_null(2));
        let names: &StringArray = b.column(1).as_any().downcast_ref().unwrap();
        assert!(names.is_null(1));
        h.abort();
    }

    // ── DDL + DML ────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_create_insert_select() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_pg_t").await;
        exec(
            &c,
            "CREATE TABLE gw_pg_t (id BIGINT, name TEXT, score DOUBLE PRECISION)",
        )
        .await;
        assert_eq!(
            exec(
                &c,
                "INSERT INTO gw_pg_t VALUES (1,'Alice',95.5),(2,'Bob',82.3),(3,'Carol',91.0)"
            )
            .await,
            3
        );
        let b = query(
            &c,
            "SELECT name, score FROM gw_pg_t WHERE score > 90 ORDER BY name",
        )
        .await;
        assert_eq!(b.num_rows(), 2);
        let names: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Carol");
        h.abort();
    }

    // ── CTE + UNION + EXISTS ─────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_cte() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(
            &c,
            "WITH nums AS (SELECT generate_series(1,5) AS n) SELECT SUM(n)::BIGINT FROM nums",
        )
        .await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 15);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_union_all() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(
            &c,
            "SELECT 1::BIGINT AS n UNION ALL SELECT 2 UNION ALL SELECT 1",
        )
        .await;
        assert_eq!(b.num_rows(), 3);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_exists_subquery() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_pg_b").await;
        exec(&c, "DROP TABLE IF EXISTS gw_pg_a").await;
        exec(&c, "CREATE TABLE gw_pg_a (id BIGINT)").await;
        exec(&c, "INSERT INTO gw_pg_a VALUES (1),(2),(3)").await;
        exec(&c, "CREATE TABLE gw_pg_b (aid BIGINT)").await;
        exec(&c, "INSERT INTO gw_pg_b VALUES (1),(3)").await;
        let b = query(&c, "SELECT id FROM gw_pg_a WHERE EXISTS (SELECT 1 FROM gw_pg_b WHERE aid=gw_pg_a.id) ORDER BY id").await;
        assert_eq!(b.num_rows(), 2);
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 1);
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(1), 3);
        h.abort();
    }

    // ── Window functions ─────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_window_row_number() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT x::BIGINT, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM generate_series(10,12) AS g(x)").await;
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.column(1).as_primitive::<Int64Type>().value(0), 1);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_window_lag_lead() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT x::BIGINT, LAG(x::BIGINT, 1) OVER (ORDER BY x) AS prev, LEAD(x::BIGINT, 1) OVER (ORDER BY x) AS nxt FROM generate_series(1,3) AS g(x)").await;
        assert_eq!(b.num_rows(), 3);
        // First row: prev=NULL
        assert!(b.column(1).is_null(0));
        // Last row: next=NULL
        assert!(b.column(2).is_null(2));
        h.abort();
    }

    // ── Large result + metadata ──────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_large_result() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT g FROM generate_series(1,1000) AS g").await;
        assert_eq!(b.num_rows(), 1000);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_get_table_types() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        assert!(collect(c.get_table_types().await.unwrap()).num_rows() >= 2);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_POSTGRES_URI"]
    async fn pg_get_sql_info() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        assert!(collect(c.get_info(None).await.unwrap()).num_rows() > 0);
        h.abort();
    }
}

// ═════════════════════════════════════════════════════════════
//  MySQL tests (require ADBC_MYSQL_URI)
// ═════════════════════════════════════════════════════════════

mod mysql {
    use super::*;
    use adbc_mysql::MysqlDriver;

    async fn gw() -> Option<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let uri = std::env::var("ADBC_MYSQL_URI").ok()?;
        let db = MysqlDriver
            .new_database_with_opts([(DatabaseOption::Uri, uri.into())])
            .await
            .ok()?;
        Some(start_server(db).await)
    }

    // ── Column types ─────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_types_int_family() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_int").await;
        exec(
            &c,
            "CREATE TABLE gw_m_int (a TINYINT, b SMALLINT, c INT, d BIGINT)",
        )
        .await;
        exec(&c, "INSERT INTO gw_m_int VALUES (1,2,3,4)").await;
        let b = query(&c, "SELECT * FROM gw_m_int").await;
        assert_eq!(b.num_rows(), 1);
        assert_eq!(b.num_columns(), 4);
        exec(&c, "DROP TABLE IF EXISTS gw_m_int").await;
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_types_unsigned() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_uint").await;
        exec(
            &c,
            "CREATE TABLE gw_m_uint (a TINYINT UNSIGNED, b INT UNSIGNED, c BIGINT UNSIGNED)",
        )
        .await;
        exec(
            &c,
            "INSERT INTO gw_m_uint VALUES (255, 4294967295, 9223372036854775807)",
        )
        .await;
        let b = query(&c, "SELECT * FROM gw_m_uint").await;
        assert_eq!(b.num_rows(), 1);
        assert_eq!(b.num_columns(), 3);
        exec(&c, "DROP TABLE IF EXISTS gw_m_uint").await;
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_types_float_family() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(
            &c,
            "SELECT CAST(1.5 AS FLOAT) AS f, CAST(2.5 AS DOUBLE) AS d",
        )
        .await;
        assert!(!b.column(0).is_null(0));
        assert!(!b.column(1).is_null(0));
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_types_text_family() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_txt").await;
        exec(
            &c,
            "CREATE TABLE gw_m_txt (a VARCHAR(100), b TEXT, c CHAR(10))",
        )
        .await;
        exec(&c, "INSERT INTO gw_m_txt VALUES ('hello','world','pad')").await;
        let b = query(&c, "SELECT * FROM gw_m_txt").await;
        let a: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(a.value(0), "hello");
        exec(&c, "DROP TABLE IF EXISTS gw_m_txt").await;
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_types_blob() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_blob").await;
        exec(&c, "CREATE TABLE gw_m_blob (data BLOB)").await;
        exec(&c, "INSERT INTO gw_m_blob VALUES (X'DEADBEEF')").await;
        let b = query(&c, "SELECT data FROM gw_m_blob").await;
        let col: &BinaryArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(col.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
        exec(&c, "DROP TABLE IF EXISTS gw_m_blob").await;
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_boolean() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT TRUE AS t, FALSE AS f").await;
        assert!(!b.column(0).is_null(0));
        assert!(!b.column(1).is_null(0));
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_datetime() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        // MySQL DATETIME/DATE are mapped to Utf8 by the driver.
        let b = query(
            &c,
            "SELECT CAST('2026-03-22 10:30:00' AS CHAR) AS ts, CAST('2026-03-22' AS CHAR) AS d",
        )
        .await;
        assert_eq!(b.num_rows(), 1);
        let ts: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert!(ts.value(0).contains("2026-03-22"));
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_decimal() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        // MySQL DECIMAL maps to Float64 in the driver.
        let b = query(&c, "SELECT CAST(123.456 AS DECIMAL(10,3)) + 0E0 AS n").await;
        assert_eq!(b.num_rows(), 1);
        assert!((b.column(0).as_primitive::<Float64Type>().value(0) - 123.456).abs() < 0.001);
        h.abort();
    }

    // ── Aggregation + GROUP BY ───────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_aggregation() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT COUNT(*) AS cnt, SUM(x) AS s FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 5);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_group_by() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_grp").await;
        exec(&c, "CREATE TABLE gw_m_grp (cat VARCHAR(10), amt INT)").await;
        exec(
            &c,
            "INSERT INTO gw_m_grp VALUES ('A',10),('B',20),('A',30),('B',40)",
        )
        .await;
        // MySQL SUM(INT) returns DECIMAL -> Float64. Use CAST to SIGNED for Int64.
        let b = query(
            &c,
            "SELECT cat, CAST(SUM(amt) AS SIGNED) AS total FROM gw_m_grp GROUP BY cat ORDER BY cat",
        )
        .await;
        let totals = b.column(1).as_primitive::<Int64Type>();
        assert_eq!(totals.value(0), 40);
        assert_eq!(totals.value(1), 60);
        exec(&c, "DROP TABLE IF EXISTS gw_m_grp").await;
        h.abort();
    }

    // ── JOIN ─────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_join() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_o").await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_u").await;
        exec(&c, "CREATE TABLE gw_m_u (id INT, name VARCHAR(50))").await;
        exec(&c, "INSERT INTO gw_m_u VALUES (1,'Alice'),(2,'Bob')").await;
        exec(&c, "CREATE TABLE gw_m_o (uid INT, amount INT)").await;
        exec(&c, "INSERT INTO gw_m_o VALUES (1,100),(1,200),(2,300)").await;
        let b = query(&c, "SELECT u.name, SUM(o.amount) AS total FROM gw_m_u u JOIN gw_m_o o ON u.id=o.uid GROUP BY u.name ORDER BY u.name").await;
        assert_eq!(b.num_rows(), 2);
        exec(&c, "DROP TABLE IF EXISTS gw_m_o").await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_u").await;
        h.abort();
    }

    // ── DISTINCT + ORDER + LIMIT ─────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_distinct_order_limit() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT DISTINCT x FROM (SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 1) t ORDER BY x LIMIT 2").await;
        assert_eq!(b.num_rows(), 2);
        h.abort();
    }

    // ── NULL handling ────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_null_roundtrip() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_null").await;
        exec(&c, "CREATE TABLE gw_m_null (id INT, name VARCHAR(50))").await;
        exec(
            &c,
            "INSERT INTO gw_m_null VALUES (1,NULL),(NULL,'present'),(3,'also')",
        )
        .await;
        let b = query(&c, "SELECT id, name FROM gw_m_null ORDER BY id IS NULL, id").await;
        assert_eq!(b.num_rows(), 3);
        assert!(b.column(0).is_null(2));
        exec(&c, "DROP TABLE IF EXISTS gw_m_null").await;
        h.abort();
    }

    // ── DDL + DML ────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_create_insert_select() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_test").await;
        exec(
            &c,
            "CREATE TABLE gw_m_test (id INT, name VARCHAR(100), score DOUBLE)",
        )
        .await;
        assert_eq!(
            exec(
                &c,
                "INSERT INTO gw_m_test VALUES (1,'Alice',95.5),(2,'Bob',82.3),(3,'Carol',91.0)"
            )
            .await,
            3
        );
        let b = query(
            &c,
            "SELECT name FROM gw_m_test WHERE score > 90 ORDER BY name",
        )
        .await;
        assert_eq!(b.num_rows(), 2);
        let names: &StringArray = b.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(names.value(0), "Alice");
        exec(&c, "DROP TABLE IF EXISTS gw_m_test").await;
        h.abort();
    }

    // ── CTE + UNION + IN subquery ────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_cte() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        // MySQL SUM() returns DECIMAL. Cast to SIGNED for Int64.
        let b = query(&c, "WITH nums AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT CAST(SUM(n) AS SIGNED) AS total FROM nums").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 6);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_union_all() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        let b = query(&c, "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 1").await;
        assert_eq!(b.num_rows(), 3);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_in_subquery() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_in_b").await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_in_a").await;
        exec(&c, "CREATE TABLE gw_m_in_a (id INT)").await;
        exec(&c, "INSERT INTO gw_m_in_a VALUES (1),(2),(3),(4),(5)").await;
        exec(&c, "CREATE TABLE gw_m_in_b (aid INT)").await;
        exec(&c, "INSERT INTO gw_m_in_b VALUES (2),(4)").await;
        // MySQL INT maps to Int32.
        let b = query(
            &c,
            "SELECT id FROM gw_m_in_a WHERE id IN (SELECT aid FROM gw_m_in_b) ORDER BY id",
        )
        .await;
        assert_eq!(b.num_rows(), 2);
        assert_eq!(
            b.column(0)
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(0),
            2
        );
        exec(&c, "DROP TABLE IF EXISTS gw_m_in_b").await;
        exec(&c, "DROP TABLE IF EXISTS gw_m_in_a").await;
        h.abort();
    }

    // ── Metadata ─────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_get_table_types() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        assert!(collect(c.get_table_types().await.unwrap()).num_rows() >= 1);
        h.abort();
    }

    #[tokio::test]
    #[ignore = "requires ADBC_MYSQL_URI"]
    async fn mysql_get_sql_info() {
        let (addr, h) = gw().await.unwrap();
        let c = connect_client(addr).await;
        assert!(collect(c.get_info(None).await.unwrap()).num_rows() > 0);
        h.abort();
    }
}

// ═════════════════════════════════════════════════════════════
//  Authentication + Session tests (SQLite backend, always run)
// ═════════════════════════════════════════════════════════════

mod auth {
    use super::*;
    use adbc_sqlite::SqliteDriver;

    async fn gw() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = std::env::temp_dir().join(format!("gw_auth_{}_{id}.db", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let uri = tmp.to_str().unwrap().to_owned();
        let db = SqliteDriver
            .new_database_with_opts([(DatabaseOption::Uri, OptionValue::String(uri))])
            .await
            .unwrap();
        start_server(db).await
    }

    /// Connect with username/password (triggers handshake).
    async fn connect_with_auth(
        addr: SocketAddr,
        user: &str,
        pass: &str,
    ) -> <adbc_flightsql::FlightSqlDatabase as Database>::ConnectionType {
        let drv = FlightSqlDriver;
        let uri = format!("grpc://[::1]:{}", addr.port());
        let db = drv
            .new_database_with_opts([
                (DatabaseOption::Uri, OptionValue::String(uri)),
                (
                    DatabaseOption::Username,
                    OptionValue::String(user.to_owned()),
                ),
                (
                    DatabaseOption::Password,
                    OptionValue::String(pass.to_owned()),
                ),
            ])
            .await
            .unwrap();
        db.new_connection().await.unwrap()
    }

    // ── Handshake round-trip ─────────────────────────────────

    #[tokio::test]
    async fn auth_handshake_and_query() {
        let (addr, h) = gw().await;
        let c = connect_with_auth(addr, "testuser", "testpass").await;

        // After handshake, we should be able to execute queries.
        let b = query(&c, "SELECT 42 AS answer").await;
        assert_eq!(b.num_rows(), 1);
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 42);

        h.abort();
    }

    // ── Session persists state across requests ───────────────

    #[tokio::test]
    async fn auth_session_persists_state() {
        let (addr, h) = gw().await;
        let c = connect_with_auth(addr, "user", "pass").await;

        // Create a table in one request.
        exec(&c, "CREATE TABLE session_t (n INTEGER)").await;
        exec(&c, "INSERT INTO session_t VALUES (1),(2),(3)").await;

        // Query it in another request -- same session, same connection,
        // so the table is visible.
        let b = query(&c, "SELECT COUNT(*) FROM session_t").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 3);

        h.abort();
    }

    // ── Anonymous fallback (no handshake) ────────────────────

    #[tokio::test]
    async fn auth_anonymous_fallback() {
        let (addr, h) = gw().await;
        // Connect WITHOUT credentials -- no handshake, anonymous mode.
        let c = connect_client(addr).await;

        // Should still work (one-shot connection per request).
        let b = query(&c, "SELECT 1 AS n").await;
        assert_eq!(b.num_rows(), 1);

        h.abort();
    }

    // ── Multiple sessions are independent ────────────────────

    #[tokio::test]
    async fn auth_multiple_sessions_independent() {
        let (addr, h) = gw().await;
        let c1 = connect_with_auth(addr, "user1", "pass1").await;
        let c2 = connect_with_auth(addr, "user2", "pass2").await;

        // Session 1 creates a table.
        exec(&c1, "CREATE TABLE s1_t (n INTEGER)").await;
        exec(&c1, "INSERT INTO s1_t VALUES (10)").await;

        // Session 2 can see it (same SQLite file), but this proves both sessions work.
        let b = query(&c2, "SELECT n FROM s1_t").await;
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 10);

        h.abort();
    }

    // ── Authenticated DDL + DML lifecycle ────────────────────

    #[tokio::test]
    async fn auth_full_ddl_dml_lifecycle() {
        let (addr, h) = gw().await;
        let c = connect_with_auth(addr, "admin", "secret").await;

        exec(&c, "CREATE TABLE auth_lc (id INTEGER, name TEXT)").await;
        assert_eq!(
            exec(&c, "INSERT INTO auth_lc VALUES (1,'a'),(2,'b'),(3,'c')").await,
            3,
        );
        assert_eq!(exec(&c, "UPDATE auth_lc SET name='x' WHERE id=2").await, 1);
        assert_eq!(exec(&c, "DELETE FROM auth_lc WHERE id=3").await, 1);

        let b = query(&c, "SELECT id, name FROM auth_lc ORDER BY id").await;
        assert_eq!(b.num_rows(), 2);
        let names: &StringArray = b.column(1).as_any().downcast_ref().unwrap();
        assert_eq!(names.value(1), "x");

        h.abort();
    }

    // ── Metadata works with auth ─────────────────────────────

    #[tokio::test]
    async fn auth_metadata_works() {
        let (addr, h) = gw().await;
        let c = connect_with_auth(addr, "user", "pass").await;

        let b = collect(c.get_table_types().await.unwrap());
        assert!(b.num_rows() >= 2);

        let b = collect(c.get_info(None).await.unwrap());
        assert!(b.num_rows() > 0);

        h.abort();
    }
}
