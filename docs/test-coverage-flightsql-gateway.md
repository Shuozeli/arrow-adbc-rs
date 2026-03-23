# FlightSQL Gateway: Integration Test Coverage

> Last updated: 2026-03-22

## Overview

The `adbc-flightsql-gateway` crate has **87 integration tests** that validate the full round-trip: **FlightSQL client -> gateway server -> ADBC backend database**.

| Backend | Test count | Run condition |
|---------|-----------|---------------|
| SQLite | 46 | Always (in-memory, no external deps) |
| PostgreSQL | 21 | `ADBC_POSTGRES_URI` env var set |
| MySQL | 20 | `ADBC_MYSQL_URI` env var set |

## How to run

```bash
# SQLite tests (always pass, no setup needed)
cargo test -p adbc-flightsql-gateway

# PostgreSQL tests
ADBC_POSTGRES_URI="host=docker.yuacx.com port=5432 user=cyuan password=cyuan dbname=adbc_test" \
  cargo test -p adbc-flightsql-gateway -- postgres --ignored

# MySQL tests
ADBC_MYSQL_URI="mysql://cyuan:cyuan@docker.yuacx.com:3306/adbc_test" \
  cargo test -p adbc-flightsql-gateway -- mysql --ignored

# All tests (requires both servers)
ADBC_POSTGRES_URI="..." ADBC_MYSQL_URI="..." \
  cargo test -p adbc-flightsql-gateway -- --ignored
```

## SQLite Tests (46)

### Basic Queries (3)
| Test | Validates |
|------|-----------|
| `select_literal` | `SELECT 42` round-trip, Int64 value verification |
| `select_multiple_types` | Int64 + Float64 + Utf8 in single query |
| `select_empty_result` | 0-row result, schema preserved |

### DDL + DML (3)
| Test | Validates |
|------|-----------|
| `create_insert_select` | CREATE TABLE, INSERT (row count), SELECT ORDER BY with value verification |
| `update_and_delete` | UPDATE SET WHERE (row count), DELETE WHERE (row count), data correctness |
| `insert_select` | INSERT INTO ... SELECT FROM ... (cross-table copy) |

### NULL Handling (3)
| Test | Validates |
|------|-----------|
| `null_roundtrip` | NULLs in Int64 and Utf8 columns, positional correctness |
| `types_mixed_nulls_per_column` | 5-column table with scattered NULLs across rows |
| `empty_string_vs_null` | '' is distinct from NULL |

### Column Type Round-Trips (5)
| Test | Validates |
|------|-----------|
| `types_integer_float_text` | INT + REAL + TEXT in one table with value verification |
| `types_blob_roundtrip` | BLOB column with binary data (0xDEADBEEF) |
| `types_boolean_as_integer` | SQLite boolean as 0/1 INTEGER |
| `types_large_text` | 10KB text string round-trip |
| `many_columns` | 20-column table, all values verified |

### Aggregation (1)
| Test | Validates |
|------|-----------|
| `aggregation_count_sum_avg_min_max` | All 5 standard aggregations with correct values |

### GROUP BY (2)
| Test | Validates |
|------|-----------|
| `group_by` | SUM() with GROUP BY, per-group totals |
| `group_by_having` | GROUP BY ... HAVING ... ORDER BY DESC |

### ORDER BY + LIMIT (1)
| Test | Validates |
|------|-----------|
| `order_by_limit_offset` | ORDER BY + LIMIT 3, then LIMIT 2 OFFSET 2 |

### JOINs (3)
| Test | Validates |
|------|-----------|
| `inner_join` | 2-table INNER JOIN with GROUP BY |
| `left_join` | LEFT JOIN preserving unmatched rows (count = 0) |
| `multiple_joins` | 3-table JOIN (users -> orders -> products) |

### Subqueries (1)
| Test | Validates |
|------|-----------|
| `subquery` | WHERE price > (SELECT AVG(...)) |

### DISTINCT (1)
| Test | Validates |
|------|-----------|
| `distinct` | SELECT DISTINCT with deduplication |

### CASE Expression (1)
| Test | Validates |
|------|-----------|
| `case_expression` | CASE WHEN ... THEN ... ELSE ... END |

### UNION (2)
| Test | Validates |
|------|-----------|
| `union_all` | UNION ALL preserves duplicates |
| `union_distinct` | UNION removes duplicates |

### CTEs (2)
| Test | Validates |
|------|-----------|
| `cte_common_table_expression` | WITH ... AS (...) SELECT ... |
| `cte_recursive` | WITH RECURSIVE generating 10-row sequence |

### WHERE Operators (3)
| Test | Validates |
|------|-----------|
| `in_operator` | WHERE x IN (2,4) and WHERE x IN (SELECT ...) |
| `between_operator` | WHERE x BETWEEN 5 AND 15 |
| `like_operator` | WHERE name LIKE 'A%' |

### Functions (3)
| Test | Validates |
|------|-----------|
| `coalesce_ifnull` | COALESCE(NULL, NULL, 'fallback') |
| `string_functions` | LENGTH(), UPPER(), LOWER(), SUBSTR() |
| `math_expressions` | ABS(), +, -, *, /, %, ROUND() |

### Window Functions (3)
| Test | Validates |
|------|-----------|
| `window_row_number` | ROW_NUMBER() OVER (ORDER BY ...) |
| `window_rank` | RANK() OVER (PARTITION BY ... ORDER BY ...) |
| `window_sum_over` | SUM() OVER (PARTITION BY ...) |

### Error Handling (4)
| Test | Validates |
|------|-----------|
| `error_invalid_sql` | Malformed SQL returns error, no panic |
| `error_table_not_found` | SELECT from non-existent table |
| `error_column_not_found` | SELECT non-existent column |
| `error_constraint_violation` | INSERT violating PRIMARY KEY uniqueness |

### Edge Cases (2)
| Test | Validates |
|------|-----------|
| `special_characters_in_text` | Single quotes, double quotes in text |
| `max_integer_values` | i64::MAX and i64::MIN boundary values |

### Metadata (2)
| Test | Validates |
|------|-----------|
| `get_table_types` | FlightSQL GetTableTypes RPC (>= 2 types) |
| `get_sql_info` | FlightSQL GetSqlInfo RPC (> 0 entries) |

### Scale (1)
| Test | Validates |
|------|-----------|
| `large_result_set` | 500-row insert + SELECT with row verification |

## PostgreSQL Tests (21)

### Column Types (7)
| Test | Types Tested |
|------|-------------|
| `pg_types_int_family` | SMALLINT, INTEGER, BIGINT with value verification |
| `pg_types_float_family` | REAL, DOUBLE PRECISION |
| `pg_types_text_family` | TEXT, VARCHAR(50), CHAR(10) with value verification |
| `pg_types_boolean_roundtrip` | BOOLEAN (TRUE/FALSE/NULL) |
| `pg_types_bytea` | BYTEA binary data (0xDEADBEEF) |
| `pg_timestamp_as_text` | TIMESTAMPTZ/DATE via TEXT cast |
| `pg_numeric_as_text` | NUMERIC(10,3) via TEXT cast ("123.456") |

### SQL Patterns (7)
| Test | Pattern |
|------|---------|
| `pg_aggregation` | COUNT, SUM, MIN, MAX on generate_series |
| `pg_group_by` | VALUES + GROUP BY + ORDER BY |
| `pg_join` | VALUES tables + INNER JOIN |
| `pg_distinct_order_limit` | DISTINCT + ORDER BY + LIMIT |
| `pg_cte` | WITH nums AS (generate_series) |
| `pg_union_all` | UNION ALL |
| `pg_exists_subquery` | WHERE EXISTS (SELECT ...) |

### Window Functions (2)
| Test | Pattern |
|------|---------|
| `pg_window_row_number` | ROW_NUMBER() OVER (ORDER BY ...) |
| `pg_window_lag_lead` | LAG()/LEAD() with NULL detection |

### NULL + DDL + Scale + Metadata (5)
| Test | Validates |
|------|-----------|
| `pg_null_roundtrip` | NULL in BIGINT + TEXT, ORDER BY NULLS LAST |
| `pg_create_insert_select` | DDL + DML + WHERE filter with DOUBLE PRECISION |
| `pg_large_result` | 1000-row generate_series |
| `pg_get_table_types` | Metadata RPC |
| `pg_get_sql_info` | Server info RPC |

## MySQL Tests (20)

### Column Types (7)
| Test | Types Tested |
|------|-------------|
| `mysql_types_int_family` | TINYINT, SMALLINT, INT, BIGINT |
| `mysql_types_unsigned` | TINYINT UNSIGNED, INT UNSIGNED, BIGINT UNSIGNED |
| `mysql_types_float_family` | FLOAT, DOUBLE |
| `mysql_types_text_family` | VARCHAR(100), TEXT, CHAR(10) |
| `mysql_types_blob` | BLOB binary data (0xDEADBEEF) |
| `mysql_boolean` | TRUE/FALSE |
| `mysql_datetime` | DATETIME, DATE |
| `mysql_decimal` | DECIMAL(10,3) |

### SQL Patterns (6)
| Test | Pattern |
|------|---------|
| `mysql_aggregation` | COUNT, SUM on UNION ALL subquery |
| `mysql_group_by` | Real table + GROUP BY + ORDER BY |
| `mysql_join` | Two tables + INNER JOIN + GROUP BY |
| `mysql_distinct_order_limit` | DISTINCT + ORDER BY + LIMIT |
| `mysql_cte` | WITH ... AS (...) |
| `mysql_union_all` | UNION ALL |
| `mysql_in_subquery` | WHERE id IN (SELECT ...) |

### NULL + DDL + Metadata (5)
| Test | Validates |
|------|-----------|
| `mysql_null_roundtrip` | NULL in INT + VARCHAR |
| `mysql_create_insert_select` | DDL + DML + WHERE filter with DOUBLE |
| `mysql_get_table_types` | Metadata RPC |
| `mysql_get_sql_info` | Server info RPC |

## Coverage Matrix

| Feature | SQLite | PostgreSQL | MySQL |
|---------|--------|------------|-------|
| SELECT literal | x | x | x |
| CREATE TABLE | x | x | x |
| INSERT | x | x | x |
| UPDATE | x | | |
| DELETE | x | | |
| INSERT ... SELECT | x | | |
| WHERE clause | x | x | x |
| ORDER BY | x | x | x |
| LIMIT / OFFSET | x | x | x |
| DISTINCT | x | x | x |
| GROUP BY | x | x | x |
| HAVING | x | | |
| INNER JOIN | x | x | x |
| LEFT JOIN | x | | |
| 3-table JOIN | x | | |
| Subquery (scalar) | x | | |
| EXISTS subquery | | x | |
| IN subquery | x | | x |
| CASE expression | x | | |
| UNION ALL | x | x | x |
| UNION DISTINCT | x | | |
| CTE | x | x | x |
| Recursive CTE | x | | |
| BETWEEN | x | | |
| LIKE | x | | |
| COALESCE | x | | |
| String functions | x | | |
| Math expressions | x | | |
| Window ROW_NUMBER | x | x | |
| Window RANK | x | | |
| Window SUM OVER | x | | |
| Window LAG/LEAD | | x | |
| Error: invalid SQL | x | | |
| Error: table not found | x | | |
| Error: column not found | x | | |
| Error: constraint violation | x | | |
| NULL round-trip | x | x | x |
| Empty string vs NULL | x | | |
| i64 boundary values | x | | |
| 20+ columns | x | | |
| Binary/BLOB | x | x | x |
| Boolean | x | x | x |
| Timestamp/Date | | x | x |
| Decimal/Numeric | | x | x |
| Large text (10KB) | x | | |
| Large result (500+ rows) | x | x | |
| FlightSQL GetTableTypes | x | x | x |
| FlightSQL GetSqlInfo | x | x | x |

## Known driver limitations tested around

- **PostgreSQL NUMERIC**: Driver does not natively deserialize `NUMERIC`; tests use `::TEXT` cast.
- **PostgreSQL TIMESTAMPTZ/DATE**: Driver does not natively deserialize temporal types; tests use `::TEXT` cast.
- **MySQL DDL**: Non-transactional; tests use explicit `DROP TABLE IF EXISTS` for idempotency.
