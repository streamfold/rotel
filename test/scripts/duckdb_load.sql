-- Loads parquet files into duckdb tables.

-- How to run:
-- assumes there are parquet files in /tmp/rotel/logs, /tmp/rotel/spans, and /tmp/rotel/metrics
-- duckdb otel.duckdb < load_otel_data.sql

-- Logs
CREATE TABLE IF NOT EXISTS otel_logs AS
  SELECT * FROM read_parquet('/tmp/rotel/logs/*.parquet') LIMIT 0;

INSERT INTO otel_logs
  SELECT * FROM read_parquet('/tmp/rotel/logs/*.parquet');

-- Spans
CREATE TABLE IF NOT EXISTS otel_spans AS
  SELECT * FROM read_parquet('/tmp/rotel/spans/*.parquet') LIMIT 0;

INSERT INTO otel_spans
  SELECT * FROM read_parquet('/tmp/rotel/spans/*.parquet');

-- Metrics
CREATE TABLE IF NOT EXISTS otel_metrics AS
  SELECT * FROM read_parquet('/tmp/rotel/metrics/*.parquet') LIMIT 0;

INSERT INTO otel_metrics
  SELECT * FROM read_parquet('/tmp/rotel/metrics/*.parquet');
