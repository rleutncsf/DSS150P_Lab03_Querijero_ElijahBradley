-- Run: docker exec -i dss150p-postgres psql -U dss150p -d dss150p < sql/checks/verify.sql
\echo '== 1. rerun-safety: total must equal distinct order_id'
SELECT COUNT(*) AS total, COUNT(DISTINCT order_id) AS distinct_orders FROM curated.sales_order_lines;
\echo '== 2. sample audit columns'
SELECT order_id, source_updated_at, pipeline_run_id, processed_at_utc, left(record_hash, 12) AS hash_prefix
FROM curated.sales_order_lines ORDER BY order_id LIMIT 5;
\echo '== 3. amounts sanity (expect 0 bad rows)'
SELECT COUNT(*) AS bad_amount_rows FROM curated.sales_order_lines
WHERE net_amount <> gross_amount - discount_amount OR gross_amount < 0 OR net_amount < 0;
\echo '== 4. pipeline runs (audit.pipeline_runs)'
SELECT pipeline_run_id, status, rows_staging, rows_curated, rows_quarantined, started_at_utc, completed_at_utc, message
FROM audit.pipeline_runs ORDER BY started_at_utc;
\echo '== 5. partition loads (audit.partition_loads)'
SELECT * FROM audit.partition_loads ORDER BY loaded_at_utc;
\echo '== 6. rows of a loaded partition all belong to it (2026-01)'
SELECT COUNT(*) AS rows_in_partition,
       MIN(order_timestamp) AS first_ts, MAX(order_timestamp) AS last_ts
FROM curated.sales_order_lines
WHERE order_timestamp >= '2026-01-01+00' AND order_timestamp < '2026-02-01+00';
\echo '== 7. PostgreSQL storage footprint of the production table'
SELECT pg_size_pretty(pg_table_size('curated.sales_order_lines')) AS table_size,
       pg_size_pretty(pg_indexes_size('curated.sales_order_lines')) AS index_size,
       pg_size_pretty(pg_total_relation_size('curated.sales_order_lines')) AS total_size;