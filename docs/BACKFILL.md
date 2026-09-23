# Backfilling a historical month (Goal 4 optional challenge)

**Data interval.** Each daily run covers the Airflow interval `[data_interval_start, data_interval_end)`,
but this pipeline reads the *whole current source snapshot* and stamps the run with Airflow's `run_id`.
The interval therefore identifies *when* the run happened, not *which orders* it loads.

**Recommended approach.** To reload e.g. March 2025, trigger one run with
`run_mode=partition, year=2025, month=3` (UI "Trigger DAG w/ config" or
`airflow dags trigger dss150p_sales_pipeline --conf '{"run_mode":"partition","year":2025,"month":3}'`).
The load task calls `python -m src.cli load-partition --year 2025 --month 3`, which reads only that Parquet
partition and UPSERTs it.

**Why this cannot double-load.** The target key is `order_id` and every write is
`INSERT ... ON CONFLICT (order_id) DO UPDATE ... WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash`.
Loading the same month twice (or a month that overlaps a previous full load) updates only rows whose business
content really changed and inserts nothing twice. `audit.partition_loads` keeps one row per partition
(`order_year=2025/order_month=3`) that is refreshed on each reload, so the audit trail shows the last load
without duplicates.

**Why not `catchup=True`?** Enabling catch-up would create one full-snapshot run per missed day - hundreds of
identical, idempotent runs - and every one would read today's source, not the historical state. If you do use
`airflow dags backfill -s <start> -e <end> dss150p_sales_pipeline`, keep `max_active_runs=1` (already set) so
runs never overlap on the shared `data/` folders, and prefer one partition-mode run per month.

**Safe to re-run:** extract (atomic replace of its own raw folder), transform (deterministic rebuild),
load / load-partition (hash-guarded UPSERT), validate (read-only). No manual database cleanup is required.