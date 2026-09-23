# Backfilling a historical month (Goal 4 optional challenge)

### Data interval

Each daily run covers the Airflow interval `[data_interval_start, data_interval_end)`, but the pipeline pulls a full source snapshot and tags the run with Airflow's `run_id`. The interval tracks when the DAG executed, not the order dates it pulled.

### Recommended approach

To reload a specific period like March 2025, trigger one run configured with `run_mode=partition, year=2025, month=3`. You can do this in the UI via "Trigger DAG w/ config" or run:

`airflow dags trigger dss150p_sales_pipeline --conf '{"run_mode":"partition","year":2025,"month":3}'`

The load step executes `python -m src.cli load-partition --year 2025 --month 3`, reading and upserting only that Parquet partition.

### Why this will not double-load

Writes use `order_id` as the conflict key:
`INSERT ... ON CONFLICT (order_id) DO UPDATE ... WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash`

Running the same month again (or a window overlapping a previous full run) only touches rows where payload values actually changed, with zero duplicate inserts. In `audit.partition_loads`, the pipeline maintains one row per partition (`order_year=2025/order_month=3`) and overwrites it on every reload so the log reflects the latest run.

### Why not catchup=True?

Setting `catchup=True` queues a separate full-snapshot run for every missed calendar day. You end up with hundreds of redundant runs that all read today's live source rather than historical snapshots. If you do run `airflow dags backfill -s <start> -e <end> dss150p_sales_pipeline`, leave `max_active_runs=1` intact so concurrent tasks do not collide in the shared `data/` directory, and trigger one partition run per month.

### Re-running tasks safely

All tasks are safe to retry out of the box:

* Extract atomically overwrites its raw directory.
* Transform rebuilds outputs deterministically.
* Load and load-partition rely on hash-checked upserts.
* Validate only reads data.

You do not need to clean up database state manually before re-running.