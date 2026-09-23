Source: `data\benchmarks\benchmark_results.csv` and `benchmark_raw_timings.csv`, 49,897 curated rows, `status = DELIVERED` filter (8,355 matching rows, 16.7% of the dataset), 5 repetitions per measurement, median reported. Environment: fill in from `benchmark_environment.json` (add disk type, power mode, and background load manually; the file has a note field for them).

Goal 3 analysis questions

1. Which file format was smallest on your machine, and what encoding/compression characteristics help explain the result?
Parquet with zstd was the smallest output at 3.29 MB. Parquet with snappy was 5.15 MB, raw CSV was 14.26 MB, and JSON Lines came out to 29.27 MB. That put zstd 36% ahead of snappy and 77% smaller than CSV on the exact same rows.
Parquet stays small because it stores values as binary data by column rather than raw ASCII strings. In this dataset, `status` only has six distinct values across all 49,897 rows, while columns like `brand` and `category` repeat constantly. That lets Parquet dictionary-encode those columns down to small integer pointers plus a tiny lookup table. zstd trades more CPU cycles during writes for a tighter compression algorithm than snappy, which yielded the smaller file at a minimal write cost (0.095s versus 0.076s median). CSV and JSON store everything as plain characters, meaning a float like 129.99 takes 6 bytes of text instead of 4 or 8 bytes of binary data. JSON Lines blows up even further because it repeats the schema keys on every row, which explains why it is over twice the size of the CSV.
2. Which representation was fastest for a full dataset read? Does that imply it is best for every workload?
Both Parquet options tied for fastest: median read times were 0.0203s for snappy and 0.0202s for zstd. CSV required 0.442s, JSON Lines took 0.847s, and running `SELECT *` in PostgreSQL took 0.848s. That puts Parquet around 22 times faster than CSV and roughly 42 times faster than PostgreSQL or JSON Lines for reading the full dataset.
That does not mean Parquet wins for every use case. Parquet files are static, whole-file objects. You cannot update one row or append a transaction safely with concurrent writers; any modification requires writing out a new file. PostgreSQL is much slower for whole-table bulk scans, but it handles row-level updates, concurrency, transactions, and the upsert logic our loader relies on. CSV is slow, but anyone can open it in Excel, Notepad, or Python without specific binary drivers. You pick Parquet for analytical scans over cold data, PostgreSQL when records change continuously, and CSV when a human colleague needs to inspect the raw export.
3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design (such as an index) could change the result?
Filtering helped PostgreSQL significantly. Querying `status = DELIVERED` took 0.169s, an 80% drop from the 0.848s full table scan, because the server only had to serialize and stream 8,355 rows instead of all 49,897. Adding a standard B-tree index on `status` dropped query time to 0.152s, an extra 10% speedup. The index gain was small because `status` has very low cardinality: six values, with `DELIVERED` representing 16.7% of the table. A B-tree index provides major speedups when a lookup matches a tiny fraction of the table; here, it only lets the query engine skip roughly five out of six records.
Parquet went the other way. Filtered reads were slower than full table scans on both compression formats: 0.0265s versus 0.0203s for snappy (31% slower), with the same behavior on zstd. Parquet uses predicate pushdown by checking min/max statistics stored in each row group header. Because this dataset was not sorted or clustered by `status` before saving, every row group contained rows from all six statuses. As a result, pyarrow could not skip a single row group and had to parse every block while evaluating the filter expression row by row. If we sorted or partitioned the data by `status` before exporting to Parquet, pyarrow could discard unneeded row groups immediately, though that benefit would disappear if a query filtered by a different column instead.
4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?
Each line in JSON Lines is a standalone, valid JSON object terminated by a newline. Appending an order is just a matter of writing a new line to the end of the file without locking, parsing, or altering existing bytes. Downstream processors can read the file line by line with a small memory footprint or slice chunks by byte offsets for parallel workers. If one record gets clipped or corrupted, the parser can discard that single line and keep processing the rest. A giant JSON array requires closing brackets and comma separators across the whole document. Appending requires locating the closing array delimiter, which prevents safe concurrent writes and makes streaming ingest difficult.
5. What happens if a partition key has extremely high cardinality or poor query locality?
High cardinality splinters the dataset into thousands of tiny files and folders. The operating system overhead of listing directories and opening file handles quickly swallows any disk I/O savings. Compression ratios also tank because small files do not have enough repetitive data to build effective dictionary tables. If queries do not filter on that exact column, the engine pays the directory lookup tax on every run without skipping any data. A good partition key has a small, controlled number of values (like the 21 monthly folders here) and matches how downstream queries actually filter. Partitioning on `order_id` or an unqueried column gives you all of the file system overhead with none of the scan benefits.

Partition load

The `data\partitioned` folder was split into 21 monthly directories (2025-01 through 2026-09) covering the 49,897 rows, totaling 6,792,477 bytes (6.48 MB) across 21 files. Most partitions contain 2,283 to 2,540 rows. The final partition (2026-09) has only 951 rows because it represents the partial current month in the source data, not missing records.

The partitioned output is 25.7% larger than the single unpartitioned snappy Parquet file (5.15 MB). Breaking the data across 21 files replicates headers, metadata footers, schema definitions, and dictionary tables 21 times, while smaller row blocks give the compressor less repetition to work with.

The payoff comes from targeted queries. Reading only the `order_year=2026/order_month=9` directory took 0.0098s. Reading the full partitioned dataset while passing a filter on `(order_year, order_month)` took 0.0161s, 39% slower, because pyarrow still had to open metadata blocks in all 21 files before pruning. Both approaches returned the exact same 951 rows (`rows_match: true`). Pointing directly at the partition folder skips opening irrelevant file metadata entirely.

Technical questions

Why is record_hash useful for rerun-safe loading, and which columns should not be included in it?

The hash represents the business content of the row. In `load_stage()`, the upsert queries compare incoming hashes to existing records, only updating rows where the hash differs. If a pipeline job runs twice with no data changes, it updates 0 rows (as seen in the Goal 2 test). Columns that vary per run without changing business meaning must stay out of the hash: `pipeline_run_id`, `processed_at_utc`, and source timestamps like `source_updated_at` that upstream systems refresh on unchanged records.

Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?

Raw storage provides an immutable copy of the source records. If a business logic bug slips into staging or curated transformations, the fix is to patch the code and reprocess from the raw snapshot. You cannot rely on extracting from the upstream system again, as databases get pruned, records update in place, and APIs frequently drop older history.

What is the difference between a data-quality rejection and a system exception?

A rejection happens when data violates validation logic (like a zero quantity or unknown status code). The pipeline handles this routinely by writing the record to quarantine with an error reason and continuing the job. A system exception is an infrastructure failure (a missing source file, database timeout, or out-of-memory error) that stops the run and requires the scheduler to retry or ping on-call.

Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?

Parquet is columnar, so queries only read the specific columns requested rather than parsing entire rows off disk. Values are stored in binary format, eliminating CPU overhead from string-to-number conversions, and compression reduces disk throughput. In our test, reading all 49,897 rows took 0.0203s in Parquet versus 0.442s in CSV.

Why might a DAG that contains all transformation logic directly be considered harder to maintain?

Putting transformation code inside DAG files binds your business rules directly to the Airflow scheduler. You cannot test those rules locally with pytest without mocking Airflow dependencies. Keeping the DAG as a thin wrapper around CLI commands lets you test transformations in plain Python, which is how `tests/test_transform_rules.py` and `tests/test_partition.py` were tested before connecting to Airflow.

How do retries interact with idempotency? Give an example where retries without idempotency cause damage.

A retry re-runs a task after a failure. If the task is idempotent, repeating it has zero unintended side effects. If an ingestion step runs a raw `INSERT` and fails on a client timeout after the database already committed the transaction, a retry will insert the records a second time, duplicating table rows. Our `load_stage()` step prevents this by using an upsert keyed on `order_id` and verified against `record_hash`.

What trade-off is introduced by partitioning too aggressively?

Partitioning too finely creates a massive number of tiny files. That increases directory scanning times, bloats file system metadata, degrades dictionary compression within files, and slows down queries that do not filter by the partition key.

How would you adapt the pipeline if the source became an API or database instead of local files?

Only `src/extract` would change. It would handle API pagination, database cursor queries with an `updated_at` watermark, `.env` credentials, and network retries. Once written to the raw landing zone in the expected format, downstream code (`src/transform`, `src/load`, `src/validate`, and `src/benchmark`) runs without any modifications.