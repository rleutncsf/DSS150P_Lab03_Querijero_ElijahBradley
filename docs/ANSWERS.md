### Goal 3: Storage format benchmark report

Dataset: `curated.sales_order_lines`, 49,897 rows, from a single curated run of the DSS150P Lab 3 pipeline. The filtered-read benchmark uses `status = 'DELIVERED'`, which matches 8,355 rows (16.7% of the dataset).

Method: Each format was written once, then read in full five times and read with the status filter five times. The table below lists the median of those five runs. Individual timings are in `benchmark_raw_timings.csv`. System specifications (OS, CPU, RAM, library versions) are in `benchmark_environment.json`.

#### Results

| Format | Size | Write (median) | Full read (median) | Filtered read (median) |
| --- | --- | --- | --- | --- |
| CSV | 14.26 MB | 1.503 s | 0.442 s | 0.427 s |
| JSON Lines | 29.27 MB | 1.794 s | 0.847 s | 0.864 s |
| Parquet (snappy) | 5.15 MB | 0.076 s | 0.020 s | 0.026 s |
| Parquet (zstd) | 3.29 MB | 0.095 s | 0.020 s | 0.026 s |
| PostgreSQL | 14.01 MB (table only, no index) | 0.969 s | 0.848 s | 0.169 s |
| PostgreSQL (status indexed) | 14.36 MB (table + 360 KB index) | 0.974 s | 0.894 s | 0.152 s |

PostgreSQL size was measured with `pg_total_relation_size()` on a scratch table containing the test rows. A database relation is managed by the server and is not directly comparable to a standalone file, but the lab prompt asks for an empirical measurement rather than an estimate.

#### Interpretation

**Size.** Parquet with zstd compression was the smallest format at 3.29 MB, saving 77% relative to CSV and 89% relative to JSON Lines. Parquet stores values in binary typed columns rather than ASCII characters. Low-cardinality fields like `status` (six distinct values across all 49,897 rows) compress efficiently into small dictionary entries. JSON Lines was the largest format by a wide margin (more than double CSV) because it repeats every column key on every row in addition to the text encoding overhead on every value.

**Speed, full read.** Both Parquet configurations were roughly 22 times faster than CSV and 42 times faster than JSON Lines or a PostgreSQL `SELECT *` across the entire table. However, Parquet files are static, append-oriented snapshots. They do not handle row-level modifications, transactional isolation, or concurrent writes, which PostgreSQL provides and which the pipeline's load step requires for idempotent UPSERTs. Parquet is built for fast scans over historical batches, whereas PostgreSQL handles active state management.

**Speed, filtered read.** Filtering cut PostgreSQL read times by 80% because the server only serializes and transmits the 8,355 matching rows instead of the entire table. Adding a b-tree index on `status` took off another 10%. That gain is modest because a 16.7% match rate is not selective enough for the index to bypass most table pages. Parquet did the opposite: filtering was roughly 31% slower than an unfiltered read. Parquet relies on row-group statistics (min/max values) to skip blocks of rows. Because the source dataset was not sorted or clustered by status before being written, each row group contains all six status values. Predicate pushdown cannot eliminate any row groups, so evaluating the filter simply adds overhead to an already fast 20 ms scan. Sorting by status before writing would fix pushdown for this specific filter, but it would offer no benefit for queries filtering on different attributes.

**Format selection.** Each format serves a different role in the pipeline. Parquet is the best fit for bulk scans and analytical queries over stable data. PostgreSQL is the practical choice when records require selective retrieval, concurrent access, or UPSERT semantics. The raw read difference between PostgreSQL's filtered query (0.169 s) and Parquet (0.020 s) is secondary to PostgreSQL's transactional guarantees. CSV and JSON Lines had the worst storage and runtime numbers, but they remain useful for external data interchange (CSV) or line-by-line streaming ingestion (JSON Lines).

#### Partitioning

Splitting the dataset into year and month folders generated 21 partitions (2025-01 through 2026-09) taking 6.48 MB. That is 25.7% larger than the 5.15 MB unpartitioned Snappy Parquet file. Splitting into small files duplicates metadata headers, footers, and dictionaries across all 21 files while reducing the repeated text available for compression in each block. Most partitions held between 2,283 and 2,540 rows, while the last partition (2026-09) held 951 rows because it represents the partial current month.

Reading the targeted partition folder directly (`order_year=2026/order_month=9`) took 0.0098 s. Reading the root dataset with a matching `(order_year, order_month)` filter took 0.0161 s (39% slower) to return the same 951 rows. That difference is file discovery overhead: pyarrow has to inspect metadata across all 21 partition files to decide which ones to prune. Reading the directory directly (as in `load-partition --year --month`) skips directory traversal and metadata inspection. Partitioning adds storage overhead here, but it speeds up queries that can target a single folder directly.