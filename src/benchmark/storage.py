def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL.

    Capture:
    - storage/file size where applicable
    - write time
    - full-read time
    - filtered-read/query time
    - row count

    Use multiple repetitions and report a median for read/query timing.
    """
    raise NotImplementedError('Implement Week 6 storage benchmark')


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    raise NotImplementedError('Implement Week 6 partitioning')
