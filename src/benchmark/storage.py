from pathlib import Path
import json
import logging
import os
import platform
import shutil
import statistics
import sys
import time

import pandas as pd
import pyarrow
import pyarrow.parquet as pq

from src.common.errors import PipelineError
from src.config import BENCHMARK

log = logging.getLogger(__name__)

TS_COLS = ['order_timestamp', 'source_updated_at', 'processed_at_utc']
BENCH_SCHEMA, BENCH_TABLE = 'benchmark', 'benchmark.sales_order_lines_bench'
RESULT_COLUMNS = ['storage_type', 'file_size_bytes', 'write_seconds', 'full_read_seconds',
                  'filtered_read_seconds', 'row_count', 'notes', 'filtered_row_count', 'repeats']


def _timed(fn):
    t = time.perf_counter()
    out = fn()
    return time.perf_counter() - t, out


def _median_of(fn, repeats: int, label: str, raw: list, storage: str, op: str):
    times, last = [], None
    for i in range(repeats):
        secs, last = _timed(fn)
        times.append(secs)
        raw.append({'storage_type': storage, 'operation': op, 'repeat': i + 1, 'seconds': secs})
    return statistics.median(times), last


def _fix_ts(df: pd.DataFrame) -> pd.DataFrame:
    for c in TS_COLS:
        df[c] = pd.to_datetime(df[c], utc=True, format='ISO8601')
    return df


def environment_context() -> dict:
    mem_gb = None
    try:
        mem_gb = round(os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1024 ** 3, 1)
    except (ValueError, OSError, AttributeError):
        pass
    return {
        'platform': platform.platform(), 'processor': platform.processor() or platform.machine(),
        'cpu_count': os.cpu_count(), 'ram_gb': mem_gb, 'python': sys.version.split()[0],
        'pandas': pd.__version__, 'pyarrow': pyarrow.__version__,
        'running_in_docker': Path('/.dockerenv').exists(),
        'note': 'Add disk type (SSD/HDD), power mode and background load manually to your report.',
    }


# ----------------------------------------------------------------------------- file formats
def _bench_file_format(name, write_fn, read_fn, path, df, repeats, status, raw, notes):
    """Generic file-format benchmark: median write, median full read, median filtered read."""
    if path.exists():
        path.unlink() if path.is_file() else shutil.rmtree(path)
    write_s, _ = _median_of(lambda: write_fn(path), repeats, 'write', raw, name, 'write')
    size = path.stat().st_size
    full_s, full = _median_of(lambda: read_fn(path, None), repeats, 'full', raw, name, 'full_read')
    filt_s, filt = _median_of(lambda: read_fn(path, status), repeats, 'filtered', raw, name, 'filtered_read')
    return {'storage_type': name, 'file_size_bytes': size, 'write_seconds': write_s,
            'full_read_seconds': full_s, 'filtered_read_seconds': filt_s, 'row_count': len(full),
            'notes': notes, 'filtered_row_count': len(filt), 'repeats': repeats}, full


def _csv_read(path, status):
    df = _fix_ts(pd.read_csv(path))
    return df if status is None else df[df['status'] == status]


def _jsonl_read(path, status):
    df = _fix_ts(pd.read_json(path, lines=True, convert_dates=False, dtype={'order_id': str, 'customer_id': str,
                                                                            'product_id': str}))
    return df if status is None else df[df['status'] == status]


def _parquet_read(path, status):
    return pd.read_parquet(path, engine='pyarrow', filters=None if status is None else [('status', '==', status)])


# ----------------------------------------------------------------------------- PostgreSQL
def _bench_postgres(df, repeats, status, raw, index_on_status: bool):
    from src.load.postgres import connect
    from psycopg import sql

    name = 'postgresql_indexed_status' if index_on_status else 'postgresql'
    cols = list(df.columns)
    d = df.copy()
    for c in TS_COLS:
        d[c] = d[c].dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
    rows = list(d.astype(object).where(d.notna(), None).itertuples(index=False, name=None))

    def write():
        with connect() as conn:
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS {BENCH_SCHEMA}')
            conn.execute(f'DROP TABLE IF EXISTS {BENCH_TABLE}')
            conn.execute(f'CREATE TABLE {BENCH_TABLE} (LIKE curated.sales_order_lines INCLUDING DEFAULTS)')
            with conn.cursor() as cur, cur.copy(f'COPY {BENCH_TABLE} ({", ".join(cols)}) FROM STDIN') as cp:
                for r in rows:
                    cp.write_row(r)
            if index_on_status:
                conn.execute(f'CREATE INDEX ix_bench_status ON {BENCH_TABLE} (status)')
            conn.execute(f'ANALYZE {BENCH_TABLE}')

    def query(where):
        with connect() as conn, conn.cursor() as cur:
            if where:
                cur.execute(f'SELECT {", ".join(cols)} FROM {BENCH_TABLE} WHERE status = %s', (status,))
            else:
                cur.execute(f'SELECT {", ".join(cols)} FROM {BENCH_TABLE}')
            return pd.DataFrame(cur.fetchall(), columns=cols)

    write_s, _ = _median_of(write, repeats, 'write', raw, name, 'write')
    full_s, full = _median_of(lambda: query(False), repeats, 'full', raw, name, 'full_read')
    filt_s, filt = _median_of(lambda: query(True), repeats, 'filtered', raw, name, 'filtered_read')
    with connect() as conn:
        tbl, idx, total = conn.execute(
            f"SELECT pg_table_size('{BENCH_TABLE}'), pg_indexes_size('{BENCH_TABLE}'), "
            f"pg_total_relation_size('{BENCH_TABLE}')").fetchone()
    note = (f'server table; size = pg_total_relation_size (table {tbl} B + indexes {idx} B); '
            f'write = create+COPY{"+index" if index_on_status else ""}+ANALYZE; read includes client fetch into DataFrame')
    return {'storage_type': name, 'file_size_bytes': total, 'write_seconds': write_s, 'full_read_seconds': full_s,
            'filtered_read_seconds': filt_s, 'row_count': len(full), 'notes': note,
            'filtered_row_count': len(filt), 'repeats': repeats}, full


# ----------------------------------------------------------------------------- partitioning
def add_partition_columns(df: pd.DataFrame) -> pd.DataFrame:
    ts = df['order_timestamp'].dt.tz_convert('UTC')
    out = df.copy()
    out['order_year'] = ts.dt.year.astype('int32')
    out['order_month'] = ts.dt.month.astype('int32')
    return out


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month (Hive layout: order_year=2026/order_month=1/).

    The target folder is rebuilt from scratch so reruns never leave stale partitions behind.
    """
    output_dir = Path(output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    part = add_partition_columns(df)
    part.to_parquet(output_dir, engine='pyarrow', compression='snappy', index=False,
                    partition_cols=list(BENCHMARK['partition_columns']))
    return output_dir


def read_partition(dataset_dir, year: int, month: int) -> pd.DataFrame:
    """Read ONLY one partition (only that folder's files are opened) and verify its rows."""
    part_dir = Path(dataset_dir) / f'order_year={year}' / f'order_month={month}'
    if not part_dir.is_dir():
        raise PipelineError('load-partition', f'partition folder not found: {part_dir}', year=year, month=month)
    df = pd.read_parquet(part_dir, engine='pyarrow')
    ts = df['order_timestamp'].dt.tz_convert('UTC')
    if not ((ts.dt.year == year) & (ts.dt.month == month)).all():
        raise PipelineError('load-partition', 'partition contains rows from another year/month',
                            year=year, month=month)
    return df


def partition_report(dataset_dir, repeats: int) -> dict:
    """Listing + I/O comparison: read one partition vs read everything and filter."""
    dataset_dir = Path(dataset_dir)
    parts = []
    for f in sorted(dataset_dir.glob('order_year=*/order_month=*/*.parquet')):
        md = pq.ParquetFile(f).metadata
        parts.append({'partition': f.parent.relative_to(dataset_dir).as_posix(), 'file': f.name,
                      'rows': md.num_rows, 'bytes': f.stat().st_size})
    last = parts[-1]['partition'] if parts else None
    report = {'partitions': parts, 'partition_count': len(parts),
              'total_rows': sum(p['rows'] for p in parts), 'total_bytes': sum(p['bytes'] for p in parts)}
    if last:
        y, m = (int(x.split('=')[1]) for x in last.split('/'))
        raw: list = []
        t_part, one = _median_of(lambda: read_partition(dataset_dir, y, m), repeats, '', raw, 'partitioned', 'one')
        t_all, allf = _median_of(
            lambda: pd.read_parquet(dataset_dir, filters=[('order_year', '==', y), ('order_month', '==', m)]),
            repeats, '', raw, 'partitioned', 'pushdown')
        report['selected_partition_demo'] = {
            'partition': last, 'rows': len(one), 'read_one_partition_folder_seconds': t_part,
            'read_with_partition_filter_seconds': t_all, 'rows_match': len(one) == len(allf)}
    return report


# ----------------------------------------------------------------------------- entry point
def run_benchmark(curated_path, output_dir, repeats: int = 5, partition_dir=None):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet (snappy + zstd) and PostgreSQL.

    Writes benchmark_results.csv, benchmark_raw_timings.csv, benchmark_environment.json and
    partition_report.json into output_dir. Returns the results DataFrame.
    """
    if repeats < 5:
        log.warning('repeats=%d < 5: the lab requires at least five repetitions', repeats)
    output_dir = Path(output_dir)
    work = output_dir / 'files'
    work.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(curated_path, engine='pyarrow')
    status = BENCHMARK['filter_status']
    raw: list = []
    results, frames = [], {}

    log.info('benchmarking %d rows, repeats=%d, filter status=%s', len(df), repeats, status)
    specs = [
        ('csv', lambda p: df.to_csv(p, index=False), _csv_read, work / 'curated.csv',
         'text, untyped: types/timestamps re-parsed on every read; no predicate push-down'),
        ('jsonl', lambda p: df.to_json(p, orient='records', lines=True, date_format='iso', date_unit='us'),
         _jsonl_read, work / 'curated.jsonl', 'one JSON object per line; column names repeated in every row'),
        ('parquet_snappy', lambda p: df.to_parquet(p, engine='pyarrow', compression='snappy', index=False),
         _parquet_read, work / 'curated_snappy.parquet', 'columnar, typed, snappy; filter pushed down to row groups'),
        ('parquet_zstd', lambda p: df.to_parquet(p, engine='pyarrow', compression='zstd', index=False),
         _parquet_read, work / 'curated_zstd.parquet', 'columnar, typed, zstd (higher ratio, more CPU)'),
    ]
    for name, writer, reader, path, notes in specs:
        row, full = _bench_file_format(name, writer, reader, path, df, repeats, status, raw, notes)
        results.append(row)
        frames[name] = full

    try:
        for indexed in (False, True):
            row, full = _bench_postgres(df, repeats, status, raw, indexed)
            results.append(row)
            frames[row['storage_type']] = full
    except PipelineError as exc:
        log.warning('PostgreSQL benchmark skipped: %s', exc)
        results.append({'storage_type': 'postgresql', 'file_size_bytes': None, 'write_seconds': None,
                        'full_read_seconds': None, 'filtered_read_seconds': None, 'row_count': None,
                        'notes': f'SKIPPED: {exc}', 'filtered_row_count': None, 'repeats': repeats})

    # same-logical-dataset check: identical row counts and identical order_id sets in every representation
    expected_ids = set(df['order_id'])
    for name, full in frames.items():
        if len(full) != len(df) or set(full['order_id']) != expected_ids:
            raise PipelineError('benchmark', f'{name} does not contain the same logical row set as curated',
                                expected_rows=len(df), actual_rows=len(full))

    res = pd.DataFrame(results, columns=RESULT_COLUMNS)
    output_dir.mkdir(parents=True, exist_ok=True)
    res.to_csv(output_dir / 'benchmark_results.csv', index=False)
    pd.DataFrame(raw).to_csv(output_dir / 'benchmark_raw_timings.csv', index=False)
    (output_dir / 'benchmark_environment.json').write_text(json.dumps(environment_context(), indent=2))

    pdir = Path(partition_dir) if partition_dir else output_dir.parent / 'partitioned'
    write_partitioned_parquet(df, pdir)
    report = partition_report(pdir, repeats)
    (output_dir / 'partition_report.json').write_text(json.dumps(report, indent=2))
    log.info('partitioned parquet written to %s (%d partitions)', pdir, report['partition_count'])
    return res