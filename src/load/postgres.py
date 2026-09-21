from datetime import datetime, timezone
import logging

import pandas as pd
import psycopg

from src.common.errors import PipelineError
from src.config import DB

log = logging.getLogger(__name__)

TABLE = 'curated.sales_order_lines'
COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp', 'customer_city', 'customer_tier',
    'product_name', 'category', 'brand', 'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount', 'status',
    'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
]
TS_COLUMNS = ['order_timestamp', 'source_updated_at', 'processed_at_utc']


def connect(autocommit: bool = False, timeout: int = 10) -> psycopg.Connection:
    if not DB['password']:
        raise PipelineError('load', 'POSTGRES_PASSWORD is not set (copy .env.example to .env and edit it)')
    try:
        return psycopg.connect(host=DB['host'], port=DB['port'], dbname=DB['dbname'], user=DB['user'],
                               password=DB['password'], connect_timeout=timeout, autocommit=autocommit,
                               options='-c timezone=UTC')
    except psycopg.OperationalError as exc:
        raise PipelineError('load', f'cannot connect to PostgreSQL: {str(exc).strip()}',
                            host=DB['host'], port=DB['port'], dbname=DB['dbname'], user=DB['user']) from exc


def _rows(df: pd.DataFrame, columns: list[str]):
    d = df[columns].copy()
    for col in TS_COLUMNS:
        if col in d:
            d[col] = d[col].dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
    d = d.astype(object).where(d.notna(), None)
    return list(d.itertuples(index=False, name=None))


def _upsert(conn: psycopg.Connection, df: pd.DataFrame) -> dict:
    if df['order_id'].isna().any() or df['order_id'].duplicated().any():
        raise PipelineError('load', 'incoming data has null/duplicate order_id; refusing to load')
    update_cols = [c for c in COLUMNS if c != 'order_id']
    set_clause = ', '.join(f'{c} = EXCLUDED.{c}' for c in update_cols)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TEMP TABLE _incoming (LIKE {TABLE} INCLUDING DEFAULTS) ON COMMIT DROP')
        with cur.copy(f'COPY _incoming ({", ".join(COLUMNS)}) FROM STDIN') as copy:
            for row in _rows(df, COLUMNS):
                copy.write_row(row)
        # record_hash guard: unchanged business content => row is neither rewritten nor re-stamped
        cur.execute(f"""
            INSERT INTO {TABLE} ({", ".join(COLUMNS)})
            SELECT {", ".join(COLUMNS)} FROM _incoming
            ON CONFLICT (order_id) DO UPDATE SET {set_clause}
            WHERE {TABLE}.record_hash IS DISTINCT FROM EXCLUDED.record_hash
            RETURNING (xmax = 0) AS inserted
        """)
        flags = [r[0] for r in cur.fetchall()]
    inserted = sum(flags)
    result = {'incoming': len(df), 'inserted': inserted, 'updated': len(flags) - inserted,
              'unchanged_skipped': len(df) - len(flags)}
    result['affected'] = result['inserted'] + result['updated']
    return result


def record_run(run_id: str, status: str, *, started_at: str | None = None, completed: bool = False,
               rows_staging: int | None = None, rows_curated: int | None = None,
               rows_quarantined: int | None = None, message: str | None = None) -> None:
    """Upsert audit.pipeline_runs (later calls keep earlier non-null values)."""
    now = datetime.now(timezone.utc)
    with connect() as conn:
        conn.execute("""
            INSERT INTO audit.pipeline_runs
              (pipeline_run_id, started_at_utc, completed_at_utc, status, rows_staging, rows_curated,
               rows_quarantined, message)
            VALUES (%(id)s, %(started)s, %(completed)s, %(status)s, %(rs)s, %(rc)s, %(rq)s, %(msg)s)
            ON CONFLICT (pipeline_run_id) DO UPDATE SET
              completed_at_utc = COALESCE(EXCLUDED.completed_at_utc, audit.pipeline_runs.completed_at_utc),
              status = EXCLUDED.status,
              rows_staging = COALESCE(EXCLUDED.rows_staging, audit.pipeline_runs.rows_staging),
              rows_curated = COALESCE(EXCLUDED.rows_curated, audit.pipeline_runs.rows_curated),
              rows_quarantined = COALESCE(EXCLUDED.rows_quarantined, audit.pipeline_runs.rows_quarantined),
              message = EXCLUDED.message
        """, {'id': run_id, 'started': started_at or now, 'completed': now if completed else None,
              'status': status, 'rs': rows_staging, 'rc': rows_curated, 'rq': rows_quarantined, 'msg': message})


def upsert_curated(df, run_id: str) -> int:
    with connect() as conn:                 # one transaction: all-or-nothing
        result = _upsert(conn, df)
    log.info('upsert %s run_id=%s: %s', TABLE, run_id, result)
    return result['affected']


def load_partition(df, year: int, month: int, run_id: str) -> int:
    ts = df['order_timestamp'].dt.tz_convert('UTC')
    if len(df) == 0:
        raise PipelineError('load-partition', f'partition {year}-{month:02d} is empty', run_id=run_id)
    if not ((ts.dt.year == year) & (ts.dt.month == month)).all():
        raise PipelineError('load-partition', 'dataframe contains rows outside the selected partition',
                            year=year, month=month, run_id=run_id)
    key = f'order_year={year}/order_month={month}'
    with connect() as conn:
        result = _upsert(conn, df)
        conn.execute("""
            INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (partition_key) DO UPDATE SET
              loaded_at_utc = EXCLUDED.loaded_at_utc, row_count = EXCLUDED.row_count,
              pipeline_run_id = EXCLUDED.pipeline_run_id
        """, (key, datetime.now(timezone.utc), len(df), run_id))
    log.info('partition %s run_id=%s: %s', key, run_id, result)
    return result['affected']


def read_curated_table(year: int | None = None, month: int | None = None) -> pd.DataFrame:
    where, params = '', []
    if year is not None and month is not None:
        where = ("WHERE order_timestamp >= make_timestamptz(%s, %s, 1, 0, 0, 0, 'UTC') "
                 "AND order_timestamp < make_timestamptz(%s, %s, 1, 0, 0, 0, 'UTC') + interval '1 month'")
        params = [year, month, year, month]
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f'SELECT {", ".join(COLUMNS)} FROM {TABLE} {where}', params)
        return pd.DataFrame(cur.fetchall(), columns=COLUMNS)