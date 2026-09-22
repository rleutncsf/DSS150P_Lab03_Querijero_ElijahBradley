from datetime import datetime, timezone
from pathlib import Path
import json
import logging
import os

import pandas as pd

from src.common.audit import new_run_id
from src.common.errors import PipelineError, stage_context
from src.common.io import read_parquet, write_json, write_parquet
from src.common.layout import latest_run_id, run_dir, write_run_meta
from src.config import BENCHMARK, path_for

log = logging.getLogger(__name__)


def resolve_run_id(prev_layer: str | None, explicit: str | None = None) -> str:
    if explicit:
        return explicit
    if os.getenv('PIPELINE_RUN_ID'):
        return os.environ['PIPELINE_RUN_ID']
    if prev_layer is None:
        return new_run_id()
    try:
        return latest_run_id(prev_layer)
    except FileNotFoundError as exc:
        raise PipelineError('resolve-run', str(exc)) from exc


# ----------------------------------------------------------------------------- Goal 2 stages
def extract_stage(run_id: str) -> Path:
    from src.extract.files import extract_sources
    with stage_context('extract', run_id):
        target = extract_sources(run_id)
        log.info('raw snapshot: %s', target)
        return target


def transform_stage(run_id: str) -> dict:
    from src.transform.curated import build_curated
    from src.transform.staging import build_staging
    with stage_context('transform', run_id):
        raw = run_dir('raw', run_id)
        if not raw.is_dir():
            raise PipelineError('transform', f'raw snapshot not found: {raw} (run extract first)', run_id=run_id)
        manifest = json.loads((raw / '_manifest.json').read_text(encoding='utf-8'))

        staging, q_staging = build_staging(raw, run_id)
        curated, q_curated = build_curated(staging, run_id, upstream_quarantine=q_staging)
        quarantine = pd.concat([q for q in (q_staging, q_curated) if len(q)], ignore_index=True) \
            if (len(q_staging) or len(q_curated)) else q_staging

        stg_dir = run_dir('staging', run_id)
        for name in ('customers', 'products', 'orders'):
            write_parquet(staging[name], stg_dir / f'{name}.parquet')
        write_run_meta(stg_dir, run_id)
        cur_dir = run_dir('curated', run_id)
        write_parquet(curated, cur_dir / 'sales_order_lines.parquet')
        q_dir = run_dir('quarantine', run_id)
        q_dir.mkdir(parents=True, exist_ok=True)
        quarantine.to_csv(q_dir / 'quarantine.csv', index=False)
        write_run_meta(q_dir, run_id)

        stats = staging['stats']
        summary = {
            'pipeline_run_id': run_id,
            'started_at_utc': manifest['extracted_at_utc'],
            'transformed_at_utc': datetime.now(timezone.utc).isoformat(),
            'raw_rows': stats['raw_rows'], 'staging_rows': stats['staging_rows'],
            'duplicates_superseded': stats['duplicates_superseded'],
            'missing_email_customers': stats['missing_email_customers'],
            'curated_rows': len(curated),
            'quarantine_rows': {
                'staging': len(q_staging), 'curated': len(q_curated), 'total': len(quarantine),
                'by_reason': quarantine.groupby(['dataset', 'reason']).size().rename('rows').reset_index()
                             .to_dict('records') if len(quarantine) else [],
            },
        }
        write_json(summary, cur_dir / 'run_summary.json')
        write_run_meta(cur_dir, run_id)
        log.info('SUMMARY %s', json.dumps(summary, default=str))
        return summary


def _load_summary(run_id: str) -> dict:
    p = run_dir('curated', run_id) / 'run_summary.json'
    return json.loads(p.read_text(encoding='utf-8')) if p.exists() else {}


def _curated_df(run_id: str) -> pd.DataFrame:
    p = run_dir('curated', run_id) / 'sales_order_lines.parquet'
    if not p.exists():
        raise PipelineError('load', f'curated dataset not found: {p} (run transform first)', run_id=run_id)
    return read_parquet(p)


def load_stage(run_id: str) -> int:
    from src.load import postgres as pg
    with stage_context('load', run_id):
        df, summary = _curated_df(run_id), _load_summary(run_id)
        try:
            affected = pg.upsert_curated(df, run_id)
        except Exception as exc:
            _safe_record(pg, run_id, 'FAILED', summary, f'load failed: {type(exc).__name__}: {exc}')
            raise
        pg.record_run(run_id, 'LOADED', started_at=summary.get('started_at_utc'),
                      rows_staging=sum(summary.get('staging_rows', {}).values()) or None,
                      rows_curated=len(df), rows_quarantined=summary.get('quarantine_rows', {}).get('total'),
                      message=f'load affected {affected} of {len(df)} rows')
        log.info('load complete: %d curated rows processed, %d inserted/updated', len(df), affected)
        return affected


def load_partition_stage(run_id: str, year: int, month: int) -> int:
    from src.benchmark.storage import read_partition, write_partitioned_parquet
    from src.load import postgres as pg
    with stage_context('load-partition', run_id):
        if not 1 <= month <= 12:
            raise PipelineError('load-partition', 'month must be 1..12', month=month)
        df_all, summary = _curated_df(run_id), _load_summary(run_id)
        # (re)build the partitioned dataset from THIS run's curated data, then read only one partition
        pdir = write_partitioned_parquet(df_all, path_for('partition_dir'))
        part = read_partition(pdir, year, month).drop(columns=['order_year', 'order_month'], errors='ignore')
        affected = pg.load_partition(part, year, month, run_id)
        pg.record_run(run_id, 'LOADED_PARTITION', started_at=summary.get('started_at_utc'),
                      rows_curated=len(part), message=f'partition {year}-{month:02d}: {affected} of {len(part)} rows changed')
        log.info('partition %d-%02d: %d rows read, %d inserted/updated', year, month, len(part), affected)
        return affected


def _safe_record(pg, run_id, status, summary, message):
    try:
        pg.record_run(run_id, status, started_at=summary.get('started_at_utc'), completed=True, message=message[:500])
    except PipelineError as exc:  # DB unreachable: we still surface the ORIGINAL error to the caller
        log.warning('could not write audit.pipeline_runs (%s)', exc)


def validate_stage(run_id: str, year: int | None = None, month: int | None = None, check_db: bool = True) -> list[str]:
    from src.validate.quality import compare_key_sets, validate_curated
    with stage_context('validate', run_id):
        df = _curated_df(run_id)
        errors = [f'curated file: {e}' for e in validate_curated(df)]
        if check_db:
            from src.load import postgres as pg
            expected = df
            if year is not None and month is not None:
                ts = df['order_timestamp'].dt.tz_convert('UTC')
                expected = df[(ts.dt.year == year) & (ts.dt.month == month)]
            db = pg.read_curated_table(year, month)
            errors += [f'postgres: {e}' for e in validate_curated(db)]
            errors += compare_key_sets(expected['order_id'], db['order_id'], 'curated file')
            log.info('validated %d curated rows and %d PostgreSQL rows', len(expected), len(db))
        if errors:
            for e in errors:
                log.error('VALIDATION ERROR: %s', e)
            if check_db:
                from src.load import postgres as pg
                _safe_record(pg, run_id, 'VALIDATION_FAILED', _load_summary(run_id), '; '.join(errors)[:500])
            raise PipelineError('validate', f'{len(errors)} validation error(s): ' + ' | '.join(errors), run_id=run_id)
        if check_db:
            from src.load import postgres as pg
            pg.record_run(run_id, 'VALIDATED', completed=True, message='all validation checks passed')
        log.info('VALIDATION PASSED')
        return errors


# ----------------------------------------------------------------------------- Goal 3
def benchmark_stage(run_id: str, repeats: int) -> pd.DataFrame:
    from src.benchmark.storage import run_benchmark
    with stage_context('benchmark', run_id):
        curated = run_dir('curated', run_id) / 'sales_order_lines.parquet'
        if not curated.exists():
            raise PipelineError('benchmark', f'curated dataset not found: {curated}', run_id=run_id)
        res = run_benchmark(curated, path_for('benchmark_dir'), repeats=repeats,
                            partition_dir=path_for('partition_dir'))
        log.info('\n%s', res.drop(columns=['notes']).to_string(index=False))
        return res