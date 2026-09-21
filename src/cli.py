import argparse
import importlib.metadata as md
import logging
import platform
import sys

from src.common.errors import PipelineError, setup_logging
from src.config import DB, PROJECT_ROOT, SETTINGS, path_for

log = logging.getLogger('cli')
PACKAGES = ['pandas', 'pyarrow', 'psycopg', 'python-dotenv', 'PyYAML']


def validate_env(require_db: bool) -> int:
    print('Python           :', platform.python_version(), f'({sys.executable})')
    for pkg in PACKAGES:
        try:
            print(f'{pkg:<17}:', md.version(pkg))
        except md.PackageNotFoundError:
            print(f'{pkg:<17}: NOT INSTALLED')
            return 1
    print('PROJECT_ROOT     :', PROJECT_ROOT)
    print('Configured source:', SETTINGS['pipeline']['source_dir'])
    print('DB host/database :', DB['host'], DB['dbname'], f"(port {DB['port']}, user {DB['user']})")
    print('DB password set  :', 'yes' if DB['password'] else 'NO  <-- create .env from .env.example')
    missing = [n for n in ('customers.csv', 'products.json', 'orders.csv')
               if not (path_for('source_dir') / n).is_file()]
    print('Source files     :', 'all present' if not missing else f'MISSING {missing}')
    db_ok = False
    if DB['password']:
        try:
            from src.load.postgres import connect
            with connect(timeout=3) as conn:
                conn.execute('SELECT 1')
            db_ok = True
        except PipelineError as exc:
            print('DB connection    : UNREACHABLE ->', exc)
    print('DB connection    :', 'OK' if db_ok else 'not available (start it with: docker compose up -d postgres)')
    if missing or not DB['password'] or (require_db and not db_ok):
        return 1
    print('validate-env: OK')
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    parser.add_argument('--run-id', help='override the pipeline run id (default: $PIPELINE_RUN_ID, else derived)')
    sub = parser.add_subparsers(dest='command', required=True)
    v = sub.add_parser('validate-env', help='print environment/config info and verify prerequisites')
    v.add_argument('--require-db', action='store_true', help='exit non-zero if PostgreSQL is unreachable')
    sub.add_parser('extract', help='copy source files into data/raw/run_id=<id>/')
    sub.add_parser('transform', help='raw -> staging -> curated (+ quarantine)')
    sub.add_parser('load', help='UPSERT curated rows into PostgreSQL (rerun-safe)')
    val = sub.add_parser('validate', help='validate curated file and PostgreSQL contents')
    val.add_argument('--year', type=int)
    val.add_argument('--month', type=int)
    val.add_argument('--skip-db', action='store_true', help='validate only the curated parquet file')
    b = sub.add_parser('benchmark', help='storage comparison + partitioned parquet')
    b.add_argument('--repeats', type=int, default=None)
    p = sub.add_parser('load-partition', help='load one year/month partition into PostgreSQL')
    p.add_argument('--year', type=int, required=True)
    p.add_argument('--month', type=int, required=True)
    r = sub.add_parser('run-all', help='extract -> transform -> validate curated file (add --with-load for DB)')
    r.add_argument('--with-load', action='store_true', help='also load into PostgreSQL and validate it')
    return parser


def main(argv=None) -> int:
    setup_logging()
    args = build_parser().parse_args(argv)
    if args.command == 'validate-env':
        return validate_env(args.require_db)

    from src import stages as st
    try:
        if args.command == 'extract':
            st.extract_stage(st.resolve_run_id(None, args.run_id))
        elif args.command == 'transform':
            st.transform_stage(st.resolve_run_id('raw', args.run_id))
        elif args.command == 'load':
            st.load_stage(st.resolve_run_id('curated', args.run_id))
        elif args.command == 'validate':
            if (args.year is None) != (args.month is None):
                raise PipelineError('validate', '--year and --month must be given together')
            st.validate_stage(st.resolve_run_id('curated', args.run_id), args.year, args.month,
                              check_db=not args.skip_db)
        elif args.command == 'benchmark':
            repeats = args.repeats or int(SETTINGS['storage_benchmark']['repeats'])
            st.benchmark_stage(st.resolve_run_id('curated', args.run_id), repeats)
        elif args.command == 'load-partition':
            st.load_partition_stage(st.resolve_run_id('curated', args.run_id), args.year, args.month)
        elif args.command == 'run-all':
            run_id = st.resolve_run_id(None, args.run_id)
            st.extract_stage(run_id)
            st.transform_stage(run_id)
            st.validate_stage(run_id, check_db=False)
            if args.with_load:
                st.load_stage(run_id)
                st.validate_stage(run_id, check_db=True)
            log.info('run-all finished: run_id=%s', run_id)
    except PipelineError as exc:
        log.error('PIPELINE FAILED stage=%s | %s', exc.stage, exc)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())