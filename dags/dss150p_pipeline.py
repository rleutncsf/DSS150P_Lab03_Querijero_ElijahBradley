from datetime import datetime, timedelta
import json
import traceback

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'
FAILURE_LOG = f'{PROJECT}/logs/pipeline_failures.jsonl'
RUN = 'PIPELINE_RUN_ID="{{ run_id }}"'   # one id for every task of the DAG run


def _context_record(event: str, context) -> dict:
    ti = context['task_instance']
    exc = context.get('exception')
    return {
        'event': event,
        'dag_id': ti.dag_id, 'task_id': ti.task_id, 'run_id': context['run_id'],
        'try_number': ti.try_number, 'max_tries': ti.max_tries,
        'state': str(ti.state), 'params': dict(context['params']),
        'start_date': str(ti.start_date), 'end_date': str(ti.end_date),
        'error': f'{type(exc).__name__}: {exc}' if exc else None,
        'log_url': ti.log_url,
    }


def _report(event: str, context) -> None:
    """Print to the task log AND append one JSON line to logs/pipeline_failures.jsonl."""
    record = _context_record(event, context)
    print(f'##### {event}: ' + json.dumps(record, default=str))
    try:
        with open(FAILURE_LOG, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record, default=str) + '\n')
    except OSError:
        print('could not write', FAILURE_LOG)
        traceback.print_exc()


def retry_callback(context):
    _report('TASK_RETRY', context)


def failure_callback(context):
    _report('TASK_FAILED', context)


DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,                                   # 1 try + 2 retries
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=15),     # a task can never hang forever
    'on_retry_callback': retry_callback,
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    description='extract -> transform -> load -> validate for the e-commerce sales pipeline',
    doc_md=__doc__,
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,                              # runs share data/ folders + one DB: never overlap
    dagrun_timeout=timedelta(hours=1),
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition'],
                          description='full = load all curated rows; partition = load only year/month below'),
        'year': Param(2026, type='integer', minimum=2000, maximum=2100),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && {RUN} python -m src.cli extract',
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && {RUN} python -m src.cli transform',
    )

    load = BashOperator(
        task_id='load',
        bash_command=(
            f'cd {PROJECT} && '
            "{% if params.run_mode == 'partition' %}"
            f'{RUN} python -m src.cli load-partition --year {{{{ params.year }}}} --month {{{{ params.month }}}}'
            '{% else %}'
            f'{RUN} python -m src.cli load'
            '{% endif %}'
        ),
    )

    validate = BashOperator(
        task_id='validate',
        bash_command=(
            f'cd {PROJECT} && '
            "{% if params.run_mode == 'partition' %}"
            f'{RUN} python -m src.cli validate --year {{{{ params.year }}}} --month {{{{ params.month }}}}'
            '{% else %}'
            f'{RUN} python -m src.cli validate'
            '{% endif %}'
        ),
    )

    extract >> transform >> load >> validate