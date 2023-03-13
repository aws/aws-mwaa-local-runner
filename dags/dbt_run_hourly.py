# std
from datetime import datetime, timedelta
import json
from typing import Dict
import py_compile


# external
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

# plugins
from zion_data_modeling import dbt_operator, get_start_timestamp, dbt_failure_alert

# constants
DAG_NAME = 'dbt_run_hourly'
DAG_VERSION_NUMBER = '2'
AIRFLOW_ENVIRONMENT = Variable.get('airflow_environment')
START_DATE = datetime(2022, 6, 7, 20, 0, 0)
DELAY_MINUTES = 15

if AIRFLOW_ENVIRONMENT.lower() == 'internal':
    TARGET = 'dev'
else:
    TARGET = AIRFLOW_ENVIRONMENT.lower()

if AIRFLOW_ENVIRONMENT.lower() == 'prod':
    SCHEDULE = f'{DELAY_MINUTES} * * * *'
elif AIRFLOW_ENVIRONMENT.lower() == 'dev':
    SCHEDULE = f'{DELAY_MINUTES} 19-20 * * SUN-WED'
else:
    SCHEDULE = None


def generate_tag_string(default_tags: str, node_tags: str = None):
    if node_tags is not None:
        tag_string = ','.join((node_tags, default_tags))
    else:
        tag_string = default_tags
    return tag_string


def generate_dbt_run_command_args(snowplow_environment: str, model_path: str, dbt_run_or_build: str,
                                  include_tags: str = None, exclude_tags: str = None):

    VARS = {
        'snowplow_env': snowplow_environment.upper(),
        'start_timestamp': get_start_timestamp(DELAY_MINUTES),
    }

    command_flags = [
        dbt_run_or_build,
        '--target',
        TARGET,
        '--fail-fast',
        '--select',
        generate_tag_string(
            f'+tag:hourly,+tag:{snowplow_environment},path:models/{model_path}', include_tags),
        '--exclude',
        generate_tag_string(f'tag:historical tag:daily {exclude_tags}'),
        '--vars',
        json.dumps(VARS),
    ]
    return command_flags


def generate_task_id(model_path: str, dbt_run_or_build: str, task_id: str = None):
    if task_id != None:
        task_id = task_id
    elif model_path == '*':
        task_id = f'dbt_ecs_{dbt_run_or_build}'
    else:
        task_id = f'dbt_ecs_{dbt_run_or_build}_{model_path}'
    return task_id


def generate_dbt_ecs_run_task(snowplow_environment: str, model_path: str, dag: airflow.DAG,
                              dbt_run_or_build: str = 'run', include_tags: str = None, exclude_tags: str = None, task_id: str = None
                              ):
    command_args = generate_dbt_run_command_args(
        snowplow_environment, model_path, dbt_run_or_build, include_tags, exclude_tags)

    return dbt_operator(
        dag=dag,
        task_id=generate_task_id(model_path, dbt_run_or_build, task_id),
        command=command_args,
        snowplow_environment=snowplow_environment,
        retries=1,
        delay_minutes=DELAY_MINUTES,
        on_failure_callback=dbt_failure_alert,
    )


def test_task(dag: airflow.DAG):
    return dbt_operator(
        dag=dag,
        task_id='test',
        command=[
            'test',
            '-s',
            '+tag:hourly',
            '--target',
            TARGET
        ],
        retries=1,
        delay_minutes=DELAY_MINUTES,
        on_failure_callback=dbt_failure_alert,
    )

def seed_task(dag: airflow.DAG):
    VARS = {
        'snowplow_env': snowplow_environment.upper()
    }
    return dbt_operator(
        dag=dag,
        task_id='seed',
        command=[
            'seed',
            '--full-refresh',
            '--target',
            TARGET,
            '--vars',
            json.dumps(VARS)
        ],
        retries=1,
        delay_minutes=DELAY_MINUTES,
        on_failure_callback=dbt_failure_alert,
    )


def create_dbt_dag(
        dag_id: str,
        task_defaults_args: Dict,
        schedule,
        snowplow_environment: str,
):
    with DAG(
            dag_id=dag_id,
            schedule_interval=schedule,
            default_args=task_defaults_args,
            max_active_runs=2,
            max_active_tasks=2,
            catchup=True,
            start_date=START_DATE,
    ) as dag:


        if AIRFLOW_ENVIRONMENT.lower() == 'dev' and snowplow_environment.lower() == 'prod':
            tags = 'tag:unload'

        else:
            tags = 'tag:remediation'

        # Mainline tasks
        seed = seed_task(
            dag=dag
        )

        dbt_ecs_run_event_correction = generate_dbt_ecs_run_task(
            dag=dag,
            snowplow_environment=snowplow_environment,
            model_path='0-id-bridge-event-correction',
        )

        dbt_ecs_run_cleaned = generate_dbt_ecs_run_task(
            dag=dag,
            snowplow_environment=snowplow_environment,
            model_path='1-cleaned',
        )

        dbt_ecs_run_enriched = generate_dbt_ecs_run_task(
            dag=dag,
            snowplow_environment=snowplow_environment,
            model_path='2-enriched',
        )

        dbt_ecs_build_verticals = generate_dbt_ecs_run_task(
            dag=dag,
            snowplow_environment=snowplow_environment,
            model_path='3-verticals',
            dbt_run_or_build='run',
            exclude_tags=tags,
        )

        test = test_task(
            dag=dag
        )

        seed >> dbt_ecs_run_event_correction >> dbt_ecs_run_cleaned >> dbt_ecs_run_enriched >> dbt_ecs_build_verticals >> test

        return dag


task_default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'owner': 'zion',
    #'execution_timeout': timedelta(minutes=30),
}

list_of_snowplow_environments = ['prod', 'stage']

for snowplow_environment in list_of_snowplow_environments:
    dag_id = f'{DAG_NAME}_{snowplow_environment}_v{DAG_VERSION_NUMBER}'
    globals()[dag_id] = create_dbt_dag(
        dag_id=dag_id,
        task_defaults_args=task_default_args,
        schedule=SCHEDULE,
        snowplow_environment=snowplow_environment,
    )
