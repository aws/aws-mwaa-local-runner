from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DEFAULT_RETRIES = 0
DEFAULT_TRIGGER_RULE = "none_failed"


def create_dag(
    name,
    schedule=None,
    args={},
    catchup=False,
    concurrency=5,
):
    """
    params:
        name(str): the name of the dag
        schedule(str): the cron expression or the schedule.  Can be Macros like "@daily"
        default_args(dict): a dict with the specific keys you want to edit from the original DEFAULT_ARGS
        catchup(bool): Perform scheduler catchup (or only run latest)? Defaults to True
        concurrency(int): the number of task instances allowed to run concurrently
    returns:
        DAG object
    """
    DEFAULT_ARGS = {
        "owner": "Data Engineer",
        "depends_on_past": False,
        "start_date": datetime(2022, 1, 1),
        "email": ["data_engineers@vibrantplanet.net"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
    DEFAULT_ARGS.update(args)

    return DAG(
        dag_id=name,
        schedule_interval=schedule,
        default_args=DEFAULT_ARGS,
        catchup=catchup,
        concurrency=concurrency,
    )


def add_bash_task(
    dag, name, command, retries=DEFAULT_RETRIES, trigger_rule=DEFAULT_TRIGGER_RULE
):
    return BashOperator(
        task_id=name,
        bash_command=command,
        retries=retries,
        trigger_rule=trigger_rule,
        dag=dag,
    )


def add_python_task(
    dag,
    name,
    function,
    kwargs=None,
    retries=DEFAULT_RETRIES,
    trigger_rule=DEFAULT_TRIGGER_RULE,
):
    return PythonOperator(
        task_id=name,
        python_callable=function,
        op_kwargs=kwargs,
        retries=retries,
        trigger_rule=trigger_rule,
        dag=dag,
    )
