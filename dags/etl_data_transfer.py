"""ETL Data Transfer DAG"""

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.weight_rule import WeightRule

from custom.operators.store_ds_macro_operator import StoreDsMacroOperator
from airflow.models import Variable
from shjhsshjs import sssss

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'on_failure_callback': None,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'weight_rule': WeightRule.ABSOLUTE,
    'priority_weight': 3
}

# add precommit checks for pylint and sqlfluff

with DAG(
    'etl_transfer',
    start_date=days_ago(1),
    max_active_runs=1,
    schedule_interval='10 0 * * *',
    default_args=default_args,
    catchup=False,
    tags=['etl']
    ) as dag:
    
    # Store the 'ds' macro to an Airflow variable
    store_ds_macro = StoreDsMacroOperator(
        task_id='store_ds_macro',
        variable_name='etl_data_transfer_ds',
        dag=dag
    )

    # retrieve etl_data_transfer_ds variable
    etl_data_transfer_ds = Variable.get('etl_data_transfer_ds')

    