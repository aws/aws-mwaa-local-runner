import logging
import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import SnowflakeConnectorOperator
from airflow.operators.python_operator import PythonOperator

print(SnowflakeConnectorOperator)


default_args = {
    'owner': 'Big Data Infra',
    'start_date': datetime.datetime(2021, 5, 17),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


dag = DAG(
    'testing_snowflake_operator_plugin',
    default_args=default_args,
    catchup=False,
    schedule_interval='30 * * * *')

create_insert_query = [
    """CREATE TABLE IF NOT EXISTS TDM_SANDBOX.SANDBOX.JUNKY_TEST (
            REPORTING_TIMESTAMP TIMESTAMP,
            REPORTING_ROLE STRING,
            REPORTING_USER STRING
        ) AS
        SELECT
            CURRENT_TIMESTAMP AS REPORTING_TIMESTAMP,
            CURRENT_ROLE() AS REPORTING_ROLE,
            CURRENT_USER() AS REPORTING_USER;""",
    """INSERT INTO
            TDM_SANDBOX.SANDBOX.JUNKY_TEST
        SELECT
            CURRENT_TIMESTAMP,
            CURRENT_ROLE(),
            CURRENT_USER();""",
]


def row_count(**context):
    sf_hook = SnowflakeConnectorOperator()
    result = sf_hook.execute("select count(*) from TDM_SANDBOX.SANDBOX.JUNKY_TEST")
    logging.info(f"Number of rows in `TDM_SANDBOX.SANDBOX.JUNKY_TEST`  - {result[0]}")


with dag:
    dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

    operator_task = SnowflakeConnectorOperator(sql=create_insert_query,
                                               task_id='testing_snowflake_operator_task',
                                               dag=dag)

    get_count = PythonOperator(task_id="get_count", python_callable=row_count)

dummy_task >> operator_task >> get_count
