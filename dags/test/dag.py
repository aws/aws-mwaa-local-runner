from airflow import DAG 
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    start_date=datetime(2023,6,30),
    dag_id='test_params',
) as dag:
    
    op = PostgresOperator(
        task_id="postgres_operator",
        sql="SELECT '{{ params['value'] }}",
        params={"value": "{{ ds }}"}
    )