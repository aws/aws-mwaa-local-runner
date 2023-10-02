# Base Python
from datetime import datetime
from pathlib import Path

# Irondata
from shared.dag_factory import create_dag
from shared.irondata import Irondata
from shared.irontable import Irontable
from shared.operators.python_s3 import S3PythonOperator
from shared.operators.s3_to_redshift_operator import IronS3ToRedshift

# quiz_statistics
from canvas_rest_api.quiz_statistics.src import query_course_ids, request_quiz_statistics

# Airflow
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

DAG_CONFIG = dict(
    dag_name="canvas_quiz_statistics",
    start_date=datetime(2023, 6, 8),
    schedule="@daily",
    template_searchpath=[(Path(__file__).resolve().parent / 'sql').as_posix()]
)

with create_dag(**DAG_CONFIG) as dag:
    base_key = dag.dag_id + '/{{ ds }}/'
    tables = [
        "quiz_statistics",
        "question_statistics",
        "user_answers",
    ]

    keys = {
        table: base_key + table for table in tables
    }

    query_ids = PythonOperator(
        task_id="query_ids",
        python_callable=query_course_ids,
    )

    request = S3PythonOperator(
        task_id="request_quiz_data",
        python_callable=request_quiz_statistics,
        dest_s3_key=list(keys.values()),
    )

    transfers = []
    for table, key in keys.items():

        iron_table = Irontable(schema="canvas_consumer", table=table)
        
        drop = PostgresOperator(
            task_id=table + '_drop',
            sql=f"drop table if exists {iron_table.schema_in_env}.{iron_table.table_in_env}",
            autocommit=True,
        )
        
        create = PostgresOperator(
            task_id=table + '_create',
            sql=table + '.sql',
            autocommit=True,
            params=dict(
                schema=iron_table.schema_in_env,
                table=iron_table.table_in_env,
            )
        )
        
        transfer = IronS3ToRedshift(
            task_id=table + '_transfer',
            s3_bucket=Irondata.s3_warehouse_bucket(),
            s3_key=key,
            schema=iron_table.schema_in_env,
            table=iron_table.table_in_env,
            copy_options=[
                "csv",
                "IGNOREHEADER 1",
                "timeformat 'YYYY-MM-DDTHH:MI:SSZ'"
            ]
        )

        drop >> create >> transfer
        transfers.append(transfer)
        

query_ids >> request >> transfers

