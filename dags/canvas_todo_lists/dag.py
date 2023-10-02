# Base
from datetime import datetime
from pathlib import Path

# Airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Irondata
from shared.irondata import Irondata
from shared.irontable import Irontable
from canvas_todo_lists.src import get_todo_lists
from shared.operators.python_s3 import S3PythonOperator
from shared.operators.redshift_to_s3 import RedshiftToS3Operator
from shared.operators.s3_to_redshift_staging_operator import S3ToRedshiftStagingTransfer


DAG_SETTINGS = dict(
    dag_id="canvas_todo_lists",
    schedule_interval="0 7,14 * * *",  # 7am and 2pm
    start_date=datetime(2022, 8, 26),
    catchup=False,
    default_args=dict(
        provide_context=True,
    ),
    template_searchpath=[Irondata.sql_templates_path(),
                         (Path(__file__).resolve().parent / 'sql').as_posix()
                         ]
    )

table = Irontable(schema="fis", table="canvas_todo_lists")
BUCKET = Irondata.s3_warehouse_bucket()
S3_KEY_IDS = "canvas_todo_lists/{{ ds }}/ids"
S3_KEY_TODOS = "canvas_todo_lists/{{ ds }}/todo_lists"

sql_path = Path(__file__).resolve().parent / 'sql' / 'instructor_canvas_ids.sql'
with sql_path.open('r') as file:
    select_query = file.read()

with DAG(**DAG_SETTINGS) as dag:

    educator_canvas_ids = RedshiftToS3Operator(
        task_id='educator_canvas_ids',
        bucket_name=BUCKET,
        s3_key=S3_KEY_IDS,
        sql=select_query,
        )

    todo_lists = S3PythonOperator(
        task_id="todo_lists",
        aws_conn_id='aws',
        python_callable=get_todo_lists,
        bucket_name=BUCKET,
        source_s3_key=S3_KEY_IDS,
        dest_s3_key=S3_KEY_TODOS
    )

    create_prod_table = PostgresOperator(
        task_id="create_prod_table",
        sql="canvas_todo_lists.sql",
        params={
            "schema": table.schema_in_env,
            "table": table.table_in_env,
        },
        autocommit=True,
    )

    create_staging_table = PostgresOperator(
        task_id="create_staging_table",
        sql="create_todo_staging.sql",
        params={
            "schema": table.schema_in_env,
            "table": table.table_in_env,
        },
        autocommit=True,
    )

    s3_to_staging = S3ToRedshiftStagingTransfer(
        task_id="s3_to_staging",
        s3_bucket=BUCKET,
        s3_key="canvas_todo_lists",
        schema=table.schema_in_env,
        table=table.table_in_env,
        parameters=dict(
            entity_name="todo_lists",
        ),
        copy_options=["csv", "IGNOREHEADER 1"]
    )

    staging_to_prod = PostgresOperator(
            task_id="staging_to_prod",
            sql="load_todo_lists.sql",
            params=table.to_dict(),
            autocommit=True,
        )

    create_prod_table >> create_staging_table
    create_staging_table >> s3_to_staging >> staging_to_prod
    educator_canvas_ids >> todo_lists >> s3_to_staging
