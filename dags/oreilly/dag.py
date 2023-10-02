from airflow import DAG
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta
from shared.alerting import dag_success_handler, dag_failure_handler, task_failure_handler
from shared.irondata import Irondata
from shared.irontable import Irontable
from shared.dag_factory import create_dag
from oreilly.oreilly_to_s3 import oreilly_extract

start_date = datetime(2023, 4, 4)
schedule = "0 5 * * *"

default_args = {
    "owner": "@data-education",
    "depends_on_past": False,
    "start_date": start_date,
    "email": ["data-engineering@flatironschool.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_handler
}

dag = create_dag(
    "oreilly",
    schedule=schedule,
    start_date=start_date,
    tags=["API"]
    )

SCHEMA = "oreilly"
USER_ACTIVITY_TABLE = "user_activity"
USAGE_TIMELINE_TABLE = "usage_timeline"

user_activity_table = Irontable(schema=SCHEMA, table=USER_ACTIVITY_TABLE, primary_key="id")
usage_timeline_table = Irontable(schema=SCHEMA, table=USAGE_TIMELINE_TABLE, primary_key="id")

oreilly_to_s3_op = ShortCircuitOperator(
    dag=dag,
    task_id="oreilly_to_s3",
    python_callable=oreilly_extract,
    op_kwargs={
        "bucket": Irondata.s3_warehouse_bucket(),
        "user_activity_table": user_activity_table,
        "usage_timeline_table": usage_timeline_table
    })

reset_table_op = PostgresOperator(
    dag=dag,
    task_id='reset_user_activity_table',
    sql="reset_user_activity_table.sql",
    params={
        "schema": user_activity_table.schema_in_env,
        "table": user_activity_table.table_in_env
    },
    postgres_conn_id="redshift",
    autocommit=True)

s3_to_redshift_op = S3ToRedshiftOperator(
    dag=dag,
    task_id="s3_to_redshift__user_activity",
    start_date=start_date,
    schema=user_activity_table.schema_in_env,
    table=user_activity_table.table_in_env,
    s3_bucket=Irondata.s3_warehouse_bucket(),
    s3_key=f"{user_activity_table.schema_in_env}/{{{{ds}}}}/{user_activity_table.table_in_env}",
    copy_options=["CSV", "BLANKSASNULL"])

create_table_op = PostgresOperator(
    dag=dag,
    task_id="create_usage_timeline_table",
    sql="create_usage_timeline_table.sql",
    params={
        "schema": usage_timeline_table.schema_in_env,
        "table": usage_timeline_table.table_in_env
    },
    autocommit=True,
    postgres_conn_id="redshift")

s3_to_redshift_staging_transfer_op = S3ToRedshiftOperator(
    dag=dag,
    method="UPSERT",
    task_id="s3_to_redshift_staging__usage_timeline",
    schema=usage_timeline_table.schema_in_env,
    table=usage_timeline_table.table_in_env,
    s3_bucket=Irondata.s3_warehouse_bucket(),
    s3_key=f"{usage_timeline_table.schema_in_env}/{{{{ds}}}}/{usage_timeline_table.table_in_env}",
    copy_options=["CSV"]
)

oreilly_to_s3_op >> reset_table_op >> s3_to_redshift_op
oreilly_to_s3_op >> create_table_op >> s3_to_redshift_staging_transfer_op
