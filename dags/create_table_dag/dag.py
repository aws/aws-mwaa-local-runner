from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator

from shared.irontable import Irontable
from shared.dag_factory import create_dag
from datetime import datetime
date = datetime(2023, 4, 4)
schedule = "0 7 * * *"
start_date = date

TEST_SCHEMA = 'dev_derek'

TEST_TABLE_COLS = [
  ("learn_uuid", "VARCHAR(256)"),
  ("mbg_eligibility", "VARCHAR(256)"),
  ("job_seeking_status", "VARCHAR(256)"),
  ("coach", "VARCHAR(256)"),
  ("graduation_date", "VARCHAR(256)"),
  ("snapshot_date", "DATE")
]

dag = create_dag(
    "create_table_test_dag",
    description='testing for key creation',
    schedule=schedule,
    start_date=start_date,
)
test_tbl = Irontable(
    schema=TEST_SCHEMA,
    table="create_table_test",
    primary_key="learn_uuid",
    sort_key="snapshot_date",
    dist_key="job_seeking_status")

create_table_task = PostgresOperator(
    dag=dag,
    task_id='create_redshift_table_for_tests_task',
    params={
        'schema': test_tbl.schema_in_env,
        'table': test_tbl.table,
        'cols': list(map(lambda col: " ".join(col), TEST_TABLE_COLS)),
        'pk': test_tbl.primary_key,
        'sort_key': test_tbl.sort_key,
        'dist_key': test_tbl.dist_key
    },
    sql="create_table.sql")


begin = DummyOperator(task_id="begin")

end = DummyOperator(task_id="end")

copy_data_task = S3ToRedshiftOperator(
    dag=dag,
    redshift_conn_id="redshift_default",
    task_id=f'copy_data_to_test_table',
    schema=TEST_SCHEMA,
    table=test_tbl.table,
    s3_bucket="fis-data-warehouse-development",
    s3_key="test_final.csv",
    copy_options=["csv", "acceptanydate", "ignoreheader 1", "dateformat 'auto'"])

other_copy_data_task = S3ToRedshiftOperator(
    dag=dag,
    redshift_conn_id="redshift_default",
    task_id=f'other_copy_data_to_test_table',
    schema=TEST_SCHEMA,
    table=test_tbl.table,
    s3_bucket="fis-data-warehouse",
    s3_key="test_final.csv",
    copy_options=["csv", "acceptanydate", "ignoreheader 1", "dateformat 'auto'"])

chain(begin, create_table_task, copy_data_task, end)

# sql="""
# CREATE TABLE dev_derek.create_table_test AS
# select *
# from fis.'{{ params.table }}'
# order by run_date desc limit 10;
# """,
