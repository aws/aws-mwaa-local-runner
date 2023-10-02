from shared.dag_factory import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

import os
from io import StringIO
from jinja2 import Environment, FileSystemLoader

import pendulum
from shared.irondata import Irondata
from shared.irontable import Irontable

DAG_CONFIG = dict(
    dag_name="postgres_ea_data",
    schedule='0 4 * * *',
    start_date=pendulum.datetime(2023, 5, 23, tz="America/New_York"),
    catchup=True,
    max_active_runs=1,
    description="Extract EA data from PostgreSQL"
    )

raw_ea_data = Irontable(schema="postgres_service_documents", table="raw_ea_data")
ea_addresses = Irontable(schema="postgres_service_documents", table="ea_addresses")

s3_bucket = Irondata.s3_warehouse_bucket()
s3_key = f"{raw_ea_data.schema_in_env}/{{{{ ds }}}}/{raw_ea_data.table_in_env}"

def get_query(filename, **kwargs):
    temp_path = os.path.join(os.path.dirname(__file__), "sql/")
    env = Environment(loader=FileSystemLoader(temp_path))
    template = env.get_template(filename)
    sql = template.render({
        "params": {
            **kwargs
        }
    })
    return sql

with create_dag(**DAG_CONFIG) as dag:
    postgres_to_s3 = SqlToS3Operator(
        task_id="postgres_to_s3",
        query=get_query(
            filename="extract_postgres_ea_data.sql", 
            data_interval_start="{{ data_interval_start }}",
            data_interval_end="{{ data_interval_end }}"
            ),
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        replace=True,
        sql_conn_id="postgres_service_documents",
        aws_conn_id="aws_default"
    )

    create_table = PostgresOperator(
        task_id='create_table',
        sql="create_raw_ea_data_table.sql",
        params={
            "schema": raw_ea_data.schema_in_env,
            "table": raw_ea_data.table_in_env
        },
        autocommit=True
    )

    s3_to_redshift = S3ToRedshiftOperator(
        task_id="s3_to_redshift",
        method="UPSERT",
        schema=raw_ea_data.schema_in_env,
        table=raw_ea_data.table_in_env,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        copy_options=["CSV", "BLANKSASNULL", "TIMEFORMAT 'auto'", "IGNOREHEADER 1"],
        upsert_keys=["tabid"]
    )

    transformer = PostgresOperator(
        task_id='transformer',
        sql="transform_ea_addresses.sql",
        params={
            "schema": ea_addresses.schema_in_env,
            "table": ea_addresses.table_in_env,
            "raw_data": raw_ea_data
        },
        autocommit=True
    )

    postgres_to_s3 >> create_table >> s3_to_redshift >> transformer