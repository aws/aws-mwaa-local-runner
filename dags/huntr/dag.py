from datetime import datetime

from shared.dag_factory import create_dag
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from shared.irontable import Irontable
from shared.irondata import Irondata
from shared.sql_utils import sqlize_names

from huntr.huntr_to_s3 import huntr_to_s3, _get_member_fields
from huntr.huntr_client import HuntrClient

start_date = datetime(2023, 4, 4)

dag = create_dag(
    "huntr",
    schedule="0 9 * * *",  # This is 9:00am local
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["Transformation"]
)

member_fields = sqlize_names(_get_member_fields(HuntrClient()).values())

tables = {
    "members": {"copy_params": {"method": "REPLACE"}, "cols": member_fields},
    "advisors": {"copy_params": {"method": "REPLACE"}},
    "job_posts": {"copy_params": {"method": "REPLACE"}},
    "activities": {"copy_params": {"method": "UPSERT", "upsert_keys": ["id"]}},
    "candidates_created": {"copy_params": {"method": "UPSERT", "upsert_keys": ["id"]}},
    "documents_created": {"copy_params": {"method": "UPSERT", "upsert_keys": ["id"]}}
}

huntr_to_s3_op = ShortCircuitOperator(
    dag=dag,
    task_id="huntr_api_to_s3",
    python_callable=huntr_to_s3,
    op_kwargs={
        "start": "{{ data_interval_start.int_timestamp }}"
        , "end": "{{ data_interval_end.int_timestamp }}"
        , "bucket": Irondata.s3_warehouse_bucket()
        , "tables": tables
    },
    provide_context=True)

for tablename, params in tables.items():
    table = Irontable(schema="huntr_api", table=tablename)

    create_table = PostgresOperator(
        task_id=f'create_{tablename}',
        sql=f"create_{tablename}.sql",
        params={
            "schema": table.schema_in_env,
            "table": table.table_in_env,
            **params
        },
        autocommit=True
    )
    
    s3_to_redshift_op = S3ToRedshiftOperator(
        dag=dag,
        task_id=f"{tablename}_s3_to_redshift",
        schema=table.schema_in_env,
        table=table.table_in_env,
        s3_bucket=Irondata.s3_warehouse_bucket(),
        s3_key=f"{table.schema_in_env}/{{{{ds}}}}/{table.table_in_env}",
        copy_options=["CSV", "BLANKSASNULL", "TIMEFORMAT 'epochsecs'"],
        **params.get("copy_params"))
    
    huntr_to_s3_op >> create_table >> s3_to_redshift_op
