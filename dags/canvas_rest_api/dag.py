from shared.dag_factory import create_dag
from shared.irondata import Irondata
from shared.irontable import Irontable

from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

import pendulum
from canvas_rest_api.src import canvas_extract

DAG_CONFIG = dict(
    dag_name="canvas_rest_api",
    schedule='0 12 * * *',
    start_date=pendulum.datetime(2023, 4, 4, tz="America/New_York"),
    catchup=False,
    description="Use REST API to pull data from Canvas instance",
    tags=["API"]
    )

SCHEMA = "canvas_rest_api"
TABLES = {
    "enterprise_accounts": Irontable(schema=SCHEMA, table="enterprise_accounts", primary_key="id"),
    "enterprise_courses": Irontable(schema=SCHEMA, table="enterprise_courses", primary_key="id"),
    "enterprise_enrollments": Irontable(schema=SCHEMA, table="enterprise_enrollments", primary_key="id")
}

with create_dag(**DAG_CONFIG) as dag:

    enterprise_to_s3 = ShortCircuitOperator(
        task_id="enterprise_canvas_to_s3",
        python_callable=canvas_extract,
        op_kwargs={
            "bucket": Irondata.s3_warehouse_bucket(),
            **TABLES
        },
        provide_context=True)
    
    for name, table in TABLES.items():

        reset_table = PostgresOperator(
            task_id=f'reset_{name}',
            sql=f"reset_{name}.sql",
            params={
                "schema": table.schema_in_env,
                "table": table.table_in_env
            },
            autocommit=True)

        s3_to_redshift = S3ToRedshiftOperator(
            task_id=f"s3_to_redshift__{name}",
            schema=table.schema_in_env,
            table=table.table_in_env,
            s3_bucket=Irondata.s3_warehouse_bucket(),
            s3_key=f"{table.schema_in_env}/{{{{ds}}}}/{table.table_in_env}",
            copy_options=["CSV", "BLANKSASNULL", "TIMEFORMAT 'auto'"])
        
        enterprise_to_s3 >> reset_table >> s3_to_redshift
