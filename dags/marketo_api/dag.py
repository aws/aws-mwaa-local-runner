from datetime import timedelta
from shared.dag_factory import create_dag
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from marketo_api.src import MarketoAPI

import pendulum
from shared.irondata import Irondata
from shared.irontable import Irontable

wpv_table = Irontable(schema="marketo_api", table="webpage_visits")
s3_bucket=Irondata.s3_warehouse_bucket()
s3_key = f"{wpv_table.schema_in_env}/{{{{ds}}}}/{wpv_table.table_in_env}"

DAG_CONFIG = dict(
    dag_name="marketo_api",
    schedule='0 5 * * *',
    start_date=pendulum.datetime(2022, 1, 1, tz="America/New_York"),
    catchup=True,
    max_active_runs=1,
    description="Extract data from Marketo API"
)

with create_dag(**DAG_CONFIG) as dag:
    extract_webpage_visits_to_s3 = PythonOperator(
        task_id="extract_webpage_visits_to_s3",
        python_callable=MarketoAPI().run_bulk_job,
        execution_timeout=timedelta(hours=1),
        op_kwargs={
            "bucket": s3_bucket,
            "key": s3_key,
            "json_filter": {
                "filter": {
                    "createdAt": {
                        "startAt": "{{ data_interval_start | ds}}",
                        "endAt": "{{ data_interval_end | ds }}"
                    },
                    "activityTypeIds": [
                        1  # Webpage visits
                    ]
                }
            }
        }
    )

    create_webpage_visits_table = PostgresOperator(
        task_id='create_webpage_visits_table',
        sql="create_webpage_visits_table.sql",
        params={
            "schema": wpv_table.schema_in_env,
            "table": wpv_table.table_in_env
        },
        autocommit=True
    )

    webpage_visits_s3_to_redshift = S3ToRedshiftOperator(
        task_id="webpage_visits_s3_to_redshift",
        method="UPSERT",
        schema=wpv_table.schema_in_env,
        table=wpv_table.table_in_env,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        copy_options=["CSV", "BLANKSASNULL", "TIMEFORMAT 'auto'", "IGNOREHEADER 1"]
    )

    extract_webpage_visits_to_s3 >> create_webpage_visits_table >> webpage_visits_s3_to_redshift