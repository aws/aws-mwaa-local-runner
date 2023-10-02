from shared.dag_factory import create_dag
from shared.sql_utils import reset_dynamic_table
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from shared.irondata import Irondata
from shared.irontable import Irontable
import pendulum

from shared.google_sheets import GoogleSheets

start_date = pendulum.datetime(2023, 4, 4, tz="America/New_York")
schema = "career_services"
table = "alumni_job_placements"
ajp_table = Irontable(schema=schema, table=table)

dag = create_dag(
    "alumni_job_placements",
    schedule="30 6 * * *",  # This is 6:30am local
    start_date=start_date,
    tags=["Google Sheets"]
)

ajp_to_s3 = PythonOperator(
    dag=dag,
    task_id="ajp_to_s3",
    python_callable=GoogleSheets().to_s3,
    provide_context=True,
    op_kwargs={
        "bucket_name": Irondata.s3_warehouse_bucket()
        , "bucket_key": f"{ajp_table.schema_in_env}/{{{{ds}}}}/ajp.csv"
        , "spreadsheet_id": "1U22dHBzoonXL3dgD6D_dauLhjeO3VxwXCUBBSCorxWY"
        , "worksheet_name": "#Jobs"
    })

ajp_reset_table = PythonOperator(
    dag=dag,
    task_id="ajp_reset_table",
    python_callable=reset_dynamic_table,
    op_kwargs={
        'bucket_name': Irondata.s3_warehouse_bucket(),
        'object_key': f"{ajp_table.schema_in_env}/{{{{ds}}}}/ajp.csv",
        'schema': ajp_table.schema_in_env,
        'table': ajp_table.table_in_env,
        'varchar_size': 1028
})

ajp_s3_to_redshift = S3ToRedshiftOperator(
    dag=dag,
    task_id="ajp_s3_to_redshift",
    schema=ajp_table.schema_in_env,
    table=ajp_table.table_in_env,
    s3_bucket=Irondata.s3_warehouse_bucket(),
    s3_key=f"{ajp_table.schema_in_env}/{{{{ds}}}}/ajp.csv",
    copy_options=["CSV", "BLANKSASNULL", "IGNOREHEADER 1"],
    method="REPLACE"
    )

# DAG dependencies
ajp_to_s3 >> ajp_reset_table >> ajp_s3_to_redshift
