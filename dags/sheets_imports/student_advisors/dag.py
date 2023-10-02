from shared.dag_factory import create_dag
from shared.sql_utils import reset_dynamic_table
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from shared.irondata import Irondata
from shared.irontable import Irontable
import pendulum

from shared.google_sheets import GoogleSheets

start_date = pendulum.datetime(2023, 4, 4, tz="America/New_York")
schema = "fis"
table = "student_advisors_sheet"
advisors_table = Irontable(schema=schema, table=table)

dag = create_dag(
    "student_advisors",
    schedule="30 7 * * *",  # This is 7:30am local
    start_date=start_date,
    catchup=True,
    tags=["Google Sheets"]
)

advisors_to_s3 = PythonOperator(
    dag=dag,
    task_id="advisors_to_s3",
    python_callable=GoogleSheets().to_s3,
    provide_context=True,
    op_kwargs={
        "bucket_name": Irondata.s3_warehouse_bucket()
        , "bucket_key": f"{advisors_table.schema_in_env}/{{{{ds}}}}/advisors.csv"
        , "spreadsheet_id": "1u_46LTnAELrscw8m3W4YzGxB-Jv5DGj_hCN00RU-qqY"
        , "worksheet_name": "Advisors"
    })

advisors_reset_table = PythonOperator(
    dag=dag,
    task_id="advisors_reset_table",
    python_callable=reset_dynamic_table,
    op_kwargs={
        'bucket_name': Irondata.s3_warehouse_bucket(),
        'object_key': f"{advisors_table.schema_in_env}/{{{{ds}}}}/advisors.csv",
        'schema': advisors_table.schema_in_env,
        'table': advisors_table.table_in_env,
        'varchar_size': 1028
})

advisors_s3_to_redshift = S3ToRedshiftOperator(
    dag=dag,
    task_id="advisors_s3_to_redshift",
    schema=advisors_table.schema_in_env,
    table=advisors_table.table_in_env,
    s3_bucket=Irondata.s3_warehouse_bucket(),
    s3_key=f"{advisors_table.schema_in_env}/{{{{ds}}}}/advisors.csv",
    copy_options=["CSV", "BLANKSASNULL", "IGNOREHEADER 1"],
    method="REPLACE"
    )

# DAG dependencies
advisors_to_s3 >> advisors_reset_table >> advisors_s3_to_redshift
