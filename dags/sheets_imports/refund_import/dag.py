from shared.dag_factory import create_dag
from shared.sql_utils import reset_dynamic_table
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from shared.irondata import Irondata
from shared.irontable import Irontable
import pendulum

from shared.google_sheets import GoogleSheets



# Key for the paid assumptions spreadsheet
SPREADSHEET_KEY = '1DyrL0pH2qs5mfW-bQF6wj2KQMjV7MPFcV4IDeagTRM8'
WORKSHEET_NAME = "Form Responses 1"

REPORTING_SCHEMA = "fis"

dag = create_dag(
    "refund_import",
    schedule="0 7 * * *",  # 7am local
    start_date=pendulum.datetime(2023, 9, 25, tz="America/New_York"),
    catchup=False,
    tags=["Google Sheets"]
)

table = Irontable(schema=REPORTING_SCHEMA, table="refund_results_form")
BUCKET = Irondata.s3_warehouse_bucket()
KEY = f"{REPORTING_SCHEMA}/{table.name}.csv"

to_s3 = PythonOperator(
        dag=dag,
        task_id=f"{table.name}_to_s3",
        python_callable=GoogleSheets().to_s3,
        provide_context=True,
        op_kwargs={
        "bucket_name": BUCKET
        , "bucket_key": KEY
        , "spreadsheet_id": SPREADSHEET_KEY
        , "worksheet_name": WORKSHEET_NAME
    })

reset_table = PythonOperator(
    dag=dag,
    task_id=f"{table.name}_reset_table",
    python_callable=reset_dynamic_table,
    op_kwargs={
        'bucket_name': BUCKET,
        'object_key': KEY,
        'schema': table.schema_in_env,
        'table': table.table_in_env,
        'varchar_size': "MAX"
})

s3_to_redshift = S3ToRedshiftOperator(
    dag=dag,
    task_id=f"{table.name}_s3_to_redshift",
    schema=table.schema_in_env,
    table=table.table_in_env,
    s3_bucket=BUCKET,
    s3_key=KEY,
    copy_options=["CSV", "BLANKSASNULL", "IGNOREHEADER 1", "FILLRECORD"],
    method="REPLACE"
    )

# DAG dependencies
to_s3 >> reset_table >> s3_to_redshift
