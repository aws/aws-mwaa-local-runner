from shared.dag_factory import create_dag
from shared.sql_utils import reset_dynamic_table
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from shared.irondata import Irondata
from shared.irontable import Irontable
import pendulum

from shared.google_sheets import GoogleSheets

start_date = pendulum.datetime(2023, 7, 28, tz="America/New_York")

SCHEMA = "fis"
SPREADSHEET_KEY = '1B6WlV2Vri9uTMAHiqFWwagRjbOz8kig0LtprwRtn6m4'
WORKSHEET_TABLE_DICT = {
    'Ascent Import': 'ascent_sheet',
    'Climb Import': 'climb_sheet',
    'EdAid Import': 'edaid_sheet'
}

dag = create_dag(
    "central_loan_data",
    schedule="0 10 * * *", 
    start_date=start_date,
    catchup=False,
    tags=["Google Sheets"]
)

for sheetname, tablename in WORKSHEET_TABLE_DICT.items():

    table = Irontable(schema=SCHEMA, table=tablename)
    bucket = Irondata.s3_warehouse_bucket()
    key = f"{table.schema_in_env}/{{{{ds}}}}/{table.table_in_env}.csv"

    to_s3 = PythonOperator(
        dag=dag,
        task_id=f"{tablename}_to_s3",
        python_callable=GoogleSheets().to_s3,
        provide_context=True,
        op_kwargs={
        "bucket_name": bucket
        , "bucket_key": key
        , "spreadsheet_id": SPREADSHEET_KEY
        , "worksheet_name": sheetname
    })

    reset_table = PythonOperator(
        dag=dag,
        task_id=f"{tablename}_reset_table",
        python_callable=reset_dynamic_table,
        op_kwargs={
            'bucket_name': bucket,
            'object_key': key,
            'schema': table.schema_in_env,
            'table': table.table_in_env,
            'varchar_size': 1028
    })

    s3_to_redshift = S3ToRedshiftOperator(
        dag=dag,
        task_id=f"{tablename}_s3_to_redshift",
        schema=table.schema_in_env,
        table=table.table_in_env,
        s3_bucket=bucket,
        s3_key=key,
        copy_options=["CSV", "BLANKSASNULL", "IGNOREHEADER 1"],
        method="REPLACE"
        )

    # DAG dependencies
    to_s3 >> reset_table >> s3_to_redshift
