from datetime import datetime

from shared.dag_factory import create_dag
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from shared.irondata import Irondata
from shared.irontable import Irontable

from sheets_imports.refund_import.source import extract_data


REPORTING_SCHEMA = "fis"
STAGING_SCHEMA = "staging"

# Key for the paid assumptions spreadsheet
SPREADSHEET_KEY = '1DyrL0pH2qs5mfW-bQF6wj2KQMjV7MPFcV4IDeagTRM8'
WORKSHEET_TABLE_DICT = {
    'Looker': 'refund_results_form'
}

dag = create_dag(
    "refund_import",
    schedule="0 7 * * *",  # 7am local
    start_date=datetime(2023, 4, 4),
    catchup=True,
    tags=["Google Sheets"]
)


ops = {}
for worksheet_name, table_name in WORKSHEET_TABLE_DICT.items():

    worksheet_table = Irontable(schema=REPORTING_SCHEMA, table=table_name)
    ops[f"google_sheets_to_s3__{table_name}"] = PythonOperator(
        dag=dag,
        task_id=f"google_sheets_to_s3__{table_name}",
        python_callable=extract_data,
        params={
            "bucket_name": Irondata.s3_warehouse_bucket(),
            "bucket_key": f"{REPORTING_SCHEMA}/{table_name}.csv",
            "spreadsheet_key": SPREADSHEET_KEY,
            "worksheet_name": worksheet_name
        },
        provide_context=True
    )

    ops[f"reset_table__{table_name}"] = PostgresOperator(
        dag=dag,
        task_id=f"reset_table__{table_name}",
        params=worksheet_table.to_dict(),
        postgres_conn_id="redshift",
        sql=f"reset_{table_name}.sql"
    )

    ops[f"s3_to_redshift__{table_name}"] = S3ToRedshiftOperator(
        dag=dag,
        task_id=f"s3_to_redshift__{table_name}",
        redshift_conn_id="redshift",
        schema=worksheet_table.schema_in_env,
        table=worksheet_table.table_in_env,
        s3_bucket=Irondata.s3_warehouse_bucket(),
        s3_key=f"{REPORTING_SCHEMA}/{table_name}.csv",
        copy_options=["CSV", "BLANKSASNULL", "DATEFORMAT 'auto'"]
    )

    ops[f"google_sheets_to_s3__{table_name}"] >> ops[f"reset_table__{table_name}"] \
        >> ops[f"s3_to_redshift__{table_name}"]
