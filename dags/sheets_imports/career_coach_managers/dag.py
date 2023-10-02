from shared.dag_factory import create_dag
from shared.sql_utils import reset_dynamic_table
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from shared.irondata import Irondata
from shared.irontable import Irontable
import pendulum

from shared.google_sheets import GoogleSheets

start_date = pendulum.datetime(2023, 4, 4, tz="America/New_York")

ccm_table = Irontable(schema="career_services",
                      table="coach_managers")

dag = create_dag(
    "career_coach_managers",
    schedule="30 6 * * *",  # This is 6:30am local
    start_date=start_date,
    tags=["Google Sheets"]
)

# Google Sheet import - Career Coaches by Manager
coach_sheet_to_s3 = PythonOperator(
    dag=dag,
    task_id="coach_sheet_to_s3",
    python_callable=GoogleSheets().to_s3,
    provide_context=True,
    op_kwargs={
        "bucket_name": Irondata.s3_warehouse_bucket()
        , "bucket_key": f"{ccm_table.schema_in_env}/{{{{ds}}}}/career_coach_managers.csv"
        , "spreadsheet_id": "1bLzsdRHXlGSuZmox4xvo5feeXPsgV7EXMmwcFcsZuAc"
        , "worksheet_name": "Coaches"
        , "skip_n_rows": 3
    }
)

coach_reset_table = PythonOperator(
    dag=dag,
    task_id="coach_reset_table",
    python_callable=reset_dynamic_table,
    op_kwargs={
        'bucket_name': Irondata.s3_warehouse_bucket(),
        'object_key': f"{ccm_table.schema_in_env}/{{{{ds}}}}/career_coach_managers.csv",
        'schema': ccm_table.schema_in_env,
        'table': ccm_table.table_in_env,
        'varchar_size': 1028
    }
)

coach_s3_to_redshift = S3ToRedshiftOperator(
    dag=dag,
    task_id="coach_s3_to_redshift",
    schema=ccm_table.schema_in_env,
    table=ccm_table.table_in_env,
    s3_bucket=Irondata.s3_warehouse_bucket(),
    s3_key=f"{ccm_table.schema_in_env}/{{{{ds}}}}/career_coach_managers.csv",
    copy_options=["CSV", "BLANKSASNULL", "IGNOREHEADER 1", "FILLRECORD"],
    method="REPLACE"
    )

# Career Coach workflow
coach_sheet_to_s3 >> coach_reset_table >> coach_s3_to_redshift