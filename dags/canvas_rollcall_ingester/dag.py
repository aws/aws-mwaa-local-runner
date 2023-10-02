from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonVirtualenvOperator, ShortCircuitOperator, PythonOperator

from shared.irondata import Irondata
from shared.dag_factory import create_dag
from shared.irontable import Irontable
from shared.s3 import check_for_wildcard_key, copy_to_staging

from canvas_rollcall_ingester.report_extractor import run as run_report_extractor

schedule = "0 */2 * * *"
start_date = "2023-08-14"

ROLLCALL_SCHEMA = 'canvas_rollcall'
ATTENDANCE_COLS = [
    'course_id',
    'sis_course_id',
    'course_code',
    'course_name',
    'section_name',
    'section_id',
    'sis_section_id',
    'teacher_id',
    'sis_teacher_id',
    'teacher_name',
    'student_id',
    'sis_student_id',
    'student_name',
    'class_date',
    'attendance',
    '"timestamp"',
    'blank'
]


dag = create_dag(
    "canvas_rollcall_ingester",
    description='https://flatiron.atlassian.net/l/c/tPjxcBTT',
    schedule=schedule,
    start_date=start_date,
    catchup=False)

copy_reports_op = PythonVirtualenvOperator(
    dag=dag,
    task_id=f"extract_to_s3__attendance",
    python_callable=run_report_extractor,
    requirements=["beautifulsoup4==4.9.0"],
    op_kwargs={
        'ds': '{{ds}}',
        'bucket_name': Irondata.s3_warehouse_bucket(),
        'prefix': 'canvas_rollcall/emails/unprocessed',
    })

attendance_tbl = Irontable(schema=ROLLCALL_SCHEMA, table="attendance")

# Check if attendance records exist in bucket (will skip remaining tasks if not)
check_s3_op = ShortCircuitOperator(
    task_id="check_s3_for_key"
    , python_callable=check_for_wildcard_key
    , op_kwargs={
        "wildcard_key": "canvas_rollcall/reports/{{ ds }}/attendance-*",
        "bucket_name": Irondata.s3_warehouse_bucket()
    }
    , provide_context=True
)

create_table_op = \
    PostgresOperator(
        dag=dag,
        task_id=f"create_table__{attendance_tbl.name}",
        sql=f"create_{attendance_tbl.name}.sql",
        params={'schema': attendance_tbl.schema_in_env, 'table': attendance_tbl.table_in_env},
        postgres_conn_id="redshift",
        autocommit=True)

create_staging_table_op = \
    PostgresOperator(
        dag=dag,
        task_id=f"create_staging_table__attendance",
        sql="create_staging_tables.sql",
        params={
            "schema": attendance_tbl.schema_in_env,
            "tables": [attendance_tbl.table_in_env]
        },
        postgres_conn_id="redshift",
        autocommit=True)

# For each key in bucket, copy into a temp table and upsert into staging table
s3_to_redshift_staging_op = PythonOperator(
    task_id="s3_to_redshift_staging",
    python_callable=copy_to_staging,
    op_kwargs={
        "s3_bucket": Irondata.s3_warehouse_bucket(),
        "prefix": "canvas_rollcall/reports/{{ ds }}/attendance-",
        "column_list": ATTENDANCE_COLS,
        "table": attendance_tbl,
        "upsert_keys": ["course_id", "student_id", "class_date"],
        "copy_options": ['CSV', 'IGNOREHEADER 1']
    }
)

# Upsert prod table from staging
load_from_staging_op = \
    PostgresOperator(
        dag=dag,
        task_id=f"load_from_staging__{attendance_tbl.name}",
        sql="load_staged_attendance.sql",
        params={
            "schema": attendance_tbl.schema_in_env,
            "table": attendance_tbl.table_in_env,
            "column_list": ATTENDANCE_COLS,
            "compound_key1": 'course_id',
            "compound_key2": 'student_id',
            "compound_key3": 'class_date'
        },
        postgres_conn_id="redshift",
        autocommit=True)

copy_reports_op >> check_s3_op >> create_table_op >> create_staging_table_op >> s3_to_redshift_staging_op >> load_from_staging_op
