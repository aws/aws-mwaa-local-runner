import inspect
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from shared.alerting import dag_success_handler, dag_failure_handler
from shared.irondata import Irondata
from shared.google_sheets import GoogleSheets
from shared.irontable import Irontable
from shared.operators.s3_to_redshift_operator import IronS3ToRedshift
from shared import s3


def create_google_sheets_subdag(
        parent_dag,
        schedule,
        start_date,
        spreadsheet_key,
        worksheet_name,
        schema_name,
        table_name,
        copy_options=["CSV", "BLANKSASNULL"],
        format_columns={}):

    dag_name = parent_dag.dag_id.split("__")[0]
    worksheet_tbl = Irontable(schema=schema_name, table=table_name, op_strategy="reset")
    subdag_name = f"google_sheets_to_redshift__{worksheet_tbl.name}"

    subdag = create_dag(
        f"{parent_dag.dag_id}.{subdag_name}",
        template_searchpath=[
            Irondata.sql_templates_path(),
            sql_templates_path()
        ],
        schedule=schedule,
        start_date=start_date)

    s3_bucket = Irondata.s3_warehouse_bucket()
    s3_key = f"google_sheets/{dag_name}/{spreadsheet_key}/{worksheet_name}"

    def extract_worksheet(ds, **kwargs):
        worksheet = GoogleSheets().open_by_key(spreadsheet_key).worksheet(worksheet_name)
        rows = worksheet.get_all_values()
        rows = [[i+1] + row for i, row in enumerate(rows)]
        for index, format_func in format_columns.items():
            for row in rows:
                try:
                    row[index] = format_func(row[index])
                except ValueError:
                    pass
        s3.upload_as_csv(s3_bucket, s3_key, rows)

    google_sheets_to_s3_op = PythonOperator(
        task_id=f"google_sheets_to_s3",
        python_callable=extract_worksheet,
        params={
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
            "spreadsheet_key": spreadsheet_key,
            "worksheet": worksheet_name,
            "format_columns": format_columns},
        provide_context=True)

    s3_to_redshift_op = IronS3ToRedshift(
        task_id="s3_to_redshift",
        redshift_conn_id='redshift',
        schema=worksheet_tbl.schema_in_env,
        table=worksheet_tbl.table_in_env,
        s3_bucket=Irondata.s3_warehouse_bucket(),
        s3_key=s3_key,
        copy_options=copy_options)

    subdag >> google_sheets_to_s3_op >> worksheet_tbl.op >> s3_to_redshift_op

    return SubDagOperator(task_id=subdag_name, subdag=subdag, dag=parent_dag)


def create_dag(dag_name, schedule, start_date, description='',
               owner="@data-engineers", template_searchpath=[], version=None,
               **kwargs):

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")

    if len(template_searchpath) == 0:
        template_searchpath = [Irondata.sql_templates_path(), sql_templates_path()]

    if version is None:
        version_str = ""
    else:
        version_str = f"__v{version}"

    if 'catchup' in kwargs:
        catchup = kwargs['catchup']
        kwargs.pop('catchup')
    elif Irondata.is_production():
        catchup = True
    else:
        catchup = False


    return DAG(
        f"{dag_name}{version_str}",
        catchup=catchup,
        default_args=default_args(owner),
        description=description,
        template_searchpath=template_searchpath,
        schedule_interval=schedule,
        start_date=start_date,
        on_failure_callback=dag_failure_handler,
        on_success_callback=dag_success_handler,
        **kwargs
        )


def default_args(owner):
    return {
        "owner": "@data-engineers",
        "depends_on_past": False,
        "email": ["data-engineering@flatironschool.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "params": {
            "test_env": Irondata.is_test()
        }
    }


# __file__ of caller
# https://stackoverflow.com/questions/13699283/how-to-get-the-callers-filename-method-name-in-python
def sql_templates_path():
    frame = inspect.stack()[2]  # 2 levels up the stack, the caller of create_dag
    module = inspect.getmodule(frame[0])
    return os.path.join(os.path.dirname(module.__file__), "sql")
