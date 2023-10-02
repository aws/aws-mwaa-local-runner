from canvas_rollcall_scheduler.report_scheduler import run as run_report_scheduler

from airflow.operators.python import PythonVirtualenvOperator

from shared.dag_factory import create_dag

schedule = "0 6 * * *"
start_date = "2023-04-25"

dag = create_dag(
    "canvas_rollcall_scheduler",
    description='https://flatiron.atlassian.net/l/c/tPjxcBTT',
    schedule=schedule,
    start_date=start_date)

PythonVirtualenvOperator(
    dag=dag,
    task_id='schedule_report__attendance',
    python_callable=run_report_scheduler,
    requirements=["requests==2.24.0", "beautifulsoup4==4.9.0"],
    op_kwargs={"ds": "{{ds}}"})
