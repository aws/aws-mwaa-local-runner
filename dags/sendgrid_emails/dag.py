from datetime import timedelta
from shared.dag_factory import create_dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

import pendulum
from shared.irondata import Irondata
from shared.irontable import Irontable
from sendgrid_emails.src import send_emails

DAG_CONFIG = dict(
    dag_name="sendgrid_emails",
    schedule='30 * * * *',
    start_date=pendulum.datetime(2023, 8, 18, tz="America/New_York"),
    catchup=False,
    description="Use data in Redshift to send automated Emails via SendGrid"
)
DEFAULT_SENDER = "operations@flatironschool.com"
DEFAULT_FREQ = "daily"
TEMPLATE_DICT = [
    {"id": "d-87c966f7792642b1a00bce4e5c9a1e55", "name": "flex_inactive_22_days"},
    {"id": "d-4d69274af98d4d7d8f498808b2a60d78", "name": "flex_inactive_60_days"},
    {"id": "d-ba2763a238324192831190f5c36b02ff", "name": "job_search_day_30"},
    {"id": "d-00b507c7695b4f028f3355b265372228", "name": "job_search_day_90"},
    {"id": "d-9857cbd40b844cfa83ff9989da48bce0", "name": "job_search_day_120"},
    {"id": "d-f016d3be9c764519b17421e783ce6104", "name": "end_of_job_search"},
    {"id": "d-83ace95c8c40484c8d145c7ca8803604", "name": "flex_expiration_day_180", "sender": "studentservices@flatironschool.com"},
    {"id": "d-d781fe00060a401c93c024227836a547", "name": "flex_expiration_reminder", "sender": "studentservices@flatironschool.com"},
    {"id": "d-54491ad4f5244203bac92d28950f20a2", "name": "flex_expiration_day_of", "sender": "studentservices@flatironschool.com"},
    {"id": "d-4ed6e42c5f2348f3b412eff2b0e0d4dc", "name": "holding_cohort_nurture", "sender": "onboarding@flatironschool.com"},
    {"id": "d-0dcc4e766500431d95fb2b0abbb33a92", "name": "newly_enrolled", "freq": "hourly"},
    {"id": "d-e27dbd601224486f91e3b25e27b4548f", "name": "ask_ada", "freq": "weekly"}
]

email_events_table = Irontable(schema="fis", table="sendgrid_email_events")

def _brancher(data_interval_end):
    local_ts = pendulum.timezone("America/New_York").convert(data_interval_end)
    # Only run daily tasks at 8:30 (when main_reporting runs)
    if local_ts.hour == 8 and local_ts.minute == 30:
        # Monday is 0, so only do weekly tasks on Fridays
        if local_ts.weekday() == 4:
            return ['weekly_tasks', 'daily_tasks', 'hourly_tasks']
        else:
            return ['daily_tasks', 'hourly_tasks']
    else:
        return ['hourly_tasks']


with create_dag(**DAG_CONFIG) as dag:

    daily_tasks = EmptyOperator(task_id="daily_tasks")
    hourly_tasks = EmptyOperator(task_id="hourly_tasks")
    weekly_tasks = EmptyOperator(task_id="weekly_tasks")

    branch_task = BranchPythonOperator(
        task_id="branch",
        python_callable=_brancher
    )

    email_events = PostgresOperator(
        task_id="email_events_reset",
        sql="email_events.sql",
        params={
            "schema": email_events_table.schema_in_env,
            "table": email_events_table.table_in_env
        }
    )

    rosters_sensor = ExternalTaskSensor(
        task_id="rosters_sensor",
        external_dag_id='main_reporting',
        external_task_id='students',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        # Per docs, "For yesterday, use [positive!] datetime.timedelta(days=1)"
        # This runs at 8:30am local (aka 7:30am execution_date),
        # so, we need to look forward 1 hour
        execution_delta=timedelta(hours=-1)
    )

    email_events >> branch_task >> [weekly_tasks, daily_tasks, hourly_tasks]
    
    for template in TEMPLATE_DICT:
        
        template_id = template.get("id")
        template_name = template.get("name")
        sender = template.get("sender") or DEFAULT_SENDER
        freq = template.get("freq") or DEFAULT_FREQ

        table = Irontable(schema="staging", table=f"sendgrid_{template_name}")

        s3_key = f"{table.schema_in_env}/{{{{ds}}}}"

        reset_table = PostgresOperator(
            dag=dag,
            task_id=f"{template_name}_reset",
            sql=f"{template_name}.sql",
            params={
                "schema": table.schema_in_env,
                "table": table.table_in_env,
                "template_id": template_id
            },
            postgres_conn_id="redshift",
            autocommit=True
        )

        redshift_to_s3 = RedshiftToS3Operator(
            dag=dag,
            task_id=f"{template_name}_redshift_to_s3",
            schema=table.schema_in_env,
            table=table.table_in_env,
            s3_bucket=Irondata.s3_warehouse_bucket(),
            s3_key=s3_key,
            redshift_conn_id="redshift",
            autocommit=True,
            unload_options=["CSV", "allowoverwrite", "parallel off", "HEADER"]
        )

        s3_to_sendgrid = PythonOperator(
            dag=dag,
            task_id=f"{template_name}_s3_to_sendgrid",
            python_callable=send_emails,
            op_kwargs={
                "bucket": Irondata.s3_warehouse_bucket(),
                "key": f"{s3_key}/{table.table_in_env}_000",
                "template_id": template_id,
                "sender": sender,
                "is_prod": Irondata.is_production()
            },
            provide_context=True
        )

        if freq == "weekly":
            weekly_tasks >> reset_table >> redshift_to_s3 >> s3_to_sendgrid
        elif freq == "daily":
            daily_tasks >> rosters_sensor >> reset_table >> redshift_to_s3 >> s3_to_sendgrid
        else:
            hourly_tasks >> reset_table >> redshift_to_s3 >> s3_to_sendgrid