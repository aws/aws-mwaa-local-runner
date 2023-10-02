# Packages
import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import ShortCircuitOperator

# Flatiron defined code
from shared.irondata import Irondata
from shared.irontable import Irontable
from shared.dag_factory import create_dag

# Calendly objects
from calendly.calendly_to_s3 import to_s3

# Code timestamp
start_date = pendulum.datetime(2023, 1, 1, tz="America/New_York")

# Configurable variables
SCHEMA = "calendly"
CALENDLY_TABLES = ['events', 'invitees', 'questions_and_answers']

dag = create_dag(
    "calendly",
    schedule="0 0 * * *",
    start_date=start_date,
    max_active_runs=1,
    tags=["API"]
    )

# Table Dependencies
# event_types -> This table connects to events, but does not
#                rely on api calls to the events table
# events
#      |
#      invitees -> Updates to this table rely on the results from the `events` endpoint
#           |
#           questions_and_answers -> Updates to this table rely on the results from the `invitees` endpoint
calendly_to_s3_op = ShortCircuitOperator(
    dag=dag,
    task_id=f"calendly_to_s3",
    python_callable=to_s3,
    op_kwargs={
        "bucket": Irondata.s3_warehouse_bucket(),
        "start": "{{ data_interval_start }}",
        "end": "{{ data_interval_end }}",
        "schema": SCHEMA
    },
    provide_context=True)

for table_name in CALENDLY_TABLES:

    entity = Irontable(schema=SCHEMA,
                       table=table_name)

    create_table_op = PostgresOperator(
        dag=dag,
        task_id=f"create_{entity.table}_table",
        sql=f"create_{table_name}_table.sql",
        params={
            "schema": entity.schema_in_env,
            "table": entity.table_in_env
        },
        autocommit=True)
    
    s3_to_redshift_op = S3ToRedshiftOperator(
        dag=dag,
        method="UPSERT",
        task_id=f"s3_to_redshift_{table_name}",
        schema=entity.schema_in_env,
        table=entity.table_in_env,
        s3_bucket=Irondata.s3_warehouse_bucket(),
        s3_key=f"{entity.schema_in_env}/{{{{ds}}}}/{entity.table_in_env}",
        copy_options=["CSV"]
    )

    calendly_to_s3_op >> create_table_op >> s3_to_redshift_op 
