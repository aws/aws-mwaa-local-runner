# Source code
from shared.irondata import Irondata
from shared.irontable import Irontable
from shared.dag_factory import create_dag
from matriculation_forecast.src import generate_predictions
# Dependencies
import pendulum
from pathlib import Path
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from shared.operators.s3_to_redshift_staging_operator import S3ToRedshiftStagingTransfer
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator


S3_BUCKET = Irondata.s3_warehouse_bucket()
MODEL_NAME = None

DAG_CONFIG = dict(
    dag_name="cohort_start_short_range",
    schedule='0 8 * * *',
    start_date=pendulum.datetime(2023, 4, 4, tz="America/New_York"), #datetime(2023, 4, 4),
    max_active_runs=1,
    catchup=True,
    )


table = Irontable(
    schema='inference',
    table='cohort_start_short_range',
    primary_key=['cohort_start_date', 'days_till_start']
)

enrollments_query_path = Path(__file__).resolve().parent / 'sql' / 'enrollment_states.sql'
with enrollments_query_path.open('r') as file:
    enrollments_query = file.read().replace("''", "'")

enrollments_table = Irontable(schema='staging', table='enrollment_states') # 'staging.enrollment_states'
deterministic_table = Irontable(schema='inference', table='deterministic_short_range')

BASE_KEY = '/'.join([DAG_CONFIG['dag_name'], '{{ds}}'])
PREDICTION_S3_KEY = BASE_KEY + '/predictions'

with create_dag(**DAG_CONFIG) as dag:


    create_prediction_table = PostgresOperator(
        task_id="create_prediction_table",
        autocommit=True,
        sql='cohort_start_short_range.sql',
        params=table.to_dict()
    )

    create_staging_table = PostgresOperator(
        task_id="create_staging_table",
        sql="create_staging_tables.sql",
        autocommit=True,
        params=dict(
            tables=[table.table_in_env],
            **table.to_dict()
            ),
        )

    create_enrollments_table = PostgresOperator(
        task_id="create_enrollments_table",
        autocommit=True,
        sql=enrollments_query,
        params=enrollments_table.to_dict()
    )

    model_prediction = PythonOperator(
        task_id="generate_prediction",
        python_callable=generate_predictions,
        op_args=[
            enrollments_table,
            PREDICTION_S3_KEY,
            S3_BUCKET,
            MODEL_NAME,
            "{{ data_interval_end | ds }}",
            ]
    )

    staging_op = S3ToRedshiftStagingTransfer(
        task_id=f"s3_to_staging",
        retries=3,
        s3_key=PREDICTION_S3_KEY.split('/')[0],
        schema=table.schema_in_env,
        table=table.table_in_env,
        parameters=dict(
            entity_name=PREDICTION_S3_KEY.split('/')[-1],
            ),
        s3_bucket=S3_BUCKET,
        copy_options=[
            "csv",
            "IGNOREHEADER 1",
            "timeformat 'YYYY-MM-DD HH:MI:SS'",
            "FILLRECORD",
            ],
    )

    load_prod = PostgresOperator(
        task_id="staging_to_prod",
        sql="load_staged_data.sql",
        autocommit=True,
        params=dict(
            schema=table.schema_in_env,
            table=table.table_in_env,
            pk=table.primary_key,
            column_list=[
                "created_at",
                "cohort_start_date",
                "days_till_start",
                "static_prediction",
                "auto_train_prediction",
                ]
            ),
        )

    if Irondata.is_production():
        main_reporting_sensor = ExternalTaskSensor(
            task_id="main_reporting_sensor",
            external_dag_id='main_reporting',
            external_task_id='students',
            allowed_states=['success'],
            failed_states=['failed', 'skipped'],
            execution_delta=timedelta(hours=-0.5),
            poke_interval=15*60,  # Poke every 15 min
            timeout=12*60*60  # Timeout after 12 hours
        )
    else:
        main_reporting_sensor = DummyOperator(
            task_id="main_reporting_sensor",
        )

    deterministic_short_range = PostgresOperator(
        task_id="deterministic_short_range",
        autocommit=True,
        sql='deterministic_short_range.sql',
        params=deterministic_table.to_dict()
    )


    create_prediction_table >> create_staging_table >> staging_op >> load_prod
    create_enrollments_table >> model_prediction
    model_prediction >> staging_op
    main_reporting_sensor >> deterministic_short_range
    main_reporting_sensor >> create_enrollments_table
