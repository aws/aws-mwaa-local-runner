from shared.dag_factory import create_dag
from shared.irondata import Irondata
from shared.irontable import Irontable

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

import pendulum
from application_forecast import model

DAG_CONFIG = dict(
    dag_name="application_forecast",
    schedule='0 2 * * *',
    start_date=pendulum.datetime(2023, 3, 1, tz="America/New_York"),
    catchup=True,
    max_active_runs=1,
    description="Daily forecast of applications",
    tags=["model"]
    )

daily_applications = Irontable(schema="fis", table="daily_applications")
application_predictions = Irontable(schema="inference", table="daily_application_predictions", primary_key="id")

with create_dag(**DAG_CONFIG) as dag:

    reset_table = PostgresOperator(
        task_id="reset_daily_applications",
        sql="reset_daily_applications.sql",
        params={
            "schema": daily_applications.schema_in_env,
            "table": daily_applications.table_in_env
        },
        autocommit=True)
    
    predictions_to_s3 = PythonOperator(
        task_id="predictions_to_s3",
        python_callable=model.run,
        op_kwargs={
            "execution_date": "{{ ds }}",
            "bucket": Irondata.s3_warehouse_bucket(),
            "input_table": daily_applications,
            "output_table": application_predictions
        },
        provide_context=True)
    
    create_table_op = PostgresOperator(
        dag=dag,
        task_id="create_application_predictions",
        sql="create_predictions_table.sql",
        params={
            "schema": application_predictions.schema_in_env,
            "table": application_predictions.table_in_env
        },
        autocommit=True)
    
    s3_to_redshift = S3ToRedshiftOperator(
        dag=dag,
        method="UPSERT",
        task_id="s3_to_redshift",
        schema=application_predictions.schema_in_env,
        table=application_predictions.table_in_env,
        s3_bucket=Irondata.s3_warehouse_bucket(),
        s3_key=f"{application_predictions.schema_in_env}/{{{{ ds }}}}/{application_predictions.table_in_env}",
        copy_options=["CSV", "BLANKSASNULL", "TIMEFORMAT 'auto'"]
    )
        
    reset_table >> predictions_to_s3 >> create_table_op >> s3_to_redshift