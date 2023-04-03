"""ETL Data Transfer DAG"""

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.weight_rule import WeightRule

from custom.operators.store_ds_macro_operator import StoreDsMacroOperator
from airflow.models import Variable

from custom.operators.postgres_partitions_to_s3 import PostgresPartitionsToS3Operator
from custom.operators.postgres_data_to_s3 import PostgresToS3WithSchemaOperator
from airflow.operators.dummy_operator import DummyOperator


S3_BUCKET = Variable.get('etl_s3_bucket', deserialize_json=False)
POSTGRES_DB = Variable.get('etl_postgres_db', deserialize_json=False)

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'on_failure_callback': None,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'weight_rule': WeightRule.ABSOLUTE,
    'priority_weight': 3
}

# add precommit checks for pylint and sqlfluff

with DAG(
    'etl_transfer',
    start_date=days_ago(1),
    max_active_runs=1,
    schedule_interval='10 0 * * *',
    default_args=default_args,
    catchup=False,
    tags=['etl']
    ) as dag:
    
    # Store the 'ds' macro to an Airflow variable
    store_ds_macro = StoreDsMacroOperator(
        task_id='store_ds_macro',
        variable_name='etl_data_transfer_ds',
        dag=dag
    )

    # retrieve etl_data_transfer_ds variable and 
    etl_data_transfer_ds = Variable.get('etl_data_transfer_ds', deserialize_json=False, default_var=None)

    # Transfer the table partitions to S3
    postgres_partitions_to_s3 = PostgresPartitionsToS3Operator(
        task_id='postgres_partitions_to_s3',
        postgres_conn_id='postgres_default',
        postgres_db=POSTGRES_DB,
        s3_conn_id='aws_default',
        s3_bucket=S3_BUCKET,
        s3_prefix='table_list',
        schema='public',
        transfer_ds = etl_data_transfer_ds,
        table_list_variable_name='peanut_analytics_tables',
        dag=dag
    )

    start_table_processing = DummyOperator(
        task_id='start_table_processing',
        dag=dag
    )

    peanut_analytics_tables = Variable.get('peanut_analytics_tables', deserialize_json=True, default_var=None)

    for table in peanut_analytics_tables:

        postgres_data_to_s3 = PostgresToS3WithSchemaOperator(
            task_id = f"{table}_to_s3",
            s3_bucket = S3_BUCKET,
            s3_partition_dir = 'table_list',
            s3_export_dir = 'export',
            db=POSTGRES_DB,
            db_schema = 'public',
            table_name = table,
            transfer_ds = etl_data_transfer_ds,
            dag=dag

        )
        start_table_processing >> postgres_data_to_s3


    store_ds_macro >> [
        postgres_partitions_to_s3
    ] >> start_table_processing

# add the original partition name to the output of the dump