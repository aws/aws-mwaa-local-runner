from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def list_files(bucket_name: str):
    print('WOOT')
    hook = S3Hook('s3_conn')
    result = hook.list_keys(bucket_name)
    print('READ BUCKET PRINTING RESULTS')
    print(result)

with DAG(
    dag_id='s3_list_files_test',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:
    # Upload the file
    task_list_files = PythonOperator(
        task_id='list_files',
        python_callable=list_files,
        op_kwargs={
            'bucket_name': 'vp-eng-test-data'
        }
    )