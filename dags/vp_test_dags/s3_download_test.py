import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def download_from_s3(key: str, bucket_name: str) -> str:
    hook = S3Hook('s3_conn')
    file_name = hook.download_file(key=key, bucket_name=bucket_name)
    return file_name

def upload_to_s3(ti, key: str, bucket_name: str) -> None:
  hook = S3Hook('s3_conn')
  downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])

  print(f'DOWNLOADED FILE NAME {downloaded_file_name[0]}')
  # downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
  # print(f'DOWNLOADED FILE PATH {downloaded_file_path}')
  # os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")
  hook.load_file(downloaded_file_name[0], key, bucket_name, replace=True, encrypt=False, gzip=False, acl_policy=None)
      

with DAG(
    dag_id='s3_download_test',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

  task_download_from_s3 = PythonOperator(
    task_id='download_from_s3',
    python_callable=download_from_s3,
    op_kwargs={
        'key': 'test.csv',
        'bucket_name': 'vp-eng-test-data'
    }
  )

  task_upload_to_s3 = PythonOperator(
    task_id='upload_copy_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={
        'key': 'copy_test.csv',
        'bucket_name': 'vp-eng-test-data'
    }
  )

  task_download_from_s3 >> task_upload_to_s3
