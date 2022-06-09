from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from utils.dag_factory import create_dag, add_python_task


def download_from_s3(key: str, bucket_name: str) -> str:
    hook = S3Hook("s3_conn")
    file_name = hook.download_file(key=key, bucket_name=bucket_name)
    return file_name


def upload_to_s3(ti, key: str, bucket_name: str) -> None:
    hook = S3Hook("s3_conn")
    downloaded_file_name = ti.xcom_pull(task_ids=["download_from_s3"])

    print(f"DOWNLOADED FILE NAME {downloaded_file_name[0]}")
    hook.load_file(
        downloaded_file_name[0],
        key,
        bucket_name,
        replace=True,
        encrypt=False,
        gzip=False,
        acl_policy=None,
    )


dag = create_dag(name="example_s3_download_upload", schedule="@daily")

task_download_from_s3 = add_python_task(
    name="download_from_s3",
    function=download_from_s3,
    kwargs={"key": "test.csv", "bucket_name": "vp-eng-test-data"},
    dag=dag,
)

task_upload_to_s3 = add_python_task(
    name="upload_copy_to_s3",
    function=upload_to_s3,
    kwargs={"key": "copy_test.csv", "bucket_name": "vp-eng-test-data"},
    dag=dag,
)

task_download_from_s3 >> task_upload_to_s3
