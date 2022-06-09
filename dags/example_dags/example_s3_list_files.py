from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.dag_factory import create_dag, add_python_task


def list_files(bucket_name):
    hook = S3Hook("s3_conn")
    result = hook.list_keys(bucket_name)
    print(f"The Files in S3 Bucket: {bucket_name}")
    print(result)


dag = create_dag(name="example_s3_list_files", schedule="@daily")

list_files = add_python_task(
    name="list_s3_files",
    function=list_files,
    kwargs={"bucket_name": "vp-eng-test-data"},
    dag=dag,
)
