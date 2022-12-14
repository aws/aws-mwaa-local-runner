from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
import pendulum
import logging
import time




def log_task(text: str):
    logging.info(text)


def discover_from_cmr_task(ti, text):
    print(ti.dag_run.conf)
    log_task(text)


def discover_from_s3_task(text):
    log_task("I am discovering")
    time.sleep(1)
    log_task("Done discovering")
    log_task(text)


def move_files_to_maap_store_task(text):
    log_task("I am moving files")
    time.sleep(3)
    log_task("Done moving files")
    log_task(text)


def generate_cmr_metadata_task(text):
    log_task(text)


def push_to_cmr_task(text):
    log_task(text)


with DAG(
    dag_id="example_etl_flow",
    start_date=pendulum.today("UTC").add(days=-1),
    schedule_interval=None,
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    discover_from_cmr = PythonOperator(
        task_id="discover_from_cmr",
        python_callable=discover_from_cmr_task,
        op_kwargs={"text": "Discover from CMR"},
        dag=dag,
    )
    discover_from_s3 = PythonOperator(
        task_id="discover_from_s3",
        python_callable=discover_from_s3_task,
        op_kwargs={"text": "Discover from S3"},
        dag=dag,
    )

    move_files_to_maap_store = PythonOperator(
        task_id="move_files_to_maap_store",
        python_callable=move_files_to_maap_store_task,
        op_kwargs={"text": "Moving Files to MAAP store"},
        dag=dag,
    )

    generate_cmr_metadata = PythonOperator(
        task_id="generate_cmr_metadata",
        python_callable=generate_cmr_metadata_task,
        op_kwargs={"text": "Generate CMR metadata"},
        dag=dag,
    )

    push_to_cmr = PythonOperator(
        task_id="push_to_cmr",
        python_callable=push_to_cmr_task,
        op_kwargs={"text": "Push to CMR"},
        dag=dag,
    )
    docker_step = DockerOperator(
        task_id='docker_command',
        container_name="docker_command_task",
        image='python:3.7',
        api_version='auto',
        auto_remove=True,
        command="python --version",
        environment={"Name": "Abdelhak"},
        docker_url="tcp://docker-in-docker:2375",
        mount_tmp_dir=False,
        network_mode="bridge"
    )
    end = EmptyOperator(task_id="end", dag=dag)
    start >> docker_step >> discover_from_cmr
    start >> discover_from_s3 >> move_files_to_maap_store
    [discover_from_cmr, move_files_to_maap_store] >> generate_cmr_metadata >> push_to_cmr >> end