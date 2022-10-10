from http import client
import json
import boto3
from pprint import pprint
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

CLUSTER_NAME="ohm-airflow-cluster" #Replace value for CLUSTER_NAME with your information.
CONTAINER_NAME="ohm-airflow-simple" #Replace value for CONTAINER_NAME with your information.
LAUNCH_TYPE="FARGATE"
LOG_GROUP="ohm-airflow-fargate"

# Should not include secrets
PASS_VARIABLES=['s3_bucket_name']

with DAG(
    dag_id = "ecs_fargate_dag",
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1)
) as dag:
    client = boto3.client('ecs')
    services = client.list_services(cluster=CLUSTER_NAME, launchType=LAUNCH_TYPE)
    service = client.describe_services(cluster=CLUSTER_NAME, services=services['serviceArns'])

    def get_operator(name, overrides):
        return ECSOperator(
            task_id = f"ecs_operator_task_{name}",
            dag=dag,
            cluster=CLUSTER_NAME,
            task_definition=service['services'][0]['taskDefinition'],
            launch_type=LAUNCH_TYPE,
            overrides={
                "containerOverrides": [
                    overrides
                ]
            },
            network_configuration=service['services'][0]['networkConfiguration'],
            awslogs_group=LOG_GROUP,
            awslogs_stream_prefix=f"ecs/{CONTAINER_NAME}",
        )

    ecs_operator_task = get_operator("first", {
        "name": CONTAINER_NAME,
        "command": ["python", "main.py"],
        'environment': [
            {
                'name': 'execution_date',
                'value': "{{ execution_date }}"
            },
        ],
    })

    ecs_operator_task2 = get_operator("second", {
        "name": CONTAINER_NAME,
        "command": ["python", "main.py"],
        'environment': [
            {
                'name': 'execution_date',
                'value': "{{ execution_date }}"
            },
            {
                'name': 'second_task',
                'value': "true"
            },
        ],
    })

    def print_context():
        print("PRINT_CONTEXT")
        return 'testing_result.txt'

    on_worker_task = PythonOperator(
        task_id='runs_on_worker',
        python_callable=print_context,
        dag=dag
    )

    def print_context2(file_path, task, task2, test):
        print("PRINT_CONTEXT 2", file_path)
        print("TESTING_OUT", task)
        print("TESTING_OUT2", task2)
        print("TESTING_OUT2.5", type(task2))
        print("TESTING_OUT3", test)
        if test:
            print("CONTAINER_RESULT", json.loads(test))
        # print("TESTING_CONTAINER", task2.xcom_pull(task_ids='ecs_operator_task_second'))
        return 'TEST'

    on_worker_task2 = PythonOperator(
        task_id='runs_on_worker2',
        python_callable=print_context2,
        op_kwargs={'file_path': "{{ ti.xcom_pull(task_ids='runs_on_worker') }}", 'task': "{{ task }}", 'task2': "{{ ti }}", 'test': "{{ ti.xcom_pull(task_ids='ecs_operator_task_second') }}"},
        dag=dag
    )

    ecs_operator_task >> ecs_operator_task2 >> on_worker_task >> on_worker_task2

