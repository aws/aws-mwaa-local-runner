# DAG file exhibiting dynamic generated DAGs
# https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html
# Modified for clarity and flexibility

from datetime import datetime
from airflow.decorators import dag, task

dags_config = {
    'example_dag_1': {
        'owner': 'John',
        'retries': 1,
        'email': 'john@example.com',
        'schedule_interval': '0 12 * * *',
        'tags': ['tag_1', 'tag_2'],
        'message': 'DAG 1 - Hello world.'
    },
    'example_dag_2': {
        'owner': 'Bob',
        'retries': 2,
        'email': 'bob@example.com',
        'schedule_interval': '@daily',
        'tags': ['tag_3', 'tag_4'],
        'message': 'DAG 2 - Hello world.'
    },
}

for dag_name, dag_config in dags_config.items():
    default_args = {
        'owner': dag_config.get('owner'),
        'retries': dag_config.get('retries'),
        'email': dag_config.get('email'),
    }

    @dag(
        dag_id=f'{dag_name}_dynamic',
        default_args=default_args,
        schedule_interval=dag_config.get('schedule_interval'),
        start_date=datetime(2022, 2, 1),
        tags=dag_config.get('tags')
    )
    def dynamic_generated_dag():
        @task
        def print_message(message):
            print(message)

        print_message(dag_config.get('message'))

    dynamic_generated_dag()
