from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import datetime as dt
from pprint import pprint


default_args = {
    'owner': 'Airflow Demo',
    'start_date': dt.datetime(2021, 4, 28),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG(
    'demo_dag', 
    default_args=default_args, 
    catchup=False, 
    schedule_interval='*/1 * * * *'
)

def print_hello_world(*args, **kwargs):
    pprint(args)
    pprint(kwargs)
    return 'Returning hello world'

python_hello_world = PythonOperator(
    task_id='print_hello_world_python',
    provide_context=True,
    python_callable=print_hello_world,
    op_args=['Testing op_args'],
    op_kwargs={'test_key1': 'test value1', 'test_key2': 'test value2', 'test_key3': 'test value3'},
    dag=dag
)

bash_sleep = BashOperator(
    task_id='sleep_bash',
    bash_command='sleep 2',
    dag=dag
)

bash_hello_world = BashOperator(
    task_id='print_hello_world_bash',
    bash_command='echo Hello from bash',
    dag=dag
)

python_hello_world >> bash_sleep >> bash_hello_world