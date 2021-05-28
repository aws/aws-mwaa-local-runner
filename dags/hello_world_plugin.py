from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import HelloWorldOperator
import datetime


print(HelloWorldOperator)

default_args = {
    'owner': 'Big Data Infra',
    'start_date': datetime.datetime(2021, 5, 17),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'testing_hello_world_plugin',
    default_args=default_args,
    catchup=False,
    schedule_interval='30 * * * *')

dummy_task = DummyOperator(task_id='dummy_task',
                           dag=dag)

operator_task = HelloWorldOperator(my_operator_param='Testing.... 1, 2, 3 Testing....',
                                   task_id='testing_hello_world_operator_task',
                                   dag=dag)

dummy_task >> operator_task
