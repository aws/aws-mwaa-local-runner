from airflow import DAG
from datetime import datetime, timedelta
from operators.my_airflow_operator import MyOperator
from sensors.my_airflow_sensor import MySensor
from operators.hello_operator import HelloOperator

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime(2018, 1, 1),
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
}


with DAG('customdag',
		 max_active_runs=3,
		 schedule_interval='@once',
		 default_args=default_args) as dag:

	sens = MySensor(
		task_id='taskA'
	)

	op = MyOperator(
		task_id='taskB',
		my_field='some text'
	)

	hello_task = HelloOperator(task_id='sample-task', name='foo_bar')



	sens >> op >> hello_task
