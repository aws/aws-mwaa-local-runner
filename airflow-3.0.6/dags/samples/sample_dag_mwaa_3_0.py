"""
Sample DAG for MWAA Airflow 3.0 - Clean Test DAG

This is a simple, clean DAG to test MWAA Airflow 3.0 setup.
It demonstrates basic operators and follows Airflow 3.0 syntax.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create the DAG
dag = DAG(
    'sample_dag_mwaa_3_0',
    default_args=default_args,
    description='Sample DAG for MWAA Airflow 3.0 testing',
    schedule=timedelta(days=1),  # Airflow 3.0 uses 'schedule' instead of 'schedule_interval'
    catchup=False,
    tags=['sample', 'mwaa', 'test'],
)

# Task 1: Bash task that prints a message
task_print_info = BashOperator(
    task_id='print_info',
    bash_command='echo "MWAA Airflow 3.0 Sample DAG - Task 1" && echo "Execution time: $(date)" && echo "Task completed successfully!"',
    dag=dag,
)

# Task 2: Python task that processes data
def process_data():
    """Simple Python function that processes data"""
    print("=" * 60)
    print("MWAA Airflow 3.0 Sample DAG - Task 2")
    print(f"Current timestamp: {datetime.now()}")
    print("Processing data...")
    
    # Simulate some processing
    data = [1, 2, 3, 4, 5]
    result = sum(data)
    
    print(f"Data processed. Result: {result}")
    print("Task completed successfully!")
    print("=" * 60)
    return result

task_process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

# Task 3: Final summary task
task_summary = BashOperator(
    task_id='summary',
    bash_command='echo "All tasks completed successfully!" && echo "DAG execution finished at $(date)"',
    dag=dag,
)

# Define task dependencies
# Task 1 runs first, then Task 2, then Task 3
task_print_info >> task_process_data >> task_summary

