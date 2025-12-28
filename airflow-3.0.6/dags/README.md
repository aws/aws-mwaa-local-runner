# DAGs Folder

Place your Airflow DAG files in this directory.

## Sample DAGs

Sample DAGs are available in the `samples/` subdirectory:
- `sample_dag_mwaa_3_0.py` - Basic DAG example
- `sample_taskflow_api_dag.py` - TaskFlow API example
- `sample_airflow_3_syntax_dag.py` - Airflow 3.0 syntax examples

## Airflow 3.0 Syntax

Remember to use Airflow 3.0 syntax:
- Use `schedule` instead of `schedule_interval`
- Use `logical_date` instead of `execution_date`
- Use `data_interval_start` and `data_interval_end` for data intervals

## Example

```python
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='my_dag',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 0 * * *",  # Airflow 3.0 syntax
    catchup=False,
) as dag:
    task = BashOperator(
        task_id='my_task',
        bash_command='echo "Hello Airflow 3.0"',
    )
```

