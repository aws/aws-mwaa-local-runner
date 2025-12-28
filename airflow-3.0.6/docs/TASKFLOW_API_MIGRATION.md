# TaskFlow API Migration: 2.10.3 to 3.0.6

## Compatibility

**Yes, TaskFlow API DAGs from 2.10.3 will work in 3.0.6**, but there are some recommended updates.

## What Works Without Changes

Most TaskFlow API code from 2.10.3 will work in 3.0.6 without modification:

```python
# This code works in both 2.10.3 and 3.0.6
from airflow.decorators import dag, task

@dag(
    dag_id="my_dag",
    schedule_interval=timedelta(days=1),  # Works, but deprecated
    start_date=datetime(2024, 1, 1),
)
def my_dag():
    @task
    def my_task():
        return "Hello"
    
    my_task()
```

## Recommended Updates for 3.0.6

### 1. Use New Stable Interface (Recommended)

Airflow 3.0 introduces a stable DAG authoring interface under `airflow.sdk`:

```python
# Old (2.10.3) - Still works, but deprecated
from airflow.decorators import dag, task

# New (3.0.6) - Recommended
from airflow.sdk import dag, task
```

### 2. Update Schedule Syntax

```python
# Old (2.10.3)
@dag(
    dag_id="my_dag",
    schedule_interval=timedelta(days=1),  # Deprecated
    ...
)

# New (3.0.6)
@dag(
    dag_id="my_dag",
    schedule=timedelta(days=1),  # Use 'schedule' instead
    ...
)
```

### 3. Update Date References

```python
# Old (2.10.3)
@task
def my_task(**context):
    execution_date = context['execution_date']
    # ...

# New (3.0.6)
@task
def my_task(**context):
    logical_date = context['logical_date']  # or data_interval_start
    # ...
```

## Complete Example

### 2.10.3 Version

```python
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="example_taskflow",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def example_dag():
    @task
    def extract(**context):
        execution_date = context['execution_date']
        return {"date": str(execution_date), "data": [1, 2, 3]}
    
    @task
    def transform(data):
        return {"sum": sum(data["data"])}
    
    @task
    def load(result):
        print(f"Result: {result}")
    
    extract() >> transform >> load

example_dag()
```

### 3.0.6 Version (Updated)

```python
from datetime import datetime, timedelta
from airflow.sdk import dag, task  # Updated import

@dag(
    dag_id="example_taskflow",
    schedule=timedelta(days=1),  # Updated: 'schedule' instead of 'schedule_interval'
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def example_dag():
    @task
    def extract(**context):
        logical_date = context['logical_date']  # Updated: 'logical_date' instead of 'execution_date'
        return {"date": str(logical_date), "data": [1, 2, 3]}
    
    @task
    def transform(data):
        return {"sum": sum(data["data"])}
    
    @task
    def load(result):
        print(f"Result: {result}")
    
    extract() >> transform >> load

example_dag()
```

## Backward Compatibility

For maximum compatibility, you can use a try/except pattern:

```python
try:
    from airflow.sdk import dag, task  # Airflow 3.0+
except ImportError:
    from airflow.decorators import dag, task  # Airflow 2.x
```

## Migration Checklist

When migrating TaskFlow API DAGs from 2.10.3 to 3.0.6:

- [ ] Update imports: `airflow.decorators` → `airflow.sdk` (recommended)
- [ ] Update `schedule_interval` → `schedule` in `@dag` decorator
- [ ] Update `execution_date` → `logical_date` in task functions
- [ ] Test DAG parsing in 3.0.6 environment
- [ ] Verify task dependencies work correctly
- [ ] Check XCom behavior (should be unchanged)

## Key Differences Summary

| Aspect | 2.10.3 | 3.0.6 |
|--------|--------|-------|
| **Imports** | `from airflow.decorators import dag, task` | `from airflow.sdk import dag, task` (recommended) |
| **Schedule** | `schedule_interval=...` | `schedule=...` |
| **Date Context** | `context['execution_date']` | `context['logical_date']` or `context['data_interval_start']` |
| **Backward Compat** | N/A | Old imports still work |

## Sample DAG

See `dags/samples/sample_taskflow_api_dag.py` for a complete working example.

