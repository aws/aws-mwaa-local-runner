"""
Sample DAG demonstrating TaskFlow API in Airflow 3.0.6.

This DAG shows:
- TaskFlow API usage (recommended approach)
- Airflow 3.0 syntax (schedule, logical_date)
- Both old and new import styles (for compatibility)
"""

from datetime import datetime, timedelta

# Airflow 3.0.6: Recommended to use airflow.sdk (stable interface)
# Old imports (airflow.decorators) still work for backward compatibility
try:
    from airflow.sdk import dag, task  # Airflow 3.0+ (recommended)
except ImportError:
    from airflow.decorators import dag, task  # Airflow 2.x (backward compatible)


@dag(
    dag_id="sample_taskflow_api_dag",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(days=1),  # Airflow 3.0: use 'schedule' instead of 'schedule_interval'
    catchup=False,
    tags=["taskflow", "airflow-3.0"],
)
def taskflow_example():
    """
    TaskFlow API example DAG.
    Uses @task decorator for cleaner, more Pythonic DAG definition.
    """

    @task
    def extract_data():
        """Extract data task."""
        print("Extracting data...")
        data = {"value": 42, "items": [1, 2, 3, 4, 5]}
        print(f"Extracted data: {data}")
        return data

    @task
    def transform_data(data: dict):
        """Transform data task (receives output from extract_data)."""
        print(f"Transforming data: {data}")
        # Process the data
        transformed = {
            "value": data["value"] * 2,
            "sum": sum(data["items"]),
            "count": len(data["items"]),
        }
        print(f"Transformed data: {transformed}")
        return transformed

    @task
    def load_data(transformed_data: dict):
        """Load data task (receives output from transform_data)."""
        print(f"Loading data: {transformed_data}")
        print("Data loaded successfully!")
        return transformed_data

    # TaskFlow API automatically handles dependencies via function parameters
    # extract_data() runs first, then transform_data() receives its output,
    # then load_data() receives transform_data() output
    extracted = extract_data()
    transformed = transform_data(extracted)
    load_data(transformed)


# Create the DAG instance
taskflow_dag = taskflow_example()

