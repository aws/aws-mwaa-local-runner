"""
Sample DAG demonstrating Airflow 3.0.6 new syntax and features.

This DAG shows:
- New schedule syntax (schedule instead of schedule_interval)
- logical_date instead of execution_date
- data_interval_start and data_interval_end
- Modern Python features
- Improved task dependencies
"""

import logging
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

# ============================================================================
# Airflow 3.0.6 Syntax Changes
# ============================================================================

# OLD (Airflow 2.10.1):
# from datetime import datetime, timedelta
# default_args = {'start_date': datetime(2024, 1, 1)}
# dag = DAG(
#     'my_dag',
#     default_args=default_args,
#     schedule_interval=timedelta(days=1),
#     catchup=True,
# )

# NEW (Airflow 3.0.6):
with DAG(
    dag_id="sample_airflow_3_syntax_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),  # Use pendulum
    schedule="0 11 * * *",  # Use 'schedule' instead of 'schedule_interval'
    catchup=False,
    tags=["syntax", "airflow-3.0", "migration"],
) as dag:

    def demonstrate_new_syntax(**context):
        """
        Demonstrate Airflow 3.0.6 new syntax features.
        """
        dag_run = context["dag_run"]

        # OLD: execution_date (deprecated)
        # NEW: logical_date or data_interval_start
        logical_date = dag_run.logical_date
        data_interval_start = dag_run.data_interval_start
        data_interval_end = dag_run.data_interval_end

        logging.info("=== Airflow 3.0.6 Syntax Changes ===")
        logging.info(f"Logical Date: {logical_date}")
        logging.info(f"Data Interval Start: {data_interval_start}")
        logging.info(f"Data Interval End: {data_interval_end}")

        # Key changes:
        # 1. schedule_interval → schedule
        # 2. execution_date → logical_date or data_interval_start
        # 3. Use pendulum for dates
        # 4. provide_context removed (always True)

        return {
            "syntax_changes": {
                "schedule_interval": "→ schedule",
                "execution_date": "→ logical_date or data_interval_start",
                "datetime": "→ pendulum (recommended)",
                "provide_context": "→ removed (always True)",
            },
            "logical_date": str(logical_date),
            "data_interval_start": str(data_interval_start),
            "data_interval_end": str(data_interval_end),
        }

    new_syntax = PythonOperator(
        task_id="demonstrate_new_syntax",
        python_callable=demonstrate_new_syntax,
    )

    def demonstrate_schedule_syntax(**context):
        """
        Show different schedule syntax options in Airflow 3.0.6.
        """
        # Schedule can be:
        # - Cron expression: "0 6 * * *"
        # - Timedelta: timedelta(days=1)
        # - Cron preset: "@daily", "@hourly"
        # - None: for on-demand DAGs

        schedule_examples = {
            "cron": "0 6 * * *",  # Daily at 6 AM
            "timedelta": "timedelta(days=1)",  # Every day
            "preset": "@daily",  # Daily preset
            "none": None,  # On-demand
        }

        logging.info("Schedule syntax examples:")
        for key, value in schedule_examples.items():
            logging.info(f"  {key}: {value}")

        return schedule_examples

    schedule_demo = PythonOperator(
        task_id="demonstrate_schedule_syntax",
        python_callable=demonstrate_schedule_syntax,
    )

    def demonstrate_data_intervals(**context):
        """
        Show data interval usage (replaces execution_date concept).
        """
        dag_run = context["dag_run"]

        # Data intervals better represent the data being processed
        # Example: Daily DAG that processes yesterday's data
        # - data_interval_start: Start of data period
        # - data_interval_end: End of data period
        # - logical_date: When the DAG runs

        data_info = {
            "logical_date": str(dag_run.logical_date),
            "data_interval_start": str(dag_run.data_interval_start),
            "data_interval_end": str(dag_run.data_interval_end),
            "explanation": (
                "Data intervals represent the data being processed, "
                "not when the DAG runs"
            ),
        }

        logging.info(f"Data Interval Info: {data_info}")
        return data_info

    data_intervals = PythonOperator(
        task_id="demonstrate_data_intervals",
        python_callable=demonstrate_data_intervals,
    )

    def demonstrate_modern_python(**context):
        """
        Show modern Python features supported in Airflow 3.0.6.
        """
        # Airflow 3.0.6 supports:
        # - Python 3.9, 3.10, 3.11, 3.12
        # - Better type hints
        # - Modern Python features

        import sys

        python_version = sys.version_info
        modern_features = {
            "python_version": f"{python_version.major}.{python_version.minor}.{python_version.micro}",
            "supported_versions": ["3.9", "3.10", "3.11", "3.12"],
            "features": [
                "Type hints",
                "Dataclasses",
                "f-strings",
                "Context managers",
                "Async/await (in some contexts)",
            ],
        }

        logging.info(f"Python Features: {modern_features}")
        return modern_features

    modern_python = PythonOperator(
        task_id="demonstrate_modern_python",
        python_callable=demonstrate_modern_python,
    )

    # Task dependencies (improved in Airflow 3.0.6)
    new_syntax >> schedule_demo >> data_intervals >> modern_python

