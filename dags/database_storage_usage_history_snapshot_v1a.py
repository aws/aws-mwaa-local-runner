# v1a (2022/4/22):
# adapted from datascience-airflow-dags/dags/query_history_snapshot.py


from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import pendulum


CREATE_SNAPSHOT = """
    create table if not exists datascience.account_usage_snapshot.test_database_storage_usage_history_snapshot (
        usage_date date,
        database_id number,
        database_name varchar,
        deleted timestamp_ltz(6),
        average_database_bytes number,
        average_failsafe_bytes number
    )
"""

# first delete from the table so that the task is idempotent
POPULATE_SNAPSHOT = """
    delete from datascience.account_usage_snapshot.test_database_storage_usage_history_snapshot
    where usage_date >= to_date('{{ ds }}')
    and usage_date < dateadd('DAY', 1, to_date('{{ ds }}'));
    insert into datascience.account_usage_snapshot.test_database_storage_usage_history_snapshot (
        usage_date,
        database_id,
        database_name,
        deleted,
        average_database_bytes,
        average_failsafe_bytes
    )
    select
        usage_date,
        database_id,
        database_name,
        deleted,
        average_database_bytes,
        average_failsafe_bytes
    from datascience.account_usage.database_storage_usage_history
    where usage_date >= to_date('{{ ds }}')
    and usage_date < dateadd('DAY', 1, to_date('{{ ds }}'));
"""

SNOWFLAKE_WAREHOUSE = "DEV_TEST"
SNOWFLAKE_DATABASE = "DATASCIENCE"
SNOWFLAKE_SCHEMA = "ACCOUNT_USAGE_SNAPSHOT"
SNOWFLAKE_ROLE = "INTERNAL_PORTAL"

# this line lets us express our schedule_interval in PST instead of UTC for readability
local_tz = pendulum.timezone("America/Los_Angeles")

with DAG(
    "database_storage_usage_history_snapshot",
    # at 3 am every day (in local_tz time zone above)
    schedule_interval="0 3 * * *",
    default_args={
        "owner": "Jason Lin",
        "snowflake_conn_id": "snowflake_lacework",
        "start_date": datetime(2022, 3, 1, tzinfo=local_tz),
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
        "role": SNOWFLAKE_ROLE,
        "email": ["jason.lin@lacework.net"],
        "email_on_failure": True,
    },
    # tags=["account_usage", "snowbank"],
    catchup=False,
) as dag:

    create_snapshot = SnowflakeOperator(
        task_id="create_snapshot", sql=CREATE_SNAPSHOT
    )

    populate_snapshot = SnowflakeOperator(
        task_id="populate_snapshot", sql=POPULATE_SNAPSHOT
    )

create_snapshot >> populate_snapshot
