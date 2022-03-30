# Basic DAG which provides examples(and tests connectivity) to prod(lacework) and lwsecops.
# This dag does not have a schedule defined, it needs to be manually triggered.
# This dag also includes examples of accessing Snowflake via the SnowflakeOperator and hooks(python).

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain

from datetime import datetime


def sf_hook_test(sf_connection, sf_warehouse, sf_database, sf_schema, sf_role):
    """Basic function demonstrating querying snowflake from a hook and printing the result."""

    print(f"Environment: {sf_connection}")

    qry = """SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_USER();"""
    hook_test = SnowflakeHook(
        snowflake_conn_id=sf_connection,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
        role=sf_role,
    )

    # Execute the query, print the results
    query_result = hook_test.get_records(qry)
    print(query_result)

    # Repeat the above, but using a pandas dataframe
    df = hook_test.get_pandas_df(qry)
    print(df.to_string())


# This DAG needs to be triggered manually, it will not run on a schedule.
with DAG(
    "connectivity_tests",
    default_args={
        "owner": "Neil",
        "snowflake_conn_id": "snowflake_lacework",
        "start_date": datetime(2022, 2, 8),
        "schedule_interval": None,
    },
    tags=["example"],
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    hook_test_prod = PythonOperator(
        task_id="hook_test_prod",
        python_callable=sf_hook_test,
        op_args=[
            "snowflake_lacework",
            "dev_test",
            "datascience",
            "clone_analysis",
            "internal_portal",
        ],
    )

    hook_test_lwsecops = PythonOperator(
        task_id="hook_test_lwsecops",
        python_callable=sf_hook_test,
        op_args=[
            "snowflake_lwsecops",
            "reporting",
            "datascience",
            "neil_test_schema",
            "reporter",
        ],
    )

    # snowflake_conn_id is not defined, we're getting it from default_args
    snowflake_operator_test_prod = SnowflakeOperator(
        task_id="snowflake_operator_test_prod", sql="SELECT 1"
    )

    # We need to specify snowflake_conn_id here beacuse we are overriding the default value.
    snowflake_operator_test_lwsecops = SnowflakeOperator(
        task_id="snowflake_operator_test_lwsecops",
        snowflake_conn_id="snowflake_lwsecops",
        warehouse="reporting",
        database="datascience",
        schema="neil_test_schema",
        role="reporter",
        sql="SELECT 1",
    )

    end = DummyOperator(task_id="end")

# Using chain() produces the equivalent of:
# start >> snowflake_operator_test_prod >> hook_test_prod >> end
# start >> snowflake_operator_test_lwsecops >> hook_test_lwsecops >> end

chain(
    start,
    [snowflake_operator_test_prod, snowflake_operator_test_lwsecops],
    [hook_test_prod, hook_test_lwsecops],
    end,
)
