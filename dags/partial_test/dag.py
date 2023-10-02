"""
This file demonstrates the airflow code for 
dynamic task generation using airflow traditional
operator syntax.

The goal of this airflow job is
to schedule a manipulation operator
for datasets in s3, for whatever datasets are found.
"""

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException


test_data = {
    'table_1': {'hour_1': [], 'hour_2': ['key_1', 'key_2']},
    'table_2': {'hour_1': ['key_1', 'key_2'], 'hour_2': ['key_1', 'key_2', 'key_3', 'key_4']},
    'table_3': {'hour_1': [], 'hour_2': []},
    }


def generate_prefixes(table):
    prefix = [{table: ['hour_1', 'hour_2']}] # Airflow removes the outer list when it passes this data object to the next function
    return prefix

def check_prefixes(prefix_map):
    table, prefixes = list(prefix_map.items())[0]  # prefix_map = {table: ['hour_1', 'hour_2']}
    return [[(table, prefix) for prefix in prefixes if test_data[table][prefix]]]

def isolate_key(cleared_prefix):
    # cleared_prefix = [(table, prefix), (table, prefix)...]
    keys = []
    for table, prefix in cleared_prefix:
        for key in test_data[table][prefix]:
            keys.append([table, prefix, key])     
    return [keys]  # The outer brackets are stripped by airflow


def print_keys(found_keys):
    for table, prefix, key in found_keys:
        print(f'DATA FOUND: {table}/{prefix}/{key}')

# List of tables to generate first group of operators
# Inputs must be structured as [[input]]
TABLES = [[x] for x in test_data.keys()]


with DAG(
    dag_id="partial_test",
    start_date=datetime(2023, 9, 22)
) as dag:

    prefixes = PythonOperator.partial(
        task_id="generate_prefixes",
        python_callable=generate_prefixes,
        ).expand(op_args=TABLES)

    prefix_checks = PythonOperator.partial(
        task_id="check_prefixes",
        python_callable=check_prefixes).expand(op_args=prefixes.output)

    isolate_keys = PythonOperator.partial(
        task_id="isolate_key",
        python_callable=isolate_key).expand(op_args=prefix_checks.output)
    
    # A function that can be applied to the output of an operator
    def filter_out_empty_prefix(entry):
        if not entry[0]: # The outer brackets aren't stripped until it is passed to the next operator as input
            raise AirflowSkipException('Skipping empty prefix')
        else:
            return entry
    
    filtered_keys = isolate_keys.output.map(filter_out_empty_prefix)
    
    manipulate_key = PythonOperator.partial(
        task_id="manipulate_key",
        python_callable=print_keys
    ).expand(op_args=filtered_keys)

prefixes >> prefix_checks >> isolate_keys >> manipulate_key   
