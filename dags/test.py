import requests
import json
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


def get_rates():
    """
    Open rates API and get exchange rates for the day. Then, dump the data to the /data directory with the filename rates_<extract-date>,json
    """

    result = requests.get("https://v6.exchangerate-api.com/v6/9227005e1c0cb3cc0a373690/latest/USD")

    # If the API call is successful, get the JSON and dump it into a JSON file with the extract date.
    if result.status_code == 200:

        # Get the json data
        json_data = result.json()
        file_name = 'rates_' + str(datetime.now().date()) + '.json'
        full_name = os.path.join(os.path.dirname(__file__), 'data', file_name)

        with open(full_name, 'w') as output_file:
            json.dump(json_data, output_file)
    else:
        print("API call failed.")


def create_inserts():
    """
    Processes the JSON data, checks the types and inserts it into the Postgres database.
    """

    file_name_json = 'rates_' + str(datetime.now().date()) + '.json'
    file_name_sql = 'insert_statements.sql'

    full_name_json = os.path.join(os.path.dirname(__file__), 'data', file_name_json)
    full_name_sql = os.path.join(os.path.dirname(__file__), 'data', file_name_sql)

    # open JSON file to read
    with open(full_name_json, 'r') as input_file:
        doc = json.load(input_file)

    rate_date = str(datetime.strptime(doc['time_last_update_utc'], '%a, %d %b %Y %H:%M:%S +0000').date())
    extract_time = str(datetime.now())

    with open(full_name_sql, 'w') as output_file:

        for c, r in doc['conversion_rates'].items():

            code = c
            ratio = r

            insert_cmd = f"INSERT INTO rates (code, ratio, date, extract_time) VALUES ('{code}', {ratio}, '{rate_date}', '{extract_time}');"

            output_file.write(insert_cmd)

    output_file.close()


# Define default DAG arguments
default_args = {
    'owner': 'Daniela Aquilina',
    'depends_on_past': False,
    'email': ['danielaaquilina84@gmail.com'],
    'email_on_failure': False
}

# Define the DAG, the start date and the frequency.
with DAG(
    dag_id='ratesDAG',
    default_args=default_args,
    start_date=datetime(2022, 7, 1),
    schedule_interval='@daily'
) as dag:

    # Task 1: Get exchange rates from API

    task_get_rates = PythonOperator(
        task_id='get_rates',
        python_callable=get_rates
    )

    # Task 2: Create tables
    task_create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='rates_db',
        sql="""
        create table if not exists rates
    (
        code         varchar(3)     not null,
        ratio        numeric(18, 4) not null,
        date         date           not null,
        extract_time timestamp      not null,
        PRIMARY KEY(code, extract_time)
    );
        
    
        create table if not exists monthly_conversion_rates
        (
            month        int            not null,
            code         varchar(3)     not null,
            avg_ratio    numeric(18, 4) not null,
            last_upload  timestamp      not null,
            PRIMARY KEY(month, code, avg_ratio)
        );
    
        CREATE UNIQUE INDEX IF NOT EXISTS uq_code_month_avgratio_idx on monthly_conversion_rates (month, code);
        """
    )

    # Task 3: Create insert statements to input in database
    task_create_inserts = PythonOperator(
        task_id='create_inserts',
        python_callable=create_inserts
    )

    # Task 4: Load rates in database
    task_load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='rates_db',
        sql="data/insert_statements.sql"
    )

    # Task 5: Update monthly average conversion rates
    task_update_monthly_conversion_rates = PostgresOperator(
        task_id='update_monthly_conversion_rates',
        postgres_conn_id='rates_db',
        sql="""
        INSERT INTO monthly_conversion_rates
            (month, code, avg_ratio, last_upload)
        select date_part('month', date) as rate_month
             , code                     as rate_code
             , avg(ratio)               as rate_ratio
             , now()                    as last_upload
        from rates
        group by rate_month, rate_code
        ON CONFLICT (month, code)
            DO UPDATE
            SET avg_ratio   = excluded.avg_ratio
              , last_upload = excluded.last_upload;

        """
    )


    task_get_rates >> task_create_tables >> task_create_inserts >> task_load_data >> task_update_monthly_conversion_rates