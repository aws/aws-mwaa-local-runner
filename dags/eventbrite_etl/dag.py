from shared.irondata import Irondata
from eventbrite_etl.source import EventbriteSource

from shared.dag_factory import create_dag
from airflow.operators.sql import SQLValueCheckOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from shared.operators.s3_to_redshift_staging_operator import S3ToRedshiftStagingTransfer
from shared.utils import quote_columns


attendee_columns = ["id", "created", "changed", "ticket_class_id", "variant_id", "ticket_class_name", "checked_in", "cancelled", "refunded", "status", "event_id", "order_id", "guestlist_id", "invited_by", "delivery_method", "profile_name", "profile_email", "profile_first_name", "profile_last_name", "profile_prefix", "profile_suffix", "profile_age", "profile_job_title", "profile_company", "profile_website", "profile_blog", "profile_gender", "profile_birth_date", "profile_cell_phone", "affiliate"]  # noqa: E501

event_columns = ["id", "name", "summary", "description", "url", "start", "end", "created", "changed", "published", "status", "currency", "online_event", "venue_address_1", "venue_address_2", "venue_city", "venue_region", "venue_postal_code", "venue_country", "venue_id", "venue_capacity", "venue_name", "venue_latitude", "venue_longitude"]  # noqa: E501

column_lists = {
    "attendees": quote_columns(attendee_columns),
    "events": quote_columns(event_columns)}

dag = create_dag(
    "eventbrite_etl",
    schedule="@daily",
    start_date=datetime(2023, 4, 4),
    catchup=True,
    tags=["API"]
)
eventbrite_oauth_token = Irondata.get_config('EVENTBRITE_OAUTH_ACCESS_TOKEN')
source = EventbriteSource(eventbrite_oauth_token)
schema = "eventbrite"

ops = {}

ops["rest_api_to_s3__completed_events_and_attendees"] = PythonOperator(
    dag=dag,
    task_id='rest_api_to_s3__completed_events_and_attendees',
    python_callable=source.extract_completed_events_and_attendees,
    provide_context=True)

for table in ["events", "attendees"]:
    ops[f"create_table__{table}"] = PostgresOperator(
        dag=dag,
        task_id=f"create_table__{table}",
        params={"schema": Irondata.schema(schema), "table": Irondata.table(schema, table)},
        postgres_conn_id='redshift',
        sql=f"create_{table}.sql")

    ops[f"create_staging_table__{table}"] = \
        PostgresOperator(
            dag=dag,
            task_id=f"create_staging_table__{table}",
            sql="create_staging_tables.sql",
            params={
                "schema": Irondata.schema("eventbrite"),
                "tables": [Irondata.table("eventbrite", table)]},
            postgres_conn_id="redshift",
            autocommit=True)

    ops[f"s3_to_staging__{table}"] = \
        S3ToRedshiftStagingTransfer(
            dag=dag,
            task_id=f"s3_to_staging__{table}",
            redshift_conn_id='redshift',
            schema=Irondata.schema("eventbrite"),
            table=Irondata.table("eventbrite", table),
            parameters={
                "column_list": column_lists[table],
                "entity_name": table},
            s3_bucket=Irondata.s3_warehouse_bucket(),
            s3_key='eventbrite',
            copy_options=["CSV TIMEFORMAT 'auto'"])

    ops[f"staging_load__{table}"] = \
        PostgresOperator(
            dag=dag,
            task_id=f"staging_load__{table}",
            sql="load_staged_data.sql",
            params={
                "schema": Irondata.schema("eventbrite"),
                "table": Irondata.table("eventbrite", table),
                "column_list": column_lists[table],
                "pk": "id"},
            postgres_conn_id="redshift",
            autocommit=True)

    ops["rest_api_to_s3__completed_events_and_attendees"] >> ops[f"create_table__{table}"] >> \
        ops[f"create_staging_table__{table}"] >> ops[f"s3_to_staging__{table}"] >> ops[f"staging_load__{table}"]

ops["rest_api_to_s3__updated_attendees"] = PythonOperator(
    dag=dag,
    task_id='rest_api_to_s3__attendee_updates',
    python_callable=source.extract_updated_attendees,
    provide_context=True)

ops["create_staging_table__updated_attendees"] = \
    PostgresOperator(
        dag=dag,
        task_id=f"create_staging_table__updated_attendees",
        sql="create_staging_tables.sql",
        params={
            "schema": Irondata.schema("eventbrite"),
            "tables": [Irondata.table("eventbrite", "attendees")]},
        postgres_conn_id="redshift",
        autocommit=True)

ops["s3_to_staging__updated_attendees"] = \
    S3ToRedshiftStagingTransfer(
        dag=dag,
        task_id=f"s3_to_staging__updated_attendees",
        redshift_conn_id="redshift",
        schema=Irondata.schema("eventbrite"),
        table=Irondata.table("eventbrite", "attendees"),
        parameters={
            "column_list": column_lists["attendees"],
            "entity_name": "updated_attendees"},
        s3_bucket=Irondata.s3_warehouse_bucket(),
        s3_key="eventbrite",
        copy_options=["CSV TIMEFORMAT 'auto'"])

ops["staging_load__updated_attendees"] = \
    PostgresOperator(
        dag=dag,
        task_id=f"staging_load__updated_attendees",
        sql="load_staged_data.sql",
        params={
            "schema": Irondata.schema("eventbrite"),
            "table": Irondata.table("eventbrite", "attendees"),
            "column_list": column_lists[table],
            "pk": "id"},
        postgres_conn_id="redshift",
        autocommit=True)

ops["staging_load__attendees"] >> ops["rest_api_to_s3__updated_attendees"] >> \
    ops["create_staging_table__updated_attendees"] >> ops["s3_to_staging__updated_attendees"] >> \
    ops["staging_load__updated_attendees"]

event_check = SQLValueCheckOperator(
    dag=dag,
    task_id="validate__checked_in_events_exist",
    sql="""
SELECT
    COUNT(distinct event_id)
FROM eventbrite.attendees
WHERE
    event_id NOT IN (SELECT id FROM eventbrite.events)
    AND status = 'Checked In'
""",
    pass_value=1,
    tolerance=1,
    conn_id="redshift")

ops["staging_load__events"] >> event_check
ops["staging_load__updated_attendees"] >> event_check
