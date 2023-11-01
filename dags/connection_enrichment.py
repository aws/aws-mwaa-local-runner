from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from oap_common.elasticsearch import (
    fetch_latest_meters_by_ids,
    get_latest_connections_after,
    es_client,
    bulk_upsert,
    enrich_connections_onto_meters,
)
from oap_common.storage.storage import write_to_file, read_from_file
from oap_common.airflow import raise_error, construct_storage_filename
import logging
import os


def get_latest_connection_logs(**context):
    logging.info("Getting latest connection logs")
    es = es_client()
    execution_date = context["execution_date"]
    filter_start_date = execution_date - timedelta(minutes=10)
    logging.info(f"Filtering connections after {filter_start_date}")
    latest_connections = get_latest_connections_after(filter_start_date, es)
    file_name = construct_storage_filename(context, "latest_connections.json")
    write_to_file(file_name, latest_connections)
    logging.info(f"Writing connections to {file_name}")
    return True


def get_meters_for_connections(**context):
    logging.info("Getting meters for connections")
    es = es_client()
    file_name = construct_storage_filename(context, "latest_connections.json")
    logging.info(f"Reading connections from {file_name}")
    connections = read_from_file(file_name)
    logging.info(f"Read {len(connections)} connections")
    meter_ids = [
        connection["_source"]["dataKey"]["meter"]["id"] for connection in connections
    ]
    matching_meter_documents = fetch_latest_meters_by_ids(es, meter_ids)
    logging.info(f"Found {matching_meter_documents} matching meters")
    file_name = construct_storage_filename(context, "matching_meter_documents.json")
    write_to_file(file_name, matching_meter_documents)
    return True


def enrich_meters_with_connections(**context):
    logging.info("Enriching meters with connections")
    es = es_client()
    meters_docs_filename = construct_storage_filename(
        context, "matching_meter_documents.json"
    )
    matching_meter_documents = read_from_file(meters_docs_filename)
    connections_filename = construct_storage_filename(
        context, "latest_connections.json"
    )
    latest_connections = read_from_file(connections_filename)
    actions = enrich_connections_onto_meters(
        latest_connections, matching_meter_documents
    )
    actions_filename = construct_storage_filename(context, "actions.json")
    write_to_file(actions_filename, actions)
    return True


def upsert_enriched_meter_logs(**context):
    logging.info("Upserting enriched meter logs")
    es = es_client()
    actions_filename = construct_storage_filename(context, "actions.json")
    actions = read_from_file(actions_filename)
    bulk_upsert(es, actions)
    return True


with DAG(
    "connection_enrichment",
    description="Enriches connections onto meters",
    schedule_interval=None,
    default_args={"on_failure_callback": raise_error},
    start_date=datetime(2023, 7, 2),
    catchup=False,
    tags=["elasticsearch", "oap"],
) as dag:
    get_latest_connection_logs_task = PythonOperator(
        task_id="get_latest_connection_logs",
        python_callable=get_latest_connection_logs,
        dag=dag,
        provide_context=True,
        on_failure_callback=raise_error,
    )

    get_meters_for_connections_task = PythonOperator(
        task_id="get_meters_for_connections",
        python_callable=get_meters_for_connections,
        dag=dag,
        provide_context=True,
        on_failure_callback=raise_error,
    )

    enrich_meters_with_connections_task = PythonOperator(
        task_id="enrich_meters_with_connections",
        python_callable=enrich_meters_with_connections,
        dag=dag,
        provide_context=True,
        on_failure_callback=raise_error,
    )

    save_enriched_meters_to_es = PythonOperator(
        task_id="save_enriched_meters_to_es",
        python_callable=upsert_enriched_meter_logs,
        dag=dag,
        provide_context=True,
        on_failure_callback=raise_error,
    )

    (
        get_latest_connection_logs_task
        >> get_meters_for_connections_task
        >> enrich_meters_with_connections_task
        >> save_enriched_meters_to_es
    )
