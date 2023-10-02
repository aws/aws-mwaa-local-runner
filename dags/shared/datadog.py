from airflow import settings
from shared.irondata import Irondata
import logging


def get_connection():
    return DatadogHook(datadog_conn_id="datadog")


def send_success_to_datadog(context):
    dag = context["dag"]
    dag_id = dag.dag_id
    run_id = context["run_id"]

    title = f"Airflow: DAG {dag_id} ran successfully"
    text = f"{dag_id} completed {run_id} successfully."
    alert_type = "success"

    send_to_datadog(dag_id, title, text, alert_type)


def send_failure_to_datadog(context):
    dag = context["dag"]
    dag_id = dag.dag_id
    run_id = context["run_id"]

    title = f"Airflow: DAG {dag_id} failed"
    text = f"{dag_id} {run_id} failed."
    alert_type = "error"

    send_to_datadog(dag_id, title, text, alert_type)


def send_to_datadog(dag_id, title, text, alert_type):
    if Irondata.is_production():
        datadog = get_connection()
        datadog.post_event(title, text, alert_type=alert_type, tags=["service:airflow"])
        query_and_send_failures_to_datadog(datadog, dag_id)
    else:
        logging.info("To send to Datadog:")
        logging.info(f"title: {title}")
        logging.info(f"text: {text}")
        logging.info(f"alert_type: {alert_type}")


def query_and_send_failures_to_datadog(datadog, dag_id):
    engine = settings.engine

    [dag_failed_count] = engine.execute(
        f"""
        select count(dag_id)
        from dag_run
        where dag_id = '{dag_id}'
        and state = 'failed'
        """
    ).fetchone()
    [total_failed_count] = engine.execute(
        f"""
        select count(dag_id)
        from dag_run
        where state = 'failed'
        """
    ).fetchone()
    logging.info(f"{dag_failed_count} failure(s) found for {dag_id}. Sending to Datadog.")
    datadog.send_metric("airflow.dagrun.failed.total", total_failed_count, [
        "service:airflow"], "gauge")
    datadog.send_metric(f"airflow.dagrun.failed.{dag_id}.total", dag_failed_count, [
        "service:airflow", f"dag:{dag_id}"], "gauge")
