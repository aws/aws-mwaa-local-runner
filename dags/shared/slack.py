from airflow.hooks.base import BaseHook
from shared.irondata import Irondata

SLACK_CONN_ID = "slack"


def send_failure_to_slack(context):
    if Irondata.is_production():
        _send_failure_to_slack(context)
    else:
        print(f"To send to Slack:")
        print(f"{context.get('task_instance').task_id} failed")


def _send_failure_to_slack(context):
    conn = BaseHook.get_connection(SLACK_CONN_ID)
    slack_webhook_token = conn.password
    slack_channel = conn.extra_dejson["task_failure"]
    slack_msg = """
            :x: Task failed!
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {exec_date}

            *Error*: {exception}
            *Log Url*: {log_url}

            *Alerting*: {owner}
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=context.get("execution_date"),
        exception=context.get("exception"),
        log_url=context.get("task_instance").log_url,
        owner=context.get("dag").owner or "@data-engineers"
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_failure_alert",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
        channel=slack_channel,
        link_names=True
    )
    return failed_alert.execute(context=context)
