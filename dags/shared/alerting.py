from shared.datadog import send_success_to_datadog, send_failure_to_datadog
from shared.slack import send_failure_to_slack


def dag_success_handler(context):
    send_success_to_datadog(context)


def dag_failure_handler(context):
    send_failure_to_datadog(context)


def task_failure_handler(context):
    send_failure_to_slack(context)
