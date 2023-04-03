from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

# Send slack notification from our failure callback


def task_failure_callback(context):
    slack_msg = f"""
    :red_circle: Airflow Task Failed.
    *Task*: {context.get('task_instance').task_id}
    *Dag*: {context.get('task_instance').dag_id}
    *Execution Time*: {context.get('execution_date')}
    *Log Url*: {context.get('task_instance').log_url}
    """

    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_webhook')
    slack_hook.send(text=slack_msg)
