# std
from typing import List, Optional as Opt
import urllib

import boto3
import requests

# airflow
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator


def get_start_timestamp(delay_minutes: Opt[int] = None) -> str:
    if delay_minutes:
        return '{{ execution_date.subtract(minutes=' + str(delay_minutes) + ').isoformat() }}'
    else:
        return '{{ execution_date.isoformat() }}'


def get_end_timestamp(delay_minutes: Opt[int] = None) -> str:
    if delay_minutes:
        return '{{ next_execution_date.subtract(minutes=' + str(delay_minutes) + ').isoformat() }}'
    else:
        return '{{ next_execution_date.isoformat() }}'


def get_dbt_job_id(delay_minutes: Opt[int] = None) -> str:
    if delay_minutes:
        return '{{ execution_date.subtract(minutes=' + str(delay_minutes) + ').strftime("%Y/%m/%d/%H/%M") }}'
    else:
        return '{{ execution_date.strftime("%Y/%m/%d/%H/%M") }}'


def get_dbt_role_connection() -> str:
    variable = Variable.get('dbt_role')
    return variable


def dbt_operator(
        dag: DAG,
        task_id: str,
        command: List[str],
        snowplow_environment: str = 'dev',
        delay_minutes: int = 0,
        retries: int = 1,
        region_name: str = 'us-east-1',
        on_failure_callback=None,
        **kwargs,
):
    cluster = Variable.get('dbt_cluster')
    task_definition = Variable.get('dbt_task_definition')
    subnets = Variable.get('private_subnet_ids', deserialize_json=True)
    awslogs = Variable.get('dbt_awslogs', deserialize_json=True)
    awslogs_group = awslogs['awslogs_group']
    awslogs_region = awslogs['awslogs_region']
    awslogs_stream_prefix = awslogs['awslogs_stream_prefix']
    dbt_command = ' '.join(command)
    airflow_environment = Variable.get('airflow_environment')

    return EcsRunTaskOperator(
        aws_conn_id=get_dbt_role_connection(),
        dag=dag,
        task_id=task_id,
        on_failure_callback=on_failure_callback,
        retries=retries,
        overrides={
            'containerOverrides': [{
                'name': task_definition,
                'command': command,
                'environment': [
                    {
                        'name': 'AIRFLOW_DAG_ID',
                        'value': dag.dag_id,
                    },
                    {
                        'name': 'AIRFLOW_TASK_ID',
                        'value': task_id,
                    },
                    {
                        'name': 'AIRFLOW_RUN_ID',
                        'value': '{{ run_id }}',
                    },
                    {
                        'name': 'AIRFLOW_TASK_INSTANCE_KEY_STR',
                        'value': '{{ task_instance_key_str }}',
                    },
                    {
                        'name': 'AIRFLOW_JOB_ID',
                        'value': '{{ dag_run.creating_job_id }}',
                    },
                    {
                        'name': 'DBT_JOB_ID',
                        'value': get_dbt_job_id(delay_minutes),
                    },
                    {
                        'name': 'SNOWPLOW_ENV',
                        'value': snowplow_environment.upper(),
                    },
                    {
                        'name': 'SNOWPLOW_ENVIRONMENT',
                        'value': snowplow_environment.upper(),
                    },
                    {
                        'name': 'AIRFLOW_ENVIRONMENT',
                        'value': airflow_environment,
                    },
                    {
                        'name': 'CLUSTER',
                        'value': cluster,
                    },
                    {
                        'name': 'TASK_DEFINITION',
                        'value': task_definition,
                    },
                    {
                        'name': 'DBT_COMMAND',
                        'value': dbt_command,
                    },
                    {
                        'name': 'AWSLOGS_GROUP',
                        'value': awslogs_group,
                    },
                    {
                        'name': 'AWSLOGS_REGION',
                        'value': awslogs_region,
                    },
                    {
                        'name': 'AWSLOGS_STREAM_PREFIX',
                        'value': awslogs_stream_prefix,
                    },
                ]
            }]
        },
        cluster=cluster,
        region=region_name,
        task_definition=task_definition,
        launch_type='FARGATE',
        network_configuration={
            'awsvpcConfiguration': {
                'assignPublicIp': 'DISABLED',
                'securityGroups': [],
                'subnets': subnets,
            }
        },
        awslogs_group=awslogs_group,
        awslogs_region=awslogs_region,
        awslogs_stream_prefix=awslogs_stream_prefix,
        **kwargs,
    )

def dbt_failure_alert(context):
    dag_id = context['dag'].dag_id
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    task = task_instance.task
    task.render_template_fields(context, jinja_env=context['dag'].get_template_env())
    command = task.overrides["containerOverrides"][0]["command"]
    execution_date = context["execution_date"]

    airflow_environment = Variable.get('airflow_environment')
    full_environment = {
        "prod": "production (aws-cnndps-mutation-prod-devops)",
        "dev": "dev (aws-cnndps-mutation-dev-devops)",
        "internal": "internal (aws-cnndps-mutation-dev-devops)",
    }[airflow_environment]
    airflow_url = { # mapping of execution role to Airflow URL to create links
        "prod": "c49bfbd5-3835-4433-814a-865794e63f90.c56.us-east-1.airflow.amazonaws.com",
        "dev": "27146498-52b1-4232-a323-357a0de5c643.c66.us-east-1.airflow.amazonaws.com",
        "internal": "62ffd060-0192-41f0-b737-46012772a665.c26.us-east-1.airflow.amazonaws.com",
    }[airflow_environment]
    secret_id = {
        "prod": "secrets-slack-prod",
        "dev": "secrets-slack-nonprod",
        "internal": "secrets-slack-nonprod",
    }[airflow_environment]
    slack_channel = {
        "prod": "zion-dbt-alerts-prod",
        "dev": "zion-dbt-alerts-non-prod",
        "internal": "zion-dbt-alerts-non-prod",
    }[airflow_environment]

    webhook_url = boto3.client('secretsmanager').get_secret_value(SecretId=secret_id)["SecretString"]

    requests.post(
        webhook_url,
        json = {
            "channel": slack_channel,
            "username": "dbt Alert",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":rotating_light: *dbt Failure* :rotating_light:\n*Environment:* `{full_environment}`\n*DAG:* `{dag_id}`\n*Task:* `{task_id}`\n\n*Command:* ```{' '.join(command)}```"
                    }
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "AWS Sign In"
                            },
                            "style": "primary",
                            "url": "https://tw.okta.com/home/amazon_aws/0oan2tf6x27N9TY9F0x7/272" # link to sign into AWS for convenience
                        },
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "View DAG"
                            },
                            "style": "primary",
                            "url": f"https://{airflow_url}/tree?dag_id={dag_id}"
                        },
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "View Logs"
                            },
                            "style": "primary",
                            "url": f"https://{airflow_url}/log?dag_id={dag_id}&task_id={task_id}&execution_date={urllib.parse.quote(execution_date.isoformat())}"
                        }
                    ]
                }
            ]
        }
    )
