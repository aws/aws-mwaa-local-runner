import boto3
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook


def get_latest_rds_cpu_load(rds_instance_identifier, aws_conn_id):
    aws_connection = BaseHook.get_connection(aws_conn_id)
    aws_access_key_id = aws_connection.login
    aws_secret_access_key = aws_connection.password
    aws_session_token = aws_connection.extra_dejson.get('aws_session_token', None)
    region_name = aws_connection.extra_dejson.get('region_name', 'us-east-1')  # Set a default region if not provided
    
    cloudwatch_client = boto3.client(
        'cloudwatch',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=10)  # Last 10 minutes

    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='CPUUtilization',
        Dimensions=[
            {
                'Name': 'DBInstanceIdentifier',
                'Value': rds_instance_identifier
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=60,  # 1 minute granularity
        Statistics=['Average']
    )

    datapoints = response['Datapoints']
    
    if not datapoints:
        return None

    # Sort datapoints by timestamp and get the latest
    latest_datapoint = sorted(datapoints, key=lambda x: x['Timestamp'], reverse=True)[0]
    latest_cpu_load = latest_datapoint['Average']

    return latest_cpu_load