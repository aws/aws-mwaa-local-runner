import boto3
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook


def is_rds_available(rds_instance_identifier, aws_conn_id, cpu_threshold):
    aws_connection = BaseHook.get_connection(aws_conn_id)
    aws_access_key_id = aws_connection.login
    aws_secret_access_key = aws_connection.password
    region_name = aws_connection.extra_dejson.get('region_name', 'us-east-1')  # Set a default region if not provided
    
    rds_client = boto3.client(
        'rds',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    instance_info = rds_client.describe_db_instances(DBInstanceIdentifier=rds_instance_identifier)
    instance_status = instance_info['DBInstances'][0]['DBInstanceStatus']

    if instance_status != 'available':
        return False
    else:
        return True