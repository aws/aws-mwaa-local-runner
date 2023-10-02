from shared.irondata import Irondata
from simple_salesforce import Salesforce
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def push_predictions_to_sfdc(s3_bucket, s3_key):
    # Authenticate using prod or UAT credentials
    if Irondata.is_production():
        env_, domain = "", None
    else:
        env_, domain = "UAT_", "test"

    sf = Salesforce(
        username=Irondata.get_config(f"{env_}SALESFORCE_USERNAME"),
        password=Irondata.get_config(f"{env_}SALESFORCE_PASSWORD"),
        security_token=Irondata.get_config(f"{env_}SALESFORCE_SECURITY_TOKEN"),
        client_id=Irondata.get_config(f"{env_}SALESFORCE_CLIENT_ID"),
        domain=domain)
    
    # Connect to AWS and get object
    conn = S3Hook('aws_default').get_conn()
    obj = conn.get_object(Bucket=s3_bucket, Key=s3_key)
    lines = obj['Body'].iter_lines()
    header = _process(next(lines))

    # Parse each line in the S3 file
    # All files should be an ID and one data field to be mapped in SFDC
    raw_data = [dict(zip(header, _process(row))) for row in lines]
    data = [{'Id': i['record_id'], 'application_score__c': i['score']} for i in raw_data]

    # # If data exists, bulk update the appropriate Salesforce object
    if len(data) > 0:
        results = sf.bulk.Application__c.update(data, batch_size=10000, use_serial=True)
        [data[i].update(msg) for i, msg in enumerate(results)]
        print(data)

    # Log how many records were updated
    print(f"{len(data)} records updated")

def _process(line):
    return line.decode('utf-8').split(',')