from shared.irondata import Irondata
from simple_salesforce import Salesforce
from shared.s3 import get_object, check_for_key
import json


def bulk_update_sfdc(bucket_name, object_key, sfdc_object, sfdc_object_id, **kwargs):

    sf = Salesforce(
        username=Irondata.get_config("SALESFORCE_USERNAME"),
        password=Irondata.get_config("SALESFORCE_PASSWORD"),
        security_token=Irondata.get_config("SALESFORCE_SECURITY_TOKEN"),
        client_id=Irondata.get_config("SALESFORCE_CLIENT_ID")
    )

    # Lookup for object to be updated in Salesforce
    sfdc_object_dict = {
        "accounts": sf.bulk.Account,
        "leads": sf.bulk.Lead,
        "opportunities": sf.bulk.Opportunity,
        "contacts": sf.bulk.Contact,
        "climb_application__c": sf.bulk.Climb_Application__c
    }

    # Connect to AWS and get object
    if check_for_key(bucket_name=bucket_name, key=object_key):
        obj = get_object(bucket_name=bucket_name, key=object_key)
        data = obj['Body'].read().decode('utf-8')

        # Parse each line in the S3 file
        raw_records = [json.loads(row) for row in data.splitlines()]
        records = [_remove_nulls(d) for d in raw_records]

        # # If data exists, bulk update the appropriate Salesforce object
        if len(records) > 0:
            sfdc_object_dict[sfdc_object].upsert(records, sfdc_object_id, batch_size=10000, use_serial=True)
            
            # Log how many records were updated
            print(f"{len(records)} records updated")
        else:
            print("No records to update.")


def _remove_nulls(d: dict):
    return {k: v for k, v in d.items() if v}