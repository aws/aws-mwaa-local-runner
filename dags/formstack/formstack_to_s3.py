# Base Python
import json
from typing import Union, List
from datetime import datetime, timedelta

# Irondata
from shared import s3
from shared.irontable import Irontable
from shared.formstack.endpoints import Form

# Airflow
from airflow.models.taskinstance import TaskInstance

# –––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
# FIRST TASK FUNCTION
def formstack_extract(
        ds:                 str,
        form_id:            Union[int, str],
        bucket:             str,
        submissions_table:  Irontable,
        responses_table:    Irontable,
        days:               int,
        ti:                 TaskInstance,
        **kwargs
        ) -> bool:

    # Time interval for api calls
    date_range_start_obj = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=days)
    date_range_formatted = datetime.strftime(date_range_start_obj, "%Y-%m-%d")

    # Collection submissions (API Calls)
    form = Form()
    submissions = form.submissions(
            form_id=form_id,
            min_time=f"{date_range_formatted} 00:00:00",
            max_time=f"{ds} 23:59:59",
            )
    
    if submissions:
        # Process submissions
        submission_csv, response_csv = process_submissions(form_id, submissions)

        # S3 Keys
        submissions_key = (
            f"{submissions_table.schema_in_env}/unprocessed/"
            f"{form_id}/submissions/airflow/{ds}/{submissions_table.table_in_env}"
            )
        
        responses_key = (
            f"{responses_table.schema_in_env}/unprocessed/"
            f"{form_id}/responses/airflow/{ds}/{responses_table.table_in_env}"
            )
        
        # Upload submissions to bucket/unprocessed/airflow
        s3.upload_as_csv(
            bucket,
            submissions_key,
            submission_csv
            )
        s3.upload_as_csv(
            bucket,
            responses_key,
            response_csv)
        
        # TODO rewrite this for readability
        unprocessed_keys = list(set(
            [x.get("Key") for x in s3.list_objects(
                                        bucket_name=bucket,
                                        prefix=f"{submissions_table.schema_in_env}/unprocessed/" + form_id,
                                        raise_on_none=False)
            ] + [x.get("Key") for x in s3.list_objects(
                                            bucket_name=bucket,
                                            prefix=f"{responses_table.schema_in_env}/unprocessed/" + form_id,
                                            raise_on_none=False)
                ]))
        
        ti.xcom_push(key="s3_keys", value=unprocessed_keys)
        print("submissions found. Written to S3")
        return True
    else:
        print("Found no submissions via Formstack API")
        return False



def process_submissions(form_id, submissions) -> (List[list], List[list]):

    submission_rows = [
        [
            submission.get('id'),
            form_id,
            submission.get("timestamp"),
            submission.get("user_agent"),
            submission.get("remote_addr"),
            submission.get("latitude"),
            submission.get("longitude"),
            submission.get("approval_status"),
            ] for submission in submissions
        ]

    response_rows = [
        [
            submission.get('id'),
            form_id,
            response_data.get("field"),
            response_data.get("label"),
            (json.dumps(response_data.get("value")) 
             if response_data.get("type") in ['matrix', 'checkbox'] 
             else response_data.get("value")
             ),
            response_data.get("type")
            ] for submission in submissions for response_data in submission.get("data").values()
        ]


    return submission_rows, response_rows

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
# SECOND TASK FUNCTION
def move_processed_files(
        bucket_name: str,
        form_names: list,
        ti: TaskInstance
        ):
    key_sets = ti.xcom_pull(task_ids=[f"formstack_to_s3__{form_name}" for form_name in form_names], key="s3_keys")
    keys = set()
    for key_set in key_sets:
        for key in key_set:
            keys.add(key)
    
    print(len(keys))
    print(keys)
    
    for key in keys:
        dest_key = key.replace('unprocessed', 'processed')

        try:
            s3.copy_object(
                bucket_name=bucket_name,
                source_key=key,
                dest_key=dest_key
            )

        except Exception as e:
            if 'The specified key does not exist.' in str(e):
                continue
            else:
                raise ValueError(str(e))
        try:
            s3.delete_object(
                bucket_name=bucket_name,
                key=key
                )
        except Exception as e:
            if 'The specified key does not exist.' in str(e):
                continue
            else:
                raise ValueError(str(e))


