import boto3
import json


def write_to_s3(file_name, data):
    s3 = boto3.client("s3")
    json_str = json.dumps(data)
    s3.put_object(Body=json_str, Bucket="my-bucket", Key=file_name)


def read_from_s3(file_name):
    s3 = boto3.client("s3")
    # Read the file from S3
    s3_object = s3.get_object(Bucket="my-bucket", Key=file_name)

    # Read the content of the file
    file_content = s3_object["Body"].read().decode("utf-8")

    # Convert the file content from JSON format to a Python object
    json_data = json.loads(file_content)

    return json_data
