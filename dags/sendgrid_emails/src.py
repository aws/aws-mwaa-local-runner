import requests
from shared.irondata import Irondata
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def send_emails(bucket, key, template_id, sender, is_prod):
    BASE_URL = 'https://api.sendgrid.com/v3/mail/send'
    KEY = Irondata.get_config("SENDGRID_API_KEY")
    headers = {
        'Authorization': f'Bearer {KEY}',
        'Content-Type': 'application/json'
    }

    # Connect to AWS and get object
    conn = S3Hook('aws_default').get_conn()
    obj = conn.get_object(Bucket=bucket, Key=key)
    lines = obj['Body'].iter_lines()
    header = next(lines).decode('utf-8').split(",")

    # Parse each line in the S3 file
    template_data = [
        dict(zip(header, l.decode('utf-8').split(","))) for l in lines
    ]

    # Add customizations to email
    data = {
        "template_id": template_id,
        "from": {
            "email": sender,
            "name": "Flatiron School"
        },
        "personalizations": [
            {
                "to": [{ 
                    "email": row["email"] if is_prod else 'mitch@flatironschool.com'
                }],
                "dynamic_template_data": row
            } for row in template_data
        ]
    }

    # Send emails
    r = requests.post(BASE_URL, json=data, headers=headers)
    return {'result': r.text}
