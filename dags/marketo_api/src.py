import requests
import time
from shared import s3
import csv
from shared.irondata import Irondata


class MarketoAPI():

    def __init__(self):
        self.url = 'https://072-UWY-209.mktorest.com'
        self.client_id = Irondata.get_config("MARKETO_CLIENT_ID")
        self.client_secret = Irondata.get_config("MARKETO_CLIENT_SECRET")

    def set_token(self):
        token_endpoint = '/identity/oauth/token?grant_type=client_credentials'
        response = requests.get(f"{self.url}{token_endpoint}&client_id={self.client_id}&client_secret={self.client_secret}").json()
        self.token = response["access_token"]

    def renew_token(func):
        def wrapper(self, *args, **kwargs):
            self.set_token()
            return func(self, *args, **kwargs)    
        return wrapper
    
    @renew_token
    def get(self, endpoint, **kwargs):
        r = requests.get(f"{self.url}{endpoint}", 
                         headers={"Authorization": f"Bearer {self.token}"},
                         **kwargs)
        return r
    
    @renew_token
    def post(self, endpoint, **kwargs):
        r = requests.post(f"{self.url}{endpoint}", 
                          headers={"Authorization": f"Bearer {self.token}"},
                          **kwargs)
        return r

    def create_and_queue_bulk_job(self, **kwargs):
        r = self.post(endpoint="/bulk/v1/activities/export/create.json", json=kwargs["json_filter"]).json()
        print("Create and Queue Response: " + str(r))
        exportID = r["result"][0]["exportId"]
        self.post(endpoint=f"/bulk/v1/activities/export/{exportID}/enqueue.json").json()
        return exportID

    def check_job_status(self, jobID):
        r = self.get(endpoint=f"/bulk/v1/activities/export/{jobID}/status.json").json()
        print("Status: " + str(r))
        status = r["result"][0]["status"]
        return status
    
    def get_bulk_data(self, jobID):
        r = self.get(endpoint=f"/bulk/v1/activities/export/{jobID}/file.json")
        return self.convert_to_list(r.text)

    def convert_to_list(self, csv_str):
        reader = csv.reader(csv_str.splitlines(), delimiter=',')
        return list(reader)

    def run_bulk_job(self, **kwargs):
        # Create and Queue bulk extract
        jobID = self.create_and_queue_bulk_job(**kwargs)

        # Check status every 30 seconds until completed
        while self.check_job_status(jobID) in ('Created', 'Queued', 'Processing'):
            # Sleep for 30 seconds
            print("Bulk job processing...")
            time.sleep(30)

        if self.check_job_status(jobID) in ('Cancelled', 'Failed'):
            raise Exception('Bulk job cancelled or failed.')
        elif self.check_job_status(jobID) == 'Completed':
            print("Job completed. Uploading to S3.")
            s3.upload_as_csv(
                s3_bucket=kwargs["bucket"],
                s3_key=kwargs["key"],
                rows=self.get_bulk_data(jobID)
            )
        return True
