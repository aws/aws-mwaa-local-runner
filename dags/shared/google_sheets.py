import csv
import io
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from shared.irondata import Irondata
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



class GoogleSheets:
    def __init__(self, auth_key=None):
        if auth_key is None:
            auth_key = Irondata.google_sheets_key()

        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(auth_key), scope)

        self.client = gspread.authorize(credentials)

    def open_by_key(self, spread_sheet_id):
        return self.client.open_by_key(spread_sheet_id)

    def get_raw_data(self, spreadsheet_id, worksheet_name, **kwargs):
        worksheet = self.open_by_key(spreadsheet_id).worksheet(worksheet_name)

        worksheet_vals = worksheet.get_all_values()

        if "skip_n_rows" in kwargs:
            return worksheet_vals[kwargs["skip_n_rows"]:]
        else:
            return worksheet_vals

    def to_s3(self, bucket_name=None, bucket_key=None, spreadsheet_id=None, worksheet_name=None, **kwargs):
        # fetch raw_data
        sheet_vals = self.get_raw_data(spreadsheet_id, worksheet_name, **kwargs)

        # format as csv
        csv_in_mem = io.StringIO()
        writer = csv.writer(csv_in_mem)
        for row in sheet_vals:
            writer.writerow(row)

        # write values to s3
        s3 = S3Hook()

        s3.load_string(
            csv_in_mem.getvalue(),
            bucket_key,
            bucket_name=bucket_name,
            replace=True,
            encrypt=True)
