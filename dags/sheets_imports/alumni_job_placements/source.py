import csv
import io

from airflow.hooks.S3_hook import S3Hook

from shared.google_sheets import GoogleSheets


class AJPSource:
    def __init__(self, google_sheets_auth_key):
        self.google_sheets = GoogleSheets(google_sheets_auth_key)

    def extract(self, bucket_name=None, bucket_key=None, spreadsheet_id=None):
        # fetch ajp data
        sheet_vals = self.ajp_raw_data(self.google_sheets, spreadsheet_id)

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

    def ajp_raw_data(self, google_sheets, spreadsheet_id):
        worksheet = google_sheets.open_by_key(
            spreadsheet_id).worksheet("#Jobs")

        worksheet_vals = worksheet.get_all_values()

        return worksheet_vals
