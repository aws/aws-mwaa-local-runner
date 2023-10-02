from shared.irondata import Irondata
from shared.google_sheets import GoogleSheets
from shared import s3

import pandas as pd


def extract_data(ds, params, **kwargs):
    bucket_key = params["bucket_key"]
    bucket_name = params["bucket_name"]
    spreadsheet_key = params["spreadsheet_key"]
    worksheet_name = params["worksheet_name"]

    gs_auth_key = Irondata.google_sheets_key()
    gs = GoogleSheets(gs_auth_key)
    spreadsheet = gs.open_by_key(spreadsheet_key)
    worksheet = spreadsheet.worksheet(worksheet_name)

    try:
        rows = importer(worksheet)
    except (RuntimeError, TypeError, NameError):
        raise BaseException("Unexpected Error")

    s3.upload_as_csv(bucket_name, bucket_key, rows)


def importer(worksheet):

    # Get worksheet values and create DataFrame
    worksheet_vals = worksheet.get_all_values()
    worksheet_df = pd.DataFrame(worksheet_vals[1:], columns=worksheet_vals[0])

    # First column in the spreadsheet is blank, so we drop it
    worksheet_df.drop("", axis=1, inplace=True)

    # Split form submission timestamp and convert Excel serial date to proper datetime
    worksheet_df[["External ID", "Form Submitted At"]] = worksheet_df["External ID"].str.split('\\s+', expand=True)
    worksheet_df["Form Submitted At"] = pd.to_datetime(pd.to_numeric(worksheet_df["Form Submitted At"]),
                                                       unit="D", origin="1899-12-30")

    # Reorganize column order for DDL
    form_sub_col = worksheet_df.pop("Form Submitted At")
    worksheet_df.insert(1, "Form Submitted At", form_sub_col)

    # Format currency field as numeric
    worksheet_df["Refund Amount"].replace({'\\$': '', ',': ''}, regex=True, inplace=True)

    # Remove '_2' appended to external_ids
    worksheet_df["External ID"].replace({'_2': ''}, regex=True, inplace=True)

    # Return rows, without header, as that is set in the DDL
    rows = worksheet_df.values.tolist()

    return rows
