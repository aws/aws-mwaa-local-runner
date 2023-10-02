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
        if worksheet_name == 'Goal by Month':
            rows = clean_data(worksheet)
    except (RuntimeError, TypeError, NameError):
        raise BaseException("Unexpected Error")

    s3.upload_as_csv(bucket_name, bucket_key, rows)


def clean_data(worksheet):
    worksheet_df = pd.DataFrame(worksheet.get_all_values())

    df = worksheet_df.iloc[17:, [0, 2, 5, 7, 9, 11]]

    col_names = ['month', 'overall_goal', 'se_goal', 'ds_goal', 'cyber_goal', 'pd_goal']

    df.columns = col_names

    df = df.query("overall_goal != ''")

    rows = df.values.tolist()

    return rows
