import pandas as pd
from prophet import Prophet
from shared.irondata import Irondata
from shared import s3
import psycopg2
import hashlib

def run(execution_date, bucket, input_table, output_table, **kwargs):
    # Query and fetch data from Redshift
    df = get_data(input_table)

    # Fit Prophet model and forecast 6 weeks ahead
    m = Prophet()
    m.fit(df)
    future = m.make_future_dataframe(periods=42, include_history=False)
    forecast = m.predict(future)
    
    # Add metadata to the dataset
    forecast["execution_date"] = execution_date
    forecast["id"] = forecast["ds"].astype(str) + forecast["execution_date"]
    forecast["id"] = [hashlib.md5(x.encode("utf-8")).hexdigest() for x in forecast["id"]]
    
    # Send predictions to S3
    upload_predictions(execution_date, bucket, output_table, forecast.values.tolist())

def get_data(table):
    conn_uri = Irondata.connection_uri("redshift")
    with psycopg2.connect(conn_uri) as conn:
        sql = f"select * from {table.schema_in_env}.{table.table_in_env};"
        df = pd.read_sql_query(sql, conn)
        return df

def upload_predictions(ds, bucket, table, data_list):
    s3.upload_as_csv(
        bucket,
        f"{table.schema_in_env}/{ds}/{table.table_in_env}",
        data_list)
