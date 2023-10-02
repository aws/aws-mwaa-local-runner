import csv
import json
from io import StringIO
import logging
from pandas import DataFrame, read_csv

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block


def delete_object(bucket_name, key):
    conn = S3Hook('aws_default').get_conn()

    return conn.delete_object(
        Bucket=bucket_name,
        Key=key
    )


def read_csv_header(bucket_name, object_key, encoding='utf-8'):
    conn = S3Hook('aws_default').get_conn()
    obj = conn.get_object(Bucket=bucket_name, Key=object_key)
    return next(obj['Body'].iter_lines()).decode(encoding)


def copy_object(bucket_name, source_key, dest_key):
    conn = S3Hook('aws_default').get_conn()

    return conn.copy_object(
        Bucket=bucket_name,
        Key=dest_key,
        CopySource=f'{bucket_name}/{source_key}'
    )


def get_object(bucket_name, key):
    conn = S3Hook('aws_default').get_conn()
    return conn.get_object(Bucket=bucket_name, Key=key)


def check_for_key(key, bucket_name):
    conn = S3Hook('aws_default')
    return conn.check_for_key(key=key, bucket_name=bucket_name)


def check_for_wildcard_key(wildcard_key, bucket_name):
    conn = S3Hook('aws_default')
    return conn.check_for_wildcard_key(wildcard_key, bucket_name, delimiter='/')


def list_keys(bucket_name, prefix, delimiter):
    conn = S3Hook('aws_default')
    return conn.list_keys(bucket_name, prefix, delimiter)


def list_objects(bucket_name, prefix='', delimiter='', page_size=None, max_items=None, raise_on_none=True):
    conn = S3Hook('aws_default').get_conn()
    config = {
       'PageSize': page_size,
       'MaxItems': max_items,
    }

    paginator = conn.get_paginator('list_objects_v2')
    response = paginator.paginate(Bucket=bucket_name,
                                  Prefix=prefix,
                                  Delimiter=delimiter,
                                  PaginationConfig=config)

    for page in response:
        if 'Contents' in page:
            for obj in page['Contents']:
                yield(obj)
        else:
            logging.error(page)
            if raise_on_none:
                raise BaseException(f"no matching s3 objects found matching {bucket_name}/{prefix}")


def load_json(data, s3_path, bucket_name, replace=True, encrypt=True):
    S3Hook('aws_default').load_string(
        "\n".join(map(json.dumps, data)),
        s3_path,
        bucket_name,
        replace=True,
        encrypt=True)


def load_bytes(bytes_data, s3_path, bucket_name, replace=True, encrypt=True):
    S3Hook('aws_default').load_bytes(
        bytes_data,
        s3_path,
        bucket_name=bucket_name,
        replace=replace,
        encrypt=encrypt)


def load_file(file_path, s3_path, bucket_name, replace=True, encrypt=True):
    S3Hook('aws_default').load_file(
        file_path,
        s3_path,
        bucket_name=bucket_name,
        replace=replace,
        encrypt=encrypt)


def upload_as_csv(s3_bucket, s3_key, rows):
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    for row in rows:
        writer.writerow(row)

    S3Hook('aws_default').load_string(
        csv_buffer.getvalue(),
        s3_key,
        bucket_name=s3_bucket,
        replace=True,
        encrypt=True)
    

def copy_to_staging(ds, s3_bucket, prefix, column_list, table, upsert_keys=[], copy_options=[]):
    # Establist data source connections
    redshift_hook = PostgresHook("postgres_default")
    s3_hook = S3Hook('aws_default')

    # Bulid COPY parameters
    destination = "staging." + "__".join([table.schema_in_env, table.table_in_env, ds.replace("-", "_")])
    copy_destination = "#" + table.table_in_env
    column_names = ", ".join(column_list)
    credentials = s3_hook.get_credentials()
    credentials_block = build_credentials_block(credentials)
    copy_options = '\n\t\t\t'.join(copy_options)
    where_statement = ' AND '.join([f'{destination}.{k} = {copy_destination}.{k}' for k in upsert_keys])

    # For each key in bucket, create the temp table and upsert into staging
    key_list = list_keys(s3_bucket, prefix, '/')
    for key in key_list:
        copy_statement = f"""
            COPY {copy_destination} ({column_names})
            FROM 's3://{s3_bucket}/{key}'
            credentials
            '{credentials_block}'
            {copy_options};
        """
        sql = f"""
            CREATE TABLE {copy_destination} (LIKE {destination} INCLUDING DEFAULTS);
            {copy_statement}
            BEGIN;
            DELETE FROM {destination} USING {copy_destination} WHERE {where_statement};
            INSERT INTO {destination} SELECT * FROM {copy_destination};
            COMMIT
        """
        redshift_hook.run(sql, autocommit=True)


class S3CSVConnection:

    def __init__(
            self,
            s3_key,
            bucket_name,
            replace=True,
            aws_conn_id='aws_default',
            sep=',',
            headers=True,
            pull_header='infer',
            **kwargs
            ):
        self.hook = S3Hook(aws_conn_id=aws_conn_id)
        self.s3_key = s3_key
        self.replace = replace
        self.sep = sep
        self.bucket_name = bucket_name
        self.headers = headers
        self.pull_header = pull_header

    def push_to_s3(self, df: DataFrame):
        csv = df.to_csv(sep=self.sep, index=False, header=self.headers)
        print('\n\nCSV\n\n', csv[:500])
        self.hook.load_string(
            csv,
            self.s3_key,
            bucket_name=self.bucket_name,
            replace=self.replace,
            encrypt=True,
            )

    def pull_from_s3(self):
        encoded = self.hook.read_key(key=self.s3_key, bucket_name=self.bucket_name)
        data = StringIO(encoded)
        print('\n\nPULL data', data, '\n\n')
        df = read_csv(data, sep=self.sep, header=self.pull_header)
        return df
