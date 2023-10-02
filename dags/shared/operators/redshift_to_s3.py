from pandas import DataFrame
from typing import Sequence
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from shared.s3 import S3CSVConnection


class RedshiftToS3Operator(BaseOperator, S3CSVConnection):
    # The task node color
    ui_color: str = '#e4ae0e'
    # The task font color
    ui_fgcolor: str = 'white'
    # Telling airflow that the `.sql` and `.s3_key`
    # attributes are jinja templates
    template_fields: Sequence[str] = ('sql', 's3_key')
    # Telling airflow that the `.sql` attribute is a
    # template for an sql file.
    template_fields_renderers = {
        'sql': 'sql'
    }
    # Telling airflow if `.sql` is found in a
    # template attribute, that it is a filepath
    template_ext: Sequence[str] = ('.sql',)

    def __init__(
            self,
            sql,
            bucket_name,
            s3_key,
            s3_conn_id='aws_default',
            redshift_conn_id='postgres_default',
            replace=True,
            sep=',',
            **kwargs
            ):
        # Inheriting airflow operator tooling
        BaseOperator.__init__(self, **kwargs)
        # Opening a connection to S3
        S3CSVConnection.__init__(
            self,
            aws_conn_id=s3_conn_id,
            bucket_name=bucket_name,
            s3_key=s3_key,
            replace=replace,
            sep=sep,
            )
        # template attribute
        self.sql = sql
        # Used to connect to redshift via a PostgresHook
        self.redshift_conn_id = redshift_conn_id

    def get_data(self) -> DataFrame:
        """
        1. Opens a connection to redshift
        2. Queries redshift and returns results as a pandas dataframe
        3. Logs the shape of the data
        4. Returns the dataframe
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        data = redshift_hook.get_pandas_df(self.sql)
        print('GET DATA', data.shape)
        return data

    def execute(self, context):
        """
        The primary function used by airflow.

        1. Executes the sql and returns the results as a dataframe
        2. Converts the data to a csv file and pushes it to redshift
        3. Logs the data's shape
        """
        data = self.get_data()
        self.push_to_s3(data)
        print('\n\nPULL SHAPE\n\n', self.pull_from_s3().shape)
