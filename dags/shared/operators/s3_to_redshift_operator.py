# -*- coding: utf-8 -*-

from shared.irondata import Irondata

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer


class IronS3ToRedshift(S3ToRedshiftTransfer):
    ui_color = '#00b3e6'

    # Enables operator init params such as 'foo/{{ds}}/' or with curly braces escaped in an
    # fstring f'canvas_data_portal/{{{{ds}}}}/{table}'
    template_fields = ['s3_key']

    def __init__(self, *args, parameters=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.parameters = parameters

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        copy_options = '\n\t\t\t'.join(self.copy_options)
        copy_unload_arn = Irondata.redshift_copy_unload_role_arn()
        column_list = self.parameters and self.parameters.get("column_list")
        column_list = f"({','.join(column_list)})" if column_list else ''

        # Replaces the following line from the Airflow operator:
        # FROM 's3://{s3_bucket}/{s3_key}/{table}'
        # Decouples table naming from the S3 key in support of our development
        # environment practices.
        copy_query = """
            COPY {schema}.{table}
            {column_list}
            FROM 's3://{s3_bucket}/{s3_key}'
            credentials
            'aws_iam_role={copy_unload_arn}'
            {copy_options};
        """.format(
            schema=self.schema,
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            column_list=column_list,
            copy_unload_arn=copy_unload_arn,
            copy_options=copy_options)

        self.log.info('Executing COPY command...')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")
