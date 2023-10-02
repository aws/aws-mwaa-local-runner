# -*- coding: utf-8 -*-

from shared.irondata import Irondata

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer


class S3ToRedshiftStagingTransfer(S3ToRedshiftTransfer):
    ui_color = '#00b3e6'

    def __init__(self, strftime: str = None, parameters: dict = {}, **kwargs):
        self.strftime = strftime
        self.parameters = parameters
        super().__init__(**kwargs)

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        copy_options = '\n\t\t\t'.join(self.copy_options)
        copy_unload_arn = Irondata.redshift_copy_unload_role_arn()
        ds = (context['ds'] if not self.strftime
              else context['execution_date'].strftime(self.strftime))
        ds_table_suffix = ds.replace("-", "_")
        column_list = self.parameters.get("column_list")
        column_list = f"({','.join(column_list)})" if column_list else ''
        entity_name = self.parameters.get("entity_name")
        staging_schema = self.parameters.get("staging_schema") or "staging"
        schema_qualifier = self.parameters.get("schema_qualifer") or ""

        copy_query = """
            COPY {staging_schema}.{schema_qualifier}{schema}__{table}__{ds_table_suffix}
            {column_list}
            FROM 's3://{s3_bucket}/{s3_key}/{ds}/{filename}'
            credentials
            'aws_iam_role={copy_unload_arn}'
            {copy_options};
        """.format(
                   staging_schema=staging_schema,
                   schema_qualifier=schema_qualifier,
                   schema=self.schema,
                   table=self.table,
                   s3_bucket=self.s3_bucket,
                   s3_key=self.s3_key,
                   column_list=column_list,
                   ds=ds,
                   ds_table_suffix=ds_table_suffix,
                   filename=entity_name or self.table,
                   copy_unload_arn=copy_unload_arn,
                   copy_options=copy_options)

        self.log.info('Executing COPY command...')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")
