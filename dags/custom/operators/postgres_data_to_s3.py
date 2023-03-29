import json
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresToS3WithSchemaOperator(BaseOperator):

    def __init__(
        self,
        s3_bucket: str,
        s3_schema_dir: str,
        s3_partition_dir: str,
        s3_export_dir: str,
        schema: str,
        transfer_ds: str,
        table_name: str = None,
        schema_file_suffix: str = ".json",
        postgres_conn_id: str = 'postgres_default',
        s3_conn_id: str = 'aws_default',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.schema_file_suffix = schema_file_suffix
        self.s3_schema_dir = s3_schema_dir
        self.s3_partition_dir = s3_partition_dir
        self.s3_export_dir = s3_export_dir 
        self.schema = schema
        self.table_name = table_name
        self.transfer_ds = transfer_ds

    def execute(self, context):
        # Initialize hooks
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        # Get table name
        if self.table_name is None:
            raise ValueError("Please provide a table name")

        self.log.info(f"Exporting table {self.table_name} to S3 bucket {self.s3_bucket}")

        # Get schema definition for table
        schema_key = f"{self.s3_schema_dir}/{self.schema}/{self.table_name}{self.schema_file_suffix}"
        schema_file = s3_hook.get_key(schema_key, self.s3_bucket).get()['Body'].read().decode('utf-8')
        schema = json.loads(schema_file)

        # Get columns from schema
        columns = schema["columns"]
        column_names = [column["name"] for column in columns]

        # Get time limits
        time_column = schema['time_column']
        where_clause = ''
        if time_column:
            for column in columns:
                if column['name'] == time_column:
                    if column['type'] == 'integer':
                        where_clause = f"WHERE TO_TIMESTAMP({time_column})::DATE <= ''{self.transfer_ds}''::DATE"
                    else:
                        where_clause = f"WHERE {time_column}::DATE <= ''{self.transfer_ds}''::DATE"

        # Get partitions
        partition_key = f"{self.s3_partition_dir}/{self.schema}/{self.table_name}{self.schema_file_suffix}"
        partition_file = s3_hook.get_key(partition_key, self.s3_bucket).get()['Body'].read().decode('utf-8')
        partitions = json.loads(partition_file)

        previously_exported_partitions = [f"{partition['source_schema']}.{partition['partition_name']}" for partition in partitions['previously_exported_partitions']]
        active_partitions = [f"{partition['source_schema']}.{partition['partition_name']}" for partition in partitions['active_partitions']]
        previously_active_partitions = [f"{partition['source_schema']}.{partition['partition_name']}" for partition in partitions['previously_active_partitions']]
        inactive_partitions = [f"{partition['source_schema']}.{partition['partition_name']}" for partition in partitions['inactive_partitions']]

        partitions_to_process = active_partitions

        for partition in previously_active_partitions:
            if partition not in partitions_to_process:
                partitions_to_process.append(partition)
        
        for partition in inactive_partitions:
            if partition not in partitions_to_process and partition not in previously_exported_partitions:
                partitions_to_process.append(partition)
        
        for partition in partitions_to_process:
            # Example partition name: public.users or partitions.user_session_20231212
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, connect_timeout=600)

            # Delete data in dir
            s3_prefix = f"{self.s3_export_dir}/{self.schema}/{self.table_name}/{partition.split('.')[0]}/{partition.split('.')[1]}"
            keys = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=s3_prefix)

            # Delete all keys
            if keys:
                s3_hook.delete_objects(bucket=self.s3_bucket, keys=keys)
                self.log.info(f"{len(keys)} objects deleted from S3 bucket {self.s3_bucket}")
            else:
                self.log.info(f"No objects found with prefix {s3_prefix} in S3 bucket {self.s3_bucket}")
            
            # Construct query
            select_clause = ", ".join([ '"' + column_name + '"' for column_name in column_names])
            query = f"SELECT {select_clause} FROM {partition} {where_clause}"
            
            # Export data to S3
            s3_key = f"{self.s3_export_dir}/{self.schema}/{self.table_name}/{partition.split('.')[0]}/{partition.split('.')[1]}/{partition.split('.')[1]}.csv"

            sql = f"SELECT * FROM aws_s3.query_export_to_s3('{query}', aws_commons.create_s3_uri('{self.s3_bucket}', '{s3_key}', 'us-east-1'), options :='format csv');"
            postgres_hook.run(sql)
            
            if partition not in previously_exported_partitions:
                partitions['previously_exported_partitions'].append(
                    {
                        'source_schema': partition.split(".")[0],
                        'partition_name': partition.split(".")[1],
                        'updated_ds': self.transfer_ds
                        }
                )
            
            file_contents = json.dumps(partitions, indent=4)
            s3_hook.load_string(
                string_data=file_contents,
                key=partition_key,
                bucket_name=self.s3_bucket,
                replace=True
            )
            
            self.log.info(f"Outputted partition information for table {partition} to S3 key {s3_key}")
            