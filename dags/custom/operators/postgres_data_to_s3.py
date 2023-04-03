import json
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from custom.hooks.postgres_query_hook import PostgresQueryHook
from custom.functions.is_rds_available_and_cpu_low import is_rds_available_and_cpu_low
import time

class PostgresToS3WithSchemaOperator(BaseOperator):

    def __init__(
        self,
        s3_bucket: str,
        s3_partition_dir: str,
        s3_export_dir: str,
        db: str,
        db_schema: str,
        transfer_ds: str,
        table_name: str = None,
        postgres_conn_id: str = 'postgres_default',
        s3_conn_id: str = 'aws_default',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_partition_dir = s3_partition_dir
        self.s3_export_dir = s3_export_dir 
        self.db = db
        self.db_schema = db_schema
        self.table_name = table_name
        self.transfer_ds = transfer_ds

    def execute(self, context):
        # Initialize hooks
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        # Get table name
        if self.table_name is None:
            raise ValueError("Please provide a table name")

        self.log.info(f"Exporting table {self.table_name} to S3 bucket {self.s3_bucket}")

        # Get the partition list for the table
        table_key = f"{self.s3_partition_dir}/{self.db_schema}/{self.table_name}.json"
        table_file = s3_hook.get_key(table_key, self.s3_bucket).get()['Body'].read().decode('utf-8')
        table = json.loads(table_file)

        # From the partition file, get a list of partitions that have not been exported, or need to be re-exported
        # This is a list of dictionaries

        for partition in table:
            if partition['attached'] or partition['attached_on_last_export'] in [None, True]:
                postgres_hook = PostgresQueryHook(postgres_conn_id=self.postgres_conn_id, schema=self.db)

                # Delete data in dir
                s3_prefix = f"{self.s3_export_dir}/{self.db_schema}/{self.table_name}/{partition['schema']}/{partition['name']}"
                keys = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=s3_prefix)

                # Delete all keys
                if keys:
                    s3_hook.delete_objects(bucket=self.s3_bucket, keys=keys)
                    self.log.info(f"{len(keys)} objects deleted from S3 bucket {self.s3_bucket}")
                else:
                    self.log.info(f"No objects found with prefix {s3_prefix} in S3 bucket {self.s3_bucket}")

                # Query the schema and table information
                query = f"""
                    SELECT
                        table_name,
                        column_name,
                        data_type,
                        column_default,
                        is_nullable::boolean,
                        (
                            SELECT tc.constraint_type
                            FROM information_schema.table_constraints tc
                            WHERE
                                tc.table_schema = c.table_schema AND
                                tc.table_name = c.table_name AND
                                tc.constraint_type = 'PRIMARY KEY' AND
                                tc.constraint_name = c.column_name
                            LIMIT 1
                        ) = 'PRIMARY KEY' AS is_primary_key,
                        (
                            SELECT EXISTS (
                                SELECT 1 FROM pg_indexes i
                                WHERE
                                    i.schemaname = c.table_schema AND
                                    i.tablename = c.table_name AND
                                    i.indexdef LIKE '%' || c.column_name || '%'
                            )
                        ) AS is_indexed
                    FROM
                        information_schema.columns c
                    INNER JOIN (
                        SELECT TABLE_NAME AS parent_table
                        FROM information_schema.tables
                        WHERE table_schema = '{partition['schema']}' AND table_type = 'BASE TABLE'
                    ) AS t ON t.parent_table = c.table_name
                    WHERE
                        c.table_schema = '{partition['schema']}' AND table_name = '{partition['name']}'
                    ORDER BY
                        table_name, column_name
                    """
                rows = postgres_hook.get_results(query)

                table_schema = []
                for row in rows:
                    table_schema.append({
                        "name": row[1],
                        "type": row[2],
                        "default": row[3],
                        "nullable": row[4],
                        "primary_key": row[5],
                        "index": row[6]
                    })
                
                columns = [column['name'] for column in table_schema]

                # find time columns
                if self.table_name in ['message_event_tapped_at', 'message_event_received_at', 'message_event_sent_at']:
                    time_column = 'message_event_created_at'
                else:
                    if 'created_at' in columns:
                        time_column = 'created_at'
                    elif 'timestamp' in columns:
                        time_column = 'timestamp'
                    else:
                        time_column = None
                
                # Construct query
                # Add column name in quotes because one table has a column called "do" (in quotes) and another table has a column called reference, which is a reserved word in sql
                select_clause = ", ".join([ f'"{column}"' for column in columns])

                # Add transfer date
                select_clause = select_clause + f", \'\'{self.transfer_ds}\'\'::DATE AS transfer_ds"
                table_schema.append({
                    "name": "transfer_ds",
                    "type": "date",
                    "default": None,
                    "nullable": False,
                    "primary_key": False,
                    "index": False
                })

                # Add original database, schema, and table
                select_clause = select_clause + f", \'\'{self.db}\'\' AS original_database"
                table_schema.append({
                    "name": "original_database",
                    "type": "text",
                    "default": None,
                    "nullable": False,
                    "primary_key": False,
                    "index": False
                })

                select_clause = select_clause + f', \'\'{partition["schema"]}\'\' AS original_schema'
                table_schema.append({
                    "name": "original_schema",
                    "type": "text",
                    "default": None,
                    "nullable": False,
                    "primary_key": False,
                    "index": False
                })

                select_clause = select_clause + f', \'\'{partition["name"]}\'\' AS original_table'
                table_schema.append({
                    "name": "original_table",
                    "type": "text",
                    "default": None,
                    "nullable": False,
                    "primary_key": False,
                    "index": False
                })

                
                schema_file = {
                    "columns": table_schema,
                    "time_column": time_column
                }
                schema_s3_key = f"{self.s3_export_dir}/{self.db_schema}/{self.table_name}/{partition['schema']}/{partition['name']}/schema.json"
                
                # Output the columns to a JSON file in S3
                s3_hook.load_string(json.dumps(schema_file), key=schema_s3_key, bucket_name=self.s3_bucket, replace=True)

                # Get time limits
                where_clause = ''
                if time_column:
                    for column in table_schema:
                        if column['name'] == time_column:
                            if column['type'] == 'integer':
                                where_clause = f"WHERE TO_TIMESTAMP({time_column})::DATE <= ''{self.transfer_ds}''::DATE"
                                break
                            else:
                                where_clause = f"WHERE {time_column}::DATE <= ''{self.transfer_ds}''::DATE"
                                break
                
                table_query = f"SELECT {select_clause} FROM {partition['path']} {where_clause}"
                print(table_query)

                
                # Export data to S3
                data_s3_key = f"{self.s3_export_dir}/{self.db_schema}/{self.table_name}/{partition['schema']}/{partition['name']}/data.csv"

                # Using the above query in the export function
                export_query = f"SELECT * FROM aws_s3.query_export_to_s3('{table_query}', aws_commons.create_s3_uri('{self.s3_bucket}', '{data_s3_key}', 'us-east-1'), options :='format csv');"
                
                while not is_rds_available_and_cpu_low('peanut-prod-replica1', 'aws_default', 80):
                    self.log.info(f"RDS is starting up. Waiting 20 seconds before looking again")
                    time.sleep(20)
                
                postgres_hook.run(export_query)

                # Update partition information
                partition['last_exported_ds'] = self.transfer_ds
                partition['attached_on_last_export'] = partition['attached']
                            
                file_contents = json.dumps(table, indent=4)
                s3_hook.load_string(
                    string_data=file_contents,
                    key=table_key,
                    bucket_name=self.s3_bucket,
                    replace=True
                )
                
                self.log.info(f"Outputted partition information for table {partition}")
            