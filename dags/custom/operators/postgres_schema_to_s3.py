from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from custom.hooks.postgres_query_hook import PostgresQueryHook
import json


class PostgresSchemaToS3Operator(BaseOperator):
    
    def __init__(
        self,
        postgres_conn_id: str,
        postgres_db: str,
        s3_conn_id: str,
        s3_bucket: str,
        schema: str,
        transfer_ds: str,
        s3_prefix: str = "metadata/schema",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.postgres_db = postgres_db
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.schema = schema
        self.transfer_ds = transfer_ds
        self.s3_prefix = s3_prefix
    
    def execute(self, context):
        
        hook = PostgresQueryHook(postgres_conn_id=self.postgres_conn_id, schema=self.postgres_db)
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        
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
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ) AS t ON t.parent_table = c.table_name
            WHERE
                c.table_schema = '{self.schema}'
            ORDER BY
                table_name, column_name
        """
        
        rows = hook.get_results(query)
        
        # Build a dictionary of column information for each table
        tables = {}
        for row in rows:
            table_name = row[0]
            column_name = row[1]
            data_type = row[2]
            column_default = row[3]
            is_nullable = row[4]
            is_primary_key = row[5]
            is_indexed = row[6]
            
            if table_name not in tables:
                tables[table_name] = {
                    "name": table_name,
                    "columns": [],
                    "updated_ds": self.transfer_ds
                    }
            
            column = {
                "name": column_name,
                "type": data_type,
                "default": column_default,
                "nullable": is_nullable,
                "primary_key": is_primary_key,
                "indexed": is_indexed
            }
            
            tables[table_name]["columns"].append(column)
        
                
        # Output the column information for each table to a JSON file in S3
        for table_name, table_data in tables.items():
            columns = table_data["columns"]

            # find time columns
            if table_name in ['message_event_tapped_at', 'message_event_received_at', 'message_event_sent_at']:
                table_data['time_column'] = 'message_event_created_at'
            else:
                for column in columns:
                    if 'created_at' == column['name']:
                        table_data['time_column'] = 'created_at'
                        break
                    elif 'timestamp' == column['name']:
                        table_data['time_column'] = 'timestamp'
                        break
                    else:
                        table_data['time_column'] = None
            
            # Check if a file with the same name already exists in S3
            s3_key = f"{self.s3_prefix}/{self.schema}/{table_name}.json"
            file_exists = s3_hook.check_for_key(s3_key, bucket_name=self.s3_bucket)
            
            if file_exists:
                # If a file with the same name already exists, check if the column names and types are the same
                existing_columns = json.loads(s3_hook.read_key(s3_key, bucket_name=self.s3_bucket))
                if existing_columns == columns:
                    self.log.info(f"Skipping output of column information for table {table_name} to S3 key {s3_key} because file already exists and columns are the same")
                    continue
            
            # Output the columns to a JSON file in S3
            s3_hook.load_string(json.dumps(table_data), key=s3_key, bucket_name=self.s3_bucket, replace=True)
            
            self.log.info(f"Outputted column information for table {table_name} to S3 key {s3_key}")
