from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from custom.hooks.postgres_query_hook import PostgresQueryHook
import json

class PostgresPartitionsToS3Operator(BaseOperator):
    
    def __init__(
        self,
        postgres_conn_id: str,
        s3_conn_id: str,
        s3_bucket: str,
        schema: str,
        postgres_db: str,
        s3_prefix: str = "metadata/partitions",
        transfer_ds: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.postgres_db = postgres_db
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.schema = schema
        self.s3_prefix = s3_prefix
        self.transfer_ds = transfer_ds

    def execute(self, context):
        
        hook = PostgresQueryHook(postgres_conn_id=self.postgres_conn_id, schema=self.postgres_db)
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        
        # Query the tables and partitions in the schema
        query = f"""
            WITH parent_tables AS (
                SELECT n1.nspname AS parent_schema,
                    c.relname AS table_name,
                    n2.nspname AS linked_schema,
                    p.relname AS parent_table,
                    CASE
                        WHEN c.relname ~ '\\d{{8}}_\\d{{8}}$' THEN 'range'
                        WHEN c.relname ~ '\\d{{8}}$' THEN 'list'
                        WHEN c.relname ~ '_default' THEN 'default'
                        ELSE 'unknown'
                    END AS partitioning_method,
                    REGEXP_REPLACE(c.relname, '(_\\d{{8}}_\\d{{8}}$)|(_\\d{{8}}$)|(_default$)', '') AS base_table_name
                FROM pg_inherits i
                JOIN pg_class c ON i.inhrelid = c.oid
                JOIN pg_namespace n1 ON c.relnamespace = n1.oid
                JOIN pg_class p ON i.inhparent = p.oid
                JOIN pg_namespace n2 ON p.relnamespace = n2.oid
                WHERE c.relkind = 'r'
                AND n2.nspname = 'public'
                ORDER BY n1.nspname, c.relname, p.relname
            ),
            -- Define a CTE to find linked tables and their base tables.
            linked_tables AS (
                SELECT DISTINCT parent_table, base_table_name, partitioning_method
                FROM parent_tables
            ),
            -- Define a CTE to find all partitions in the 'partitions' schema.
            all_partitions AS (
                SELECT n.nspname AS schema_name,
                    c.relname AS table_name,
                    CASE
                        WHEN c.relname ~ '\\d{{8}}_\\d{{8}}$' THEN 'range'
                        WHEN c.relname ~ '\\d{{8}}$' THEN 'list'
                        WHEN c.relname ~ '_default' THEN 'default'
                        ELSE 'unknown'
                    END AS partitioning_method,
                    REGEXP_REPLACE(c.relname, '(_\\d{{8}}_\\d{{8}}$)|(_\\d{{8}}$)|(_default$)', '') AS base_table_name
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relkind = 'r'
                AND n.nspname = 'partitions'
                ORDER BY schema_name, table_name
            ),
            -- Define a CTE to find all base tables in the public schema.
            all_tables AS (
                SELECT TABLE_NAME AS parent_table
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            )
            SELECT
                all_tables.parent_table,
                COALESCE(linked_tables.base_table_name, all_tables.parent_table) AS base_table_name,
                COALESCE(linked_tables.partitioning_method, 'none') AS partitioning_method,
                COALESCE(all_partitions.table_name, all_tables.parent_table) AS partition_name,
                CASE WHEN all_partitions.table_name IS NULL THEN 'public' ELSE 'partitions' END AS source_schema,
                parent_tables.table_name IS NOT NULL OR all_partitions.table_name IS NULL AS attached
            FROM all_tables
            LEFT JOIN linked_tables USING (parent_table)
            LEFT JOIN all_partitions USING (base_table_name, partitioning_method)
            LEFT JOIN parent_tables USING (table_name)
            ORDER BY 1,2,3,4,5
            ;
        """
        
        rows = hook.get_results(query)

        # Loop through the rows and create a file for each table_name
        tables = {}
        for row in rows:
            table_name = row[0]
            partition_name = row[3]
            source_schema = row[4]
            attached = row[5]

            if table_name not in tables:
                tables[table_name] = {
                    'table_name': table_name,
                    'partitions': [],
                    'active_partitions': [],
                    'previously_active_partitions': None,
                    'inactive_partitions': [],
                    'previously_exported_partitions': None
                }
            
            partition_dict = {
                'source_schema': source_schema,
                'partition_name': partition_name,
                'updated_ds': self.transfer_ds
                }
            tables[table_name]['partitions'].append(partition_dict)
            if attached:
                tables[table_name]['active_partitions'].append(partition_dict)
            else:
                tables[table_name]['inactive_partitions'].append(partition_dict)
            
        # Loop through the tables and write the files to S3
        # check if a file already exists and if it does, check if the contents are the same

        for table_name, table in tables.items():

            s3_key = f"{self.s3_prefix}/{self.schema}/{table_name}.json"

            # check if the file already exists
            key_exists = s3_hook.check_for_key(key=s3_key, bucket_name=self.s3_bucket)
            # if it does, read the file into a dict
            if key_exists:
                file_contents = s3_hook.read_key(key=s3_key, bucket_name=self.s3_bucket)
                file_dict = json.loads(file_contents)
                # if the file contents are the same as the table dict, skip the file
                if file_dict == table:
                    continue
                else:
                    # TODO: add a check to see if the partitioning type has changed for a table

                    # merge the contents of table['partitions'] into file_dict['partitions']
                    # if the partition is already in the list, skip it
                    # if the partition is not in the list, add it
                    for partition in table['partitions']:
                        if partition not in file_dict['partitions']:
                            file_dict['partitions'].append(partition)
                    # sort the partitions by updated_ds
                    table['partitions'] = sorted(file_dict['partitions'], key=lambda k: k['updated_ds'])
                    
                    table['previously_active_partitions'] = file_dict['active_partitions']

                    if 'previously_exported_partitions' in file_dict:
                        table['previously_exported_partitions'] = file_dict['previously_exported_partitions']
                    else:
                        table['previously_exported_partitions'] = []

                    for partition in table['inactive_partitions']:
                        if partition not in file_dict['inactive_partitions']:
                            file_dict['inactive_partitions'].append(partition)
                    # sort the partitions by updated_ds
                    table['inactive_partitions'] = sorted(file_dict['inactive_partitions'], key=lambda k: k['updated_ds'])

            file_contents = json.dumps(table, indent=4)
            print(f"Writing {table_name} to S3")
            s3_hook.load_string(
                string_data=file_contents,
                key=s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )
            
            self.log.info(f"Outputted partition information for table {table_name} to S3 key {s3_key}")
            