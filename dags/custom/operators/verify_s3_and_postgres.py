from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


class VerifyS3AndPostgresOperator(BaseOperator):
    
    def __init__(
        self,
        postgres_db: str,
        s3_bucket: str,
        s3_schema_dir: str,
        s3_partition_dir: str,
        table_list_variable_name: list = None,
        postgres_conn_id: str = "postgres_default",
        s3_conn_id: str = "aws_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.postgres_db = postgres_db
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_schema_dir = s3_schema_dir
        self.s3_partition_dir = s3_partition_dir
        self.table_list_variable_name = table_list_variable_name

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        # Get list of keys in both S3 directories
        s3_keys1 = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_schema_dir)
        s3_keys2 = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_partition_dir)

        # Check if file names and lengths match
        s3_filenames1 = set([key.split("/")[-1] for key in s3_keys1])
        s3_filenames2 = set([key.split("/")[-1] for key in s3_keys2])
        
        if s3_filenames1 != s3_filenames2:
            raise ValueError(f"S3 directory file names do not match: {s3_filenames1} != {s3_filenames2}")
        if len(s3_keys1) != len(s3_keys2):
            raise ValueError(f"S3 directory lengths do not match: {len(s3_keys1)} != {len(s3_keys2)}")

        # Get table name from S3 file names
        table_names = [filename.split(".")[0] for filename in s3_filenames1]

        # Check if tables exist in Postgres
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.postgres_db)
        existing_tables = postgres_hook.get_records(f"SELECT table_name FROM information_schema.tables WHERE table_name IN ({','.join(['%s']*len(table_names))})", parameters=table_names)
        missing_tables = set(table_names) - set([row[0] for row in existing_tables])
        if missing_tables:
            raise ValueError(f"Tables {missing_tables} do not exist in Postgres")
        
        Variable.set(self.table_list_variable_name, table_names, serialize_json=True)