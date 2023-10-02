from shared.dag_factory import create_dag
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.baseoperator import chain

from datetime import datetime
from shared.irondata import Irondata
from shared.irontable import Irontable
from shared.operators.s3_to_redshift_staging_operator import S3ToRedshiftStagingTransfer
from discord_etl.discord_to_s3 import discord_extract


start_date = datetime(2023, 5, 31)

dag = create_dag(
    "discord",
    schedule="0 7 * * *",  # This is 7:00am local
    start_date=start_date,
    catchup=True,
    tags=["API"],
    max_active_runs=1,
)

SCHEMA = "discord"

members_table = Irontable(schema=SCHEMA, table="members", primary_key=["id", "guild_id"])
easely_members_table = Irontable(schema=SCHEMA, table="easely_members")

guilds = {
    "alumni": 948708250709262347,
    "students": 1098642064859725824
}

create_table_op = PostgresOperator(
    dag=dag,
    task_id="create_members_table",
    sql="create_members_table.sql",
    params={
        "schema": members_table.schema_in_env,
        "table": members_table.table_in_env
    },
    autocommit=True)

create_staging_op = PostgresOperator(
        dag=dag,
        task_id="create_staging_table",
        sql="create_staging_tables.sql",
        params={
            "schema": members_table.schema_in_env,
            "tables": [members_table.table_in_env]
        }
    )

identify_member_removals = PostgresOperator(
    dag=dag,
    task_id="identify_member_removals",
    sql="member_removals.sql",
    params=dict(
        columns=[
            "id",
            "name",
            "display_name",
            "nickname",
            "roles",
            "guild_id",
            "guild_name",
            "created_at",
            "joined_at"
            ],
        **members_table.to_dict()
    ))

insert_new_members = PostgresOperator(
    dag=dag,
    task_id="insert_new_members",
    sql="load_staged_data.sql",
    params={
        "schema": members_table.schema_in_env,
        "table": members_table.table_in_env,
        "pk": members_table.primary_key
        },
    )

merge_with_easely_op = PostgresOperator(
    dag=dag,
    task_id="merge_with_easely",
    sql="merge_with_easely.sql",
    params={
        "schema": easely_members_table.schema_in_env,
        "table": easely_members_table.table_in_env,
        "members_table": members_table
    },
    autocommit=True)

create_table_op >> create_staging_op
identify_member_removals >> insert_new_members
insert_new_members >> merge_with_easely_op

staging_ops = []
for guild_name, guild_id in guilds.items():

    discord_to_s3_op = ShortCircuitOperator(
        dag=dag,
        task_id=f"{guild_name}_discord_to_s3",
        python_callable=discord_extract,
        op_kwargs={
            "bucket": Irondata.s3_warehouse_bucket(),
            "members_table": members_table,
            "guild_id": guild_id,
            "guild_name": guild_name
        },
        provide_context=True)
    
    s3_to_staging = S3ToRedshiftStagingTransfer(
        dag=dag,
        task_id=f"{guild_name}_s3_to_staging",
        schema=members_table.schema_in_env,
        table=members_table.table_in_env,
        s3_key=f"{members_table.schema_in_env}/{guild_name}",
        copy_options=["CSV", "BLANKSASNULL", "TIMEFORMAT 'auto'"],
        s3_bucket=Irondata.s3_warehouse_bucket(),
    )

    create_staging_op >> s3_to_staging
    discord_to_s3_op >> s3_to_staging
    staging_ops.append(s3_to_staging)

chain(*staging_ops)
staging_ops >> identify_member_removals
