from airflow.providers.postgres.operators.postgres import PostgresOperator
from shared.dag_factory import create_dag 
from datetime import datetime
from pathlib import Path
from shared.irontable import Irontable
from shared.irondata import Irondata
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sql = Path(__file__).resolve().parent / 'sql'

with create_dag(
    dag_name="canvas_stream_mapping",
    schedule=None, # Only triggered by canvas_streaming_load
    start_date=datetime(2023, 5, 8),
) as dag:
    
    # Collect all sql files from each method directory
    sql_files = [x for x in sql.iterdir() 
                    if 'template' # exclude the template file
                    != x.stem]

    # Loop over each sql file
    ops = []
    for file in sql_files:
        
        # Generate operators for consumer and enterprise
        for offering in ['consumer', 'enterprise']:

            schema = f'canvas_{offering}'
            table = Irontable(
                schema=schema, 
                table=file.stem, 
                primary_key='id'
            )

            op = PostgresOperator(
                task_id=f"{file.stem}_{offering}_{file.parent.stem}",
                sql=f'{file.name}',
                postgres_conn_id='postgres_default' if Irondata.is_production() else 'postgres_super',
                autocommit=True,
                params=dict(
                dest_schema=table.schema_in_env,
                dest_table=table.table_in_env,
                )
            )
            ops.append(op)
            op

trigger = TriggerDagRunOperator(
    task_id="trigger_analytics_dag",
    trigger_dag_id="canvas_stream__analytics",
    trigger_rule="all_done"
)

ops >> trigger
