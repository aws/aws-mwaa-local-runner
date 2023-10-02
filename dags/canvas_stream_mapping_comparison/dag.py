from airflow.providers.postgres.operators.postgres import PostgresOperator
from shared.dag_factory import create_dag 
from datetime import datetime
from pathlib import Path
from shared.irontable import Irontable
from shared.irondata import Irondata

sql = Path(__file__).resolve().parent / 'sql'

with create_dag(
    dag_name="canvas_stream_comparison",
    schedule="0 */4 * * *", # every four hours
    start_date=datetime(2023, 5, 8),
) as dag:
    
    # Collect all sql files from each method directory
    sql_files = [x for x in sql.iterdir() 
                    if 'template' # exclude the template file
                    != x.stem]

    # Loop over each sql file
    for file in sql_files:
        

        offering = 'submissions'
        schema = f'canvas_stream_comparison'
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
            dest_schema=schema,
            dest_table=table.table,
            )
        )

        op
