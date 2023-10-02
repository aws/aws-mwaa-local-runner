from airflow import DAG
from pathlib import Path
from datetime import datetime
from jinja2 import Template
from shared.dag_factory import create_dag
from shared.irondata import Irondata
from airflow.providers.postgres.operators.postgres import PostgresOperator


sql_files = (Path(__file__).resolve().parent / 'sql').glob('*.sql')

with create_dag(
    dag_name="canvas_stream__analytics",
    start_date=datetime(2023, 8, 21),
    schedule=None,
    catchup=False,
    ) as dag:

    operators = {}
    for file in sql_files:

        # Read sql file
        sql = file.read_text()

        # Collect variables set in the .sql file using {% set variable = "value" %}
        # https://stackoverflow.com/a/71766756
        template = Template(sql)
        template.render() 
        mod = template.module   
        upstreams = {n:getattr(mod, n) for n in dir(mod) if not n.startswith('_')}.get('upstream', [])

        op = PostgresOperator(
            task_id=file.stem,
            # Extend the sql query with 
            # the shared/rotate_table.sql template
            sql=f"""
            {{% extends "rotate_table.sql" %}}
            {{% block query %}}
            {sql}
            {{% endblock %}}
            """,
            params=dict(
                schema="canvas_consumer",
                table=file.stem,
                ),
            postgres_conn_id=(
                "postgres_super" if not Irondata.is_production() 
                else "postgres_default"
                )
            )
        
        operators[file.stem] = {"operator": op, "upstreams": upstreams}

# Set upstream dependencies for each operator
for operator in operators:
    if upstreams := operators[operator]['upstreams']:
        for upstream in upstreams:
            operators[upstream]["operator"] >> operators[operator]["operator"]
