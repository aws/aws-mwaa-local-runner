from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
from pathlib import Path
import os

env_path = Path(__file__).resolve().parents[3] / 'docker' / 'config' / '.env.localrunner'
load_dotenv(env_path.as_posix())

env = os.environ
conn_id = 'postgres_super' if env.get('AIRFLOW_CONN_POSTGRES_SUPER') else 'postgres_default'

hook = PostgresHook(postgres_conn_id=conn_id)
