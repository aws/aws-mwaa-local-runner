from sys import path
from pathlib import Path
from dotenv import load_dotenv

def set_import_paths():
    dag_dir = Path(__file__).resolve().parent.parent / 'dags'
    shared_dir = dag_dir / 'shared'
    print(f'>>> Setting `from dags import` directory to: {dag_dir.as_posix()}')
    print(f'>>> Setting `from shared import` directory to: {shared_dir.as_posix()}')

    path.append(dag_dir.parent.as_posix())
    path.append(shared_dir.parent.as_posix())

def load_environment():
    env_path = Path(__file__).resolve().parent.parent / 'docker' / 'config' / '.env.localrunner'
    print(f'>>> Loading env variables from: {env_path.as_posix()}')

    load_dotenv(env_path.as_posix())


if __name__ == "__main__":
    set_import_paths()
    load_environment()