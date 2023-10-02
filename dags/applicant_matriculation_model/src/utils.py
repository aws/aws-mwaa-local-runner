import pickle
from jinja2 import Template
from pathlib import Path
import importlib.util as imp

# Source dir
SRC = Path(__file__).resolve().parent.as_posix()

# airflow_home = Path(os.getenv('AIRFLOW_HOME'))
# ASSETS = airflow_home / 'dags' / 'applicant_matriculation_model' / 'assets'
ASSETS = Path(__file__).resolve().parent.parent / 'assets'


def get_model(version: float):
    module = load_asset_module(version, 'model/model.py')
    return module.MODEL

def get_version(version=None) -> float:
    """
    Returns the highest version number
    found in the assets/ directory
    """
    if not version:
        versions_iter = ASSETS.iterdir()
        version = max(float(x.name) for x in versions_iter)
    return version


def get_sql_path(version: float = None,
                 dir: bool = False) -> str:
    """
    Returns the absolute path (str) for a `data_collection.sql`
    for a model version. If a version is not provided,
    the highest model version is used.

    By default this function returns the absolute path to the
    sql file. If `dir=True`, the absolute path to the sql file's
    parent directory is returned.
    """
    # Set version number
    version = version if version else get_version()
    # Generate path
    sql_path = (
        ASSETS / str(version) / 'data_collection.sql'
                )
    if dir:
        # Return parent directory path
        return sql_path.parent.as_posix()
    # Return sql abs path
    return sql_path.as_posix()


def load_sql(version=None, **kwargs):
    """
    Returns the sql source code for a .sql file.

    If `version=None`, the highest version number is used.
    If the sql file is a jinja template, template arguments
    can be passed as kwargs.

    """
    path = get_sql_path(version=version)
    with open(path, 'r') as file:
        sql = file.read()
    if kwargs:
        sql = Template(sql).render(**kwargs)
    return sql


def is_required(version, filename) -> bool:
    """
    Checks if a `.py` file exists within the
    a model's assets, and whether or not the `.py`
    file has a `main` attribute. If either return false,
    the function returns False. If both are true, the function
    returns True
    """
    py_file = filename if '.py' in filename else filename + '.py'

    module = load_asset_module(version, py_file)
    if module and hasattr(module, 'main'):
        return True


def load_asset_module(version, filename):
    path = ASSETS / str(version) / filename
    if path.is_file():
        return load_module(path)


def get_model_pkg_path(version):
    try:
        path = ASSETS / str(version) / 'pkg'
        dist_dir = (path / 'dist').glob('*.gz')
        dist = [x.as_posix() for x in dist_dir][0]
        dist = [dist]
    except IndexError:
        dist = []
    return dist


def load_module(module_pathlib):
    """
    Loads a python module using the path to the file.

    module_pathlib: A pathlib.Path object pointing to a .py file

    returns:

    A loaded python module object

    references:
    https://www.dev2qa.com/how-to-import-a-python-module-from-a-python-file-full-path/
    https://stackoverflow.com/questions/48841150/python3-importlib-util-spec-from-file-location-with-relative-path
    """
    name, path = module_pathlib.stem, module_pathlib.as_posix()
    spec = imp.spec_from_file_location(name, path)
    module = imp.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
