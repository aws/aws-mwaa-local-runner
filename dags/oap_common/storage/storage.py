import oap_common.storage.s3 as s3
import oap_common.storage.local as local
from airflow.models import Variable


def write_to_file(file_name, data):
    env = Variable.get("env")
    if env == "prod":
        s3.write_to_s3(file_name, data)
    else:
        local.write_to_file(file_name, data)


def read_from_file(file_name):
    env = Variable.get("env")
    if env == "prod":
        return s3.read_from_s3(file_name)
    else:
        return local.read_from_file(file_name)
