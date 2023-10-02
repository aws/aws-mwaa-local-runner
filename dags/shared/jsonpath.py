import os
from shared.irondata import Irondata
import shared.s3 as s3


def upload_jsonpath(dag_name, dag_path, entity, bucket_name=Irondata.s3_warehouse_bucket()):
    file_path = os.path.join(dag_path, f"jsonpaths/{entity}.jsonpath")
    s3_path = f"{dag_name}/{entity}.jsonpath"
    s3.load_file(file_path, s3_path, bucket_name=bucket_name)
