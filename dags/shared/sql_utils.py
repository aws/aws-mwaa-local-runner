from jinja2 import Environment, FileSystemLoader
import logging
import psycopg2
import re
import io
import csv

from shared.irondata import Irondata
from shared.s3 import read_csv_header
from shared.utils import quote_columns

DYNAMIC_TBL_TEMPL_PATH = "sql/"
DYNAMIC_TBL_TEMPL_FILENAME = "reset_dynamic_table.sql"


def reset_dynamic_table(ds, **context):
    bucket_name = context['bucket_name']
    object_key = context['object_key']
    buf = io.StringIO(read_csv_header(bucket_name, object_key))
    cols = list(csv.reader(buf))[0]
    col_list = sqlize_names(cols)
    quoted_col_list = quote_columns(col_list)

    schema = context['schema']
    table = context['table']
    varchar_size = context['varchar_size']

    create_table_sql = generate_loading_table_sql(schema, table, quoted_col_list, varchar_size)

    logging.info(create_table_sql)

    conn_uri = Irondata.connection_uri("redshift")
    print(conn_uri)
    conn = psycopg2.connect(conn_uri)
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    
    task_instance = context['task_instance']
    task_instance.xcom_push(key='num_cols', value=len(cols))


def generate_loading_table_sql(schema, table, col_list, varchar_size):
    templ_path = Irondata.file_path_helper(DYNAMIC_TBL_TEMPL_PATH)
    templ_env = Environment(loader=FileSystemLoader(templ_path))
    template = templ_env.get_template(DYNAMIC_TBL_TEMPL_FILENAME)
    return template.render({
        "params": {
            "schema": schema,
            "table": table,
            "cols": list(map(lambda col: f"{col} VARCHAR({varchar_size})", col_list))
        }
    })


def sqlize_names(cols):
    # Fill in blank columns
    unique_cols = unique_list(cols)
    
    # Snake-case columns
    snake_cols = list(map(lambda col: re.sub("\W+", "_", col.lower().strip()), unique_cols))

    return snake_cols


def unique_list(lst):
    unique_elements = {}
    result = []

    for element in lst:
        if element == "":
            result.append("X")
            if "X" not in unique_elements:
                unique_elements["X"] = 1
            else:
                unique_elements["X"] += 1
            result[-1] += str(unique_elements["X"])
        elif element in unique_elements:
            unique_elements[element] += 1
            result.append(element + str(unique_elements[element]))
        else:
            unique_elements[element] = 1
            result.append(element)

    return result
