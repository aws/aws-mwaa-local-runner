import os

from airflow.hooks.base import BaseHook

import sqlalchemy

from shared.config import Config
CONFIG = Config()


class Irondata:
    def is_production():
        return "production" == Irondata.cached_get_config('ENVIRONMENT')

    def is_test():
        return "test" == Irondata.cached_get_config('ENVIRONMENT')

    def sqlalchemy_redshift_engine():
        return Irondata.sqlalchemy_engine("redshift", "redshift+psycopg2")

    def sqlalchemy_engine(conn_id, conn_type=None):
        connection_uri = Irondata.connection_uri(conn_id, conn_type)
        return sqlalchemy.create_engine(connection_uri)

    def connection_uri(conn_id, conn_type=None):
        c = BaseHook.get_connection(conn_id)
        conn_type = conn_type if conn_type else c.conn_type

        return f"{conn_type}://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}"

    def file_path_helper(relative_path):
        return os.path.join(os.path.dirname(__file__), relative_path)

    def sql_templates_path():
        return "dags/shared/sql"

    def domain_table(domain_name, entity):
        if Irondata.is_production():
            return entity
        else:
            return f"{domain_name}__{entity}"

    def use_prod_tables_for_dependencies():
        return "true" == str(Irondata.cached_get_config("USE_PROD_TABLES_FOR_DEPENDENCIES", default="true")).lower()

    def schema(schema, is_dependency=None):
        if Irondata.is_production():
            return schema
        # elif Irondata.is_test():
        #     return Irondata.test_schema()
        else:
            return Irondata.development_schema(schema, is_dependency)

    def full_table_name(schema, table):
        return f"{Irondata.schema(schema)}.{Irondata.table(schema, table)}"

    def table(schema, table, is_dependency=None):
        if Irondata.is_production():
            return table
        else:
            return Irondata.development_table(schema, table, is_dependency)

    def redshift_copy_unload_role_arn():
        return Irondata.get_config("REDSHIFT_COPY_UNLOAD_ROLE_ARN",
                                   default="arn:aws:iam::220454698174:role/redshift-copy-unload-development")

    def development_table(schema, table, is_dependency=None):
        if is_dependency and Irondata.use_prod_tables_for_dependencies():
            dev_table = table
        else:
            dev_table = f"{schema}__{table}"

        return dev_table

    def development_schema(schema, is_dependency=None):
        if schema is None:
            dev_schema = "dev_analytics"
        elif is_dependency and Irondata.use_prod_tables_for_dependencies():
            dev_schema = schema
        else:
            dev_schema = Irondata.cached_get_config("DEVELOPMENT_SCHEMA")

        return dev_schema

    def s3_warehouse_bucket():
        if Irondata.is_production():
            return 'fis-data-warehouse'
        else:
            return 'fis-data-warehouse-development'

    def reporting_schema(schema):
        schema_default = Irondata.cached_get_config('REPORTING_SCHEMA')

        if Irondata.is_production():
            return schema
        elif schema_default:
            return schema_default
        else:
            return Irondata.development_schema(schema)

    def staging_schema(schema):
        schema_default = Irondata.cached_get_config('STAGING_SCHEMA')

        if Irondata.is_production():
            return schema
        elif schema_default:
            return schema_default
        else:
            return Irondata.development_schema(schema)

    def google_sheets_key():
        return Irondata.get_config('GOOGLE_SHEETS_AUTHORIZATION_CREDENTIALS')

    def get_config(config_key, default=None):
        return CONFIG.get(config_key, default)

    def cached_get_config(config_key, default=None):
        return CONFIG.cached_get(config_key, default)
