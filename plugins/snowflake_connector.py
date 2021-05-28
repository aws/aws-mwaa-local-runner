import boto3
import json
import os
import snowflake.connector
import botocore.exceptions

from typing import Optional, Union
from io import StringIO
from contextlib import closing
from snowflake.connector.util_text import split_statements
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.plugins_manager import AirflowPlugin
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.backends import default_backend as crypto_default_backend

TDM_NAME = os.environ.get('SNOWFLAKE_DATABASE', 'tdm_test')
TDM_SHORT_NAME = TDM_NAME[4:]
SECRETS_MANAGER_KEY = f'tdm/{TDM_NAME.lower()}_prod_etl_srvc'


def get_boto_client(service_name):
    client = boto3.client(service_name=service_name, region_name='us-west-2')
    return client


def get_local_snowflake_connection():
    print("Building local snowflake connection with sdm")
    connection_config = {
        "account": "sdm",
        "database": "TDM_SANDBOX",
        "host": "host.docker.internal",
        "password": "<password>",
        "port": "1444",
        "protocol": "http",
        "region": 'us-west-2',
        "schema": "SANDBOX",
        "user": "<username>",
        "warehouse": "TDM_SANDBOX"
    }
    print(connection_config)
    return connection_config


def get_snowflake_connection_with_secrets_manager(credentials_client):
    response = credentials_client.get_secret_value(
        SecretId=SECRETS_MANAGER_KEY
    )
    secret_data = json.loads(response['SecretString'])
    account = secret_data['account']
    user = secret_data['user']
    passphrase = secret_data['private_key_passphrase']
    default_warehouse = secret_data.get('default_warehouse', 'DEMO_WH').upper()
    default_database = secret_data.get('default_database', TDM_NAME).upper()
    default_schema = secret_data.get('default_schema', f'{TDM_SHORT_NAME}_STAGE').upper()

    private_key_value = secret_data['private_key_encrypted'].encode('utf-8')
    private_key = crypto_serialization.load_pem_private_key(
        data=private_key_value,
        password=passphrase.encode('utf-8'),
        backend=crypto_default_backend()
    )
    private_key_bytes = private_key.private_bytes(
        encoding=crypto_serialization.Encoding.DER,
        format=crypto_serialization.PrivateFormat.PKCS8,
        encryption_algorithm=crypto_serialization.NoEncryption()
    )

    connection_config = {
        "account": account,
        "database": default_database,
        "private_key": private_key_bytes,
        "region": 'us-west-2',
        "schema": default_schema,
        "user": user,
        "warehouse": default_warehouse
    }
    return connection_config


class SnowflakeConnectorHook(SnowflakeHook):

    def get_conn(self):
        """
        Returns a snowflake.connection object
        """
        try:
            conn_config = get_snowflake_connection_with_secrets_manager(
                get_boto_client(service_name='secretsmanager'))
        except (botocore.exceptions.NoCredentialsError, botocore.exceptions.ClientError) as ex:
            print(ex)
            print('Couldn''t connect to the data lake via AWS, trying sdm....')
            conn_config = get_local_snowflake_connection()
        print(conn_config)
        conn = snowflake.connector.connect(**conn_config)
        return conn

    def run(self, sql: Union[str, list], autocommit: bool = False, parameters: Optional[dict] = None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially (pulled from MWAA 2.0)
        :param sql: the sql string to be executed with possibly multiple statements,
          or a list of sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: dict or iterable
        """
        self.query_ids = []

        with self.get_conn() as conn:
            conn = self.get_conn()
            self.set_autocommit(conn, autocommit)

            if isinstance(sql, str):
                split_statements_tuple = split_statements(StringIO(sql))
                sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]

            self.log.debug("Executing %d statements against Snowflake DB", len(sql))
            with closing(conn.cursor()) as cur:
                for sql_statement in sql:

                    self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)
                    if parameters:
                        cur.execute(sql_statement, parameters)
                    else:
                        cur.execute(sql_statement)
                    self.log.info("Rows affected: %s", cur.rowcount)
                    self.log.info("Snowflake query id: %s", cur.sfqid)
                    self.query_ids.append(cur.sfqid)

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()


class SnowflakeConnectorOperator(SnowflakeOperator):

    def get_hook(self):
        """
        Returns a snowflake.hook object
        """
        return SnowflakeConnectorHook()




class SnowflakeConnectorPlugin(AirflowPlugin):
    name = "Snowflake_Connector_Plugin"
    operators = [SnowflakeConnectorOperator]
