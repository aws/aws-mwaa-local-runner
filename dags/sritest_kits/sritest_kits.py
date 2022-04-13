#Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example dag for using `RedshiftSQLOperator` to authenticate with Amazon Redshift
then execute a simple select statement
"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os
from airflow.operators.dummy import DummyOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.sensors.sql_sensor import SqlSensor
from util.sql_utils import upsert
CUR_DIR = os.path.abspath(os.path.dirname(__file__))
hodor_conn_id = "redshift_hodor_staging"
postgres_connection_id = "redshift_hodor"

default_args = {
    'owner': 'BI',
    'depends_on_past': False,
    'email_on_succcess': True,
    'email_on_retry': True,
    'email_on_failure': True,
    'email': ['sri.varatharajan@helix.com'],
    'retries': 3,
}

with DAG(
    dag_id="sritest_kits", 
    default_args = default_args,
    schedule_interval = '0 * * * *',
    start_date = datetime(2022, 3, 1),
    catchup = False,
    max_active_runs=1,
    dagrun_timeout = timedelta(minutes=60),
    tags = ['kits'],
    params = {"example_key": "example_value"},
    # template_searchpath = '/usr/local/airflow/dags',
) as dag:
    

    
    # build all individual tables

    build_genomic_order_stage = PostgresOperator(
        task_id='build_genomic_order_stage',
        postgres_conn_id=postgres_connection_id,
        sql = upsert(
            target_schema = 'sritest',
            target_table_name = 'genomic_order_stage',
            select_clause = open(f"{CUR_DIR}/sql/genomic_order_stage.sql").read(),
        ),
        params={"table": "sritest.kits"}
    ) 

    build_genomic_patient_stage = PostgresOperator(
        task_id='build_genomic_patient_stage',
        postgres_conn_id=postgres_connection_id,
        sql = upsert(
            target_schema = 'sritest',
            target_table_name = 'genomic_patient_stage',
            select_clause = open(f"{CUR_DIR}/sql/genomic_patient_stage.sql").read(),
        ),
        params={"table": "sritest.kits"}
    ) 
   
    build_accessioning_scans_stage = PostgresOperator(
        task_id='build_accessioning_scans_stage',
        postgres_conn_id=postgres_connection_id,
        sql = upsert(
            target_schema = 'sritest',
            target_table_name = 'accessioning_scans_stage',
            select_clause = open(f"{CUR_DIR}/sql/accessioning_scans_stage.sql").read(),
        ),
        params={"table": "sritest.kits"}
    ) 
    build_myhealth_workflow_custom = PostgresOperator(
        task_id='build_myhealth_workflow_custom',
        postgres_conn_id=postgres_connection_id,
        sql = upsert(
            target_schema = 'sritest',
            target_table_name = 'myhealth_workflow_custom',
            select_clause = open(f"{CUR_DIR}/sql/myhealth_workflow_custom.sql").read(),
        ),
        params={"table": "sritest.kits"}
    ) 

    build_MHWF_stage = PostgresOperator(
        task_id='build_MHWF_stage',
        postgres_conn_id=postgres_connection_id,
        sql = upsert(
            target_schema = 'sritest',
            target_table_name = 'mhwf_stage',
            select_clause = open(f"{CUR_DIR}/sql/MHWF_stage.sql").read(),
        ),
        params={"table": "sritest.kits"}
    ) 
   # update the Overall Kits table
    Update_kits_all = PostgresOperator(
        task_id='Update_kits_all',
        postgres_conn_id=postgres_connection_id,
        sql = upsert(
            target_schema = 'sritest',
            target_table_name = 'kits_all',
            select_clause = open(f"{CUR_DIR}/sql/Update_kits_all.sql").read(),
        ),
        params={"table": "sritest.kits"}
    ) 
    # Check Operators for individual tables
    check_MHWF_stage = SQLCheckOperator(
        task_id = 'check_MHWF_stage',
        conn_id = 'redshift_hodor',
        sql = open(f"{CUR_DIR}/sql/check_myhealth_workflow.sql").read(),
        params={"table": "sritest.mhwf_stage"}
    )

    check_genomic_order_stage = SQLCheckOperator(
        task_id = 'check_genomic_order_stage',
        conn_id = 'redshift_hodor',
        sql = open(f"{CUR_DIR}/sql/check_genomic_order.sql").read(),
        params={"table": "sritest.genomic_order_stage"}
    )
    check_accessioning_scans_stage = SQLCheckOperator(
        task_id = 'check_accessioning_scans_stage',
        conn_id = 'redshift_hodor',
        sql = open(f"{CUR_DIR}/sql/check_accessioning_scans.sql").read(),
        params={"table": "sritest.accessioning_scans_stage"}
    )
    check_genomic_patient_stage = SQLCheckOperator(
        task_id = 'check_genomic_patient_stage',
        conn_id = 'redshift_hodor',
        sql = open(f"{CUR_DIR}/sql/check_genomic_patient.sql").read(),
        params={"table": "sritest.genomic_patient_stage"}
    )
    check_kits_all = SQLCheckOperator(
        task_id = 'check_kits_all',
        conn_id = 'redshift_hodor',
        sql = open(f"{CUR_DIR}/sql/check_kits_all.sql").read(),
        params={"table": "sritest.kits_all"}
    )


    [build_genomic_order_stage ,build_genomic_patient_stage, build_accessioning_scans_stage,(build_myhealth_workflow_custom >> build_MHWF_stage) ]>> Update_kits_all
    build_genomic_order_stage >> check_genomic_order_stage
    build_genomic_patient_stage >> check_genomic_patient_stage
    build_accessioning_scans_stage >> check_accessioning_scans_stage
    build_MHWF_stage >> check_MHWF_stage
    Update_kits_all >> check_kits_all
    
    
# [END redshift_operator_howto_guide]