from shared.dag_factory import create_dag
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.models.baseoperator import chain

from datetime import datetime
from shared.irondata import Irondata
from shared.irontable import Irontable
from redshift_to_sfdc.sfdc_updater import bulk_update_sfdc


start_date = datetime(2023, 4, 4)

dag = create_dag(
    "redshift_to_sfdc",
    schedule="0 * * * *",
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    description="Use data in Redshift to update an object in Salesforce"
)

sfdc_params = {
    # Query name: object.field
    "accounts": "id",
    "contacts": "id",
    "opportunities": "id",
    "climb_application__c": "climb_id__c"
    # "proofs_of_hs": "account.high_school_graduation_confirmed__c",
    # "academic_clearances": "opportunity.academically_cleared__c",
    # "deposit_invoice_links": "contact.student_facing_enroll_link__c",
    # "student_statuses": "account.student_status__c"
}

for sfdc_object, sfdc_object_id in sfdc_params.items():

    table = Irontable(schema="staging",
                      table=sfdc_object)

    s3_key = f"{table.schema_in_env}/{{{{ds}}}}/{{{{macros.ds_format(ts_nodash, '%Y%m%dT%H%M%S', '%H%M')}}}}"

    reset_table_op = PostgresOperator(
        dag=dag,
        task_id=f"{sfdc_object}_reset",
        sql=f"{sfdc_object}.sql",
        params={
            "schema": table.schema_in_env,
            "table": table.table_in_env
        },
        postgres_conn_id="redshift",
        autocommit=True
    )

    redshift_to_s3_op = RedshiftToS3Operator(
        dag=dag,
        task_id=f"{sfdc_object}_redshift_to_s3",
        schema=table.schema_in_env,
        table=table.table_in_env,
        s3_bucket=Irondata.s3_warehouse_bucket(),
        s3_key=s3_key,
        redshift_conn_id="redshift",
        aws_conn_id="aws_default",
        autocommit=True,
        unload_options=["JSON", "allowoverwrite", "parallel off"]
    )

    s3_to_sfdc_op = PythonOperator(
        dag=dag,
        task_id=f"{sfdc_object}_s3_to_sfdc",
        python_callable=bulk_update_sfdc,
        op_kwargs={
            "bucket_name": Irondata.s3_warehouse_bucket(),
            "object_key": f"{s3_key}/{table.table_in_env}_000.json",
            "sfdc_object": sfdc_object,
            "sfdc_object_id": sfdc_object_id
        },
        provide_context=True
    )

    chain(reset_table_op, redshift_to_s3_op, s3_to_sfdc_op)
