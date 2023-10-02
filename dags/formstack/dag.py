import pendulum
from shared.dag_factory import create_dag
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from shared.operators.s3_to_redshift_staging_operator import S3ToRedshiftStagingTransfer
from datetime import datetime

from shared.irondata import Irondata
from shared.irontable import Irontable
from formstack.formstack_to_s3 import formstack_extract, move_processed_files

local_tz = pendulum.timezone('America/New_York')
start_date = datetime(2023, 4, 4, tzinfo=local_tz)

dag = create_dag(
    "formstack",
    schedule="0 5 * * *",  # This is 5:00am local
    start_date=start_date,
    catchup=True if Irondata.is_production() else False,
    tags=["API"]
)

NUMBER_OF_DAYS_TO_FETCH = 14
SCHEMA = "formstack"
FORMSTACK_FORMS = {
    # table_alias: form_id #
    # "canvas_feedback": "3964005",
    "end_of_phase_1": "4621129",
    "end_of_phase_2": "4622945",
    "end_of_phase_3": "4622948",
    "end_of_phase_4": "4622947",
    "end_of_phase_5": "4622949",
    "community_event": "4630513",
    "career_preferences": "4775768",
    "career_preferences_alumni": "4937441",
    "career_prep": "4630149",
    "career_workshop": "4670748",
    "job_search_day30": "4630156",
    "job_search_day90": "4647311",
    "job_search_day120": "4725660",
    "end_of_job_search": "4670641",
    "job_placement": "4630162",
    "job_details": "4770099",
    "linkedin_resume_rubric": "4943934",
    "amazon_career_choice_phase_1": "4937332",
    "amazon_career_choice_career_preferences": "4937352",
    "enterprise_end_of_phase": "4826020",
    "enterprise_end_of_phase_nps": "4981812",
    "enterprise_end_of_program": "4826139",
    "student_intake": "5092666",
    "ask_ada": "5427795"
}

merge_ops = []

for form_name, form_id in FORMSTACK_FORMS.items():
    submissions_table = Irontable(schema=SCHEMA, table=f"{form_name}_submissions", primary_key="id")
    responses_table = Irontable(schema=SCHEMA, table=f"{form_name}_responses", primary_key="submission_id")
    merged_table = Irontable(schema=SCHEMA, table=f"{form_name}_merged")

    FORM_TABLES = {
        "submissions": submissions_table,
        "responses": responses_table
    }

    formstack_to_s3_op = ShortCircuitOperator(
        dag=dag,
        task_id=f"formstack_to_s3__{form_name}",
        python_callable=formstack_extract,
        op_kwargs={
            "bucket": Irondata.s3_warehouse_bucket(),
            "form_id": form_id,
            "submissions_table": submissions_table,
            "responses_table": responses_table,
            "days": NUMBER_OF_DAYS_TO_FETCH,
        },
        ignore_downstream_trigger_rules=False,
        provide_context=True)
    
    merge_tables = PostgresOperator(
            dag=dag,
            task_id=f"merge_data_{form_name}",
            sql="merge_table.sql",
            params={
                "schema": merged_table.schema_in_env,
                "table": merged_table.table_in_env,
                "submissions_table": submissions_table,
                "responses_table": responses_table
            },
            postgres_conn_id="redshift",
            autocommit=True
        )

    for entity_type, entity in FORM_TABLES.items():
        create_table_op = PostgresOperator(
            dag=dag,
            task_id=f"create_{entity.table}_table",
            sql=f"create_{entity_type}_table.sql",
            params={
                "schema": entity.schema_in_env,
                "table": entity.table_in_env
            },
            postgres_conn_id="redshift_default",
            autocommit=True)
        
        copy_data = S3ToRedshiftOperator(
            dag=dag,
            task_id=f"copy_{entity.table}_to_staging",
            schema=entity.schema_in_env,
            table=entity.table_in_env,
            s3_bucket=Irondata.s3_warehouse_bucket(),
            s3_key= entity.schema_in_env + "/unprocessed/" + form_id + f'/{entity_type}',
            copy_options=["CSV"],
            method="UPSERT",
            upsert_keys=[entity.primary_key]
        )

        formstack_to_s3_op >> create_table_op \
            >> copy_data >>  merge_tables
        
        merge_ops.append(merge_tables)

move_processed_files = PythonOperator(
    dag=dag,
    task_id="move_processed_files",
    python_callable=move_processed_files,
    op_kwargs={
        "form_names": FORMSTACK_FORMS.keys(),
        "bucket_name": Irondata.s3_warehouse_bucket(),
        },
    trigger_rule="all_done",
    )

merge_ops >> move_processed_files