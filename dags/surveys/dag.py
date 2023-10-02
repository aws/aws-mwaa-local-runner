import pendulum
from pathlib import Path
from shared.dag_factory import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Future work may trigger these based on formstack_etl DAG
# from airflow.sensors.external_task import ExternalTaskSensor

from shared.irontable import Irontable

start_date = pendulum.datetime(2023, 4, 4, tz="America/New_York")

dag = create_dag(
    "surveys",
    schedule="0 9 * * *",
    start_date=start_date,
    catchup=False,
    max_active_runs=1,
    tags=["Transformation"]
)

SCHEMA="surveys"

historic_nps = Irontable(schema=SCHEMA,
                         table="historic_nps",
                         other_dependencies=[
                             "formstack.nps_submissions_deprecated",
                             "formstack.nps_responses_deprecated"
                         ])

end_of_phase = Irontable(schema=SCHEMA,
                         table="csat_end_of_phase",
                         other_dependencies=[
                             "formstack.end_of_phase_1_merged",
                             "formstack.end_of_phase_2_merged",
                             "formstack.end_of_phase_3_merged",
                             "formstack.end_of_phase_4_merged",
                             "formstack.end_of_phase_5_merged"
                         ])

community_event = Irontable(schema=SCHEMA,
                            table="csat_community_event",
                            other_dependencies=[
                                "formstack.community_event_merged"
                            ])

job_search = Irontable(schema=SCHEMA,
                       table="csat_job_search",
                       other_dependencies=[
                           "formstack.career_prep_merged",
                           "formstack.career_workshop_merged",
                           "formstack.job_search_day30_merged",
                           "formstack.job_search_day90_merged",
                           "formstack.job_search_day120_merged",
                           "formstack.end_of_job_search_merged",
                           "formstack.job_placement_merged"
                       ])

enterprise = Irontable(schema=SCHEMA,
                       table="csat_enterprise",
                       other_dependencies=[
                           "formstack.enterprise_end_of_phase_merged",
                           "formstack.enterprise_end_of_phase_nps_merged",
                           "formstack.enterprise_end_of_program_merged"
                       ])

csat_all = Irontable(schema=SCHEMA,
                     table="csat_all",
                     dag_dependencies=[
                         end_of_phase,
                         community_event,
                         job_search,
                         historic_nps,
                         enterprise
                     ],
                     other_dependencies=[
                         "fis.service_educator_versions",
                         "learn.users",
                         "fis.rosters",
                         "gradleaders.change_log",
                         "fis.students"
                     ]) 

linkedin_resume_rubric = Irontable(schema=SCHEMA,
                                   table="linkedin_resume_rubric",
                                   other_dependencies=[
                                       "formstack.linkedin_resume_rubric_merged"
                                   ])

career_preferences = Irontable(schema=SCHEMA,
                               table="career_preferences",
                               other_dependencies=[
                                   "formstack.career_preferences_merged",
                                   "formstack.career_preferences_alumni_merged",
                                   "formstack.amazon_career_choice_phase_1_merged",
                                   "formstack.amazon_career_choice_career_preferences_merged"
                               ])

ask_ada = Irontable(schema=SCHEMA,
                    table="ask_ada")

# DAGs with no DAG dependencies
formstack_job_details = Irontable(schema=SCHEMA,
                                  table="formstack_job_details",
                                  other_dependencies=[
                                      "formstack.job_details_merged"
                                  ])

combined_job_details = Irontable(schema=SCHEMA,
                                 table="combined_job_details",
                                 dag_dependencies=[
                                     formstack_job_details
                                 ])


dag_dict = {
    "historic_nps": historic_nps.to_dict(),
    "csat_end_of_phase": end_of_phase.to_dict(),
    "csat_community_event": community_event.to_dict(),
    "csat_job_search": job_search.to_dict(),
    "csat_enterprise": enterprise.to_dict(),
    "csat_all": csat_all.to_dict(),
    "linkedin_resume_rubric": linkedin_resume_rubric.to_dict(),
    "career_preferences": career_preferences.to_dict(),
    "ask_ada": ask_ada.to_dict(),
    "formstack_job_details": formstack_job_details.to_dict(),
    "combined_job_details": combined_job_details.to_dict()
}

for table, table_params in dag_dict.items():
    sql = f"{table}.sql"

    table_op = PostgresOperator(
        dag=dag,
        task_id=table,
        start_date=start_date,
        postgres_conn_id="redshift",
        sql=sql,
        params=table_params,
        autocommit=True)

    for dag_dep in dag_dict[table]["dag_dependencies"]:
        dag_dict[dag_dep]["operator"] >> table_op

    table_params["operator"] = table_op
