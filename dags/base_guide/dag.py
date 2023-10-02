from datetime import datetime

from shared.dag_factory import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

from shared.irontable import Irontable

import pendulum

start_date = pendulum.datetime(2023, 4, 4, tz="America/New_York")

dag = create_dag(
    "base_guide",
    catchup=True,
    schedule="30 8 * * *",  # This is 8:30am local (EST)
    start_date=start_date,
    tags=["Transformation"]
)

cta_rulesets = Irontable(schema="fis",
                         table="cta_rulesets")

cta_events = Irontable(schema="fis",
                       table="cta_events")

cta_users = Irontable(schema="fis",
                      table="cta_users",
                      dag_dependencies=[
                          cta_events
                      ])

cta_matches = Irontable(schema="fis",
                        table="cta_matches",
                        dag_dependencies=[
                            cta_rulesets,
                            cta_users
                        ])

discover_content = Irontable(schema="fis",
                             table="discover_content",
                             other_dependencies=[
                                 "service_content.contents",
                                 "service_content.contents_institutions",
                                 "service_content.institutions",
                                 "service_content.contents_student_statuses",
                                 "service_content.student_statuses",
                                 "service_content.contents_program_disciplines",
                                 "service_content.program_disciplines",
                                 "service_content.contents_program_phases",
                                 "service_content.program_phases",
                                 "service_content.contents_program_modules",
                                 "service_content.program_modules",
                                 "service_content.contents_tags",
                                 "service_content.tags"
                             ])

zoom_attendees = Irontable(schema="fis",
                           table="zoom_attendees",
                           other_dependencies=[
                               "stitch_zoom.report_meeting_participants",
                               "learn.users"
                           ])

zoom_meetings = Irontable(schema="fis",
                          table="zoom_meetings",
                          dag_dependencies=[
                               zoom_attendees
                          ],
                          other_dependencies=[
                              "stitch_zoom.meetings",
                              "stitch_zoom.report_meetings"
                          ])

live_events = Irontable(schema="fis",
                        table="live_events",
                        dag_dependencies=[
                            zoom_meetings
                        ],
                        other_dependencies=[
                                "service_content.live_events",
                                "service_content.live_events_program_disciplines",
                                "service_content.program_disciplines",
                                "service_content.live_events_program_phases",
                                "service_content.program_phases",
                                "service_content.live_events_program_modules",
                                "service_content.program_modules",
                                "service_content.institutions_live_events",
                                "service_content.institutions",
                                "service_content.live_event_locations",
                                "service_content.locations",
                                "service_content.live_events_live_event_types",
                                "service_content.live_event_types"
                        ])

live_event_attendees = Irontable(schema="fis",
                                 table="live_event_attendees",
                                 dag_dependencies=[
                                     live_events,
                                     zoom_attendees
                                 ])

base_profile = Irontable(schema="fis",
                         table="base_profile")

service_educator_versions = Irontable(schema="fis",
                                      table="service_educator_versions")

daily_student_instructor = Irontable(schema="fis",
                                     table="daily_student_instructor",
                                     dag_dependencies=[
                                         service_educator_versions
                                     ])

service_operations_versions = Irontable(schema="fis",
                                        table="service_operations_versions")

service_operations_fact = Irontable(schema="fis",
                                    table="service_operations_fact",
                                    dag_dependencies=[
                                        service_operations_versions
                                    ])

dag_dict = {
    "cta_rulesets": cta_rulesets.to_dict(),
    "cta_events": cta_events.to_dict(),
    "cta_users": cta_users.to_dict(),
    "cta_matches": cta_matches.to_dict(),
    "discover_content": discover_content.to_dict(),
    "zoom_attendees": zoom_attendees.to_dict(),
    "zoom_meetings": zoom_meetings.to_dict(),
    "live_events": live_events.to_dict(),
    "live_event_attendees": live_event_attendees.to_dict(),
    "base_profile": base_profile.to_dict(),
    "service_educator_versions": service_educator_versions.to_dict(),
    "daily_student_instructor": daily_student_instructor.to_dict(),
    "service_operations_versions": service_operations_versions.to_dict(),
    "service_operations_fact": service_operations_fact.to_dict()
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
