from datetime import datetime

from shared.dag_factory import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

from shared.irontable import Irontable

start_date = datetime(2023, 4, 4)

dag = create_dag(
    "aaq",
    schedule="0 6 * * *",  # This is 6:00am local
    start_date=start_date,
    catchup=True,
    tags=["Transformation"]
)

zendesk_tickets = Irontable(schema="fis",
                            table="zendesk_tickets",
                            other_dependencies=[
                                "zendesk.tickets",
                                "zendesk.users"
                            ])

zendesk_aaq_curriculums = Irontable(schema="fis",
                                    table="zendesk_aaq_curriculums",
                                    other_dependencies=[
                                        "zendesk.tickets",
                                        "staging.curriculum_order",
                                        "learn.curriculums"
                                    ])

zendesk_aaq_transcripts = Irontable(schema="fis",
                                    table="zendesk_aaq_transcripts",
                                    other_dependencies=[
                                        "zendesk.ticket_comments"
                                    ])

zendesk_aaq_facts = Irontable(schema="fis",
                              table="zendesk_aaq_facts",
                              dag_dependencies=[
                                  zendesk_aaq_transcripts
                              ])

zendesk_tc_ticket_forms = Irontable(schema="fis",
                                    table="zendesk_tc_ticket_forms",
                                    other_dependencies=[
                                        "zendesk.tickets",
                                        "zendesk.ticket_forms",
                                        "zendesk.ticket_forms__ticket_field_ids",
                                        "zendesk.ticket_fields",
                                        "zendesk.tickets__custom_fields"
                                    ])

zendesk_ss_ticket_forms = Irontable(schema="fis",
                                    table="zendesk_ss_ticket_forms"
                                    )

zendesk_student_segments = Irontable(schema="fis",
                                     table="zendesk_student_segments",
                                     dag_dependencies=[
                                         zendesk_tickets,
                                         zendesk_ss_ticket_forms
                                     ],
                                     other_dependencies=[
                                         "fis.students",
                                         "fis.rosters",
                                         "fis.alumni"
                                     ])

combined_aaq_tickets = Irontable(schema="fis",
                                 table="combined_aaq_tickets",
                                 dag_dependencies=[
                                     zendesk_tickets,
                                     zendesk_aaq_facts,
                                     zendesk_aaq_curriculums,
                                 ])

combined_aaq_transcripts = Irontable(schema="fis",
                                     table="combined_aaq_transcripts",
                                     dag_dependencies=[
                                         zendesk_aaq_transcripts,
                                     ])


dag_dict = {
    "zendesk_tickets": zendesk_tickets.to_dict(),
    "zendesk_aaq_curriculums": zendesk_aaq_curriculums.to_dict(),
    "zendesk_aaq_transcripts": zendesk_aaq_transcripts.to_dict(),
    "zendesk_aaq_facts": zendesk_aaq_facts.to_dict(),
    "zendesk_tc_ticket_forms": zendesk_tc_ticket_forms.to_dict(),
    "zendesk_ss_ticket_forms": zendesk_ss_ticket_forms.to_dict(),
    "zendesk_student_segments": zendesk_student_segments.to_dict(),
    "combined_aaq_tickets": combined_aaq_tickets.to_dict(),
    "combined_aaq_transcripts": combined_aaq_transcripts.to_dict()
}

for table, table_params in dag_dict.items():

    table_op = PostgresOperator(
        dag=dag,
        task_id=table,
        sql=f"{table}.sql",
        params=table_params,
        start_date=start_date,
        autocommit=True)

    for dag_dep in dag_dict[table]["dag_dependencies"]:
        dag_dict[dag_dep]["operator"] >> table_op

    table_params["operator"] = table_op
