from datetime import datetime

from shared.dag_factory import create_dag

from airflow.providers.postgres.operators.postgres import PostgresOperator

from shared.irontable import Irontable

import pendulum

start_date = pendulum.datetime(2023, 4, 4, tz="America/New_York") #datetime(2023, 4, 4)

dag = create_dag(
    "main_reporting",
    schedule="30 8 * * *",
    start_date=start_date,
    tags=["Transformation"]
)

reporting_schema = "fis"
staging_schema = "staging"


# DAGs with no DAG dependencies
cohorts = Irontable(schema=staging_schema,
                    table="cohorts",
                    other_dependencies=[
                        "service_catalog.cohorts",
                        "registrar.cohorts",
                        "course_conductor_deprecated.lms_cohorts",
                        "service_catalog.campuses",
                        "service_catalog.markets",
                        "service_catalog.course_offerings",
                        "service_catalog.disciplines",
                        "service_catalog.pacing_options",
                        "registrar.canvas_data"
                    ])
grades = Irontable(schema=reporting_schema,
                   table="grades",
                   other_dependencies=[
                       "learn.attempts",
                       "learn.users",
                       "learn.live_reviews",
                       "learn.assigned_repos",
                       "learn.batches",
                       "learn.code_challenges",
                       "learn.assignments_assignments",
                       "learn.assignments_tasks",
                       "learn.assignments_task_submissions",
                       "canvas.submission_dim",
                       "canvas.assignment_dim",
                       "canvas.assignment_group_dim",
                       "canvas.user_dim",
                       "canvas.pseudonym_dim",
                       "canvas.course_dim",
                       "canvas.account_dim"
                   ])
graduation_actions = Irontable(schema=staging_schema,
                               table="graduation_actions")
expiration_actions = Irontable(schema=staging_schema,
                               table="expiration_actions")
end_of_phase_grades = Irontable(schema=reporting_schema,
                                table="end_of_phase_grades",
                                dag_dependencies=[
                                    grades
                                ])
marketing_sources = Irontable(schema=staging_schema,
                              table="marketing_sources",
                              other_dependencies=[
                                  "stitch_marketo_production.leads",
                                  "stitch__hubspot.contacts",
                                  "stitch_new_google_ads.ad_performance_report",
                                  "personas_default.users"
                              ])
roster_changes = Irontable(schema=reporting_schema,
                           table="roster_changes",
                           dag_dependencies=[
                               cohorts
                           ],
                           other_dependencies=[
                               "registrar.admissions",
                               "registrar.registration_events",
                               "registrar.roster_actions",
                               "registrar.cohort_registrations"
                           ])
student_notes = Irontable(schema=reporting_schema,
                          table="student_notes",
                          other_dependencies=[
                              "learn.admin_student_notes",
                              "instructor_app_db.notes",
                              "instructor_app_db.note_types",
                              "instructor_app_db.students",
                              "instructor_app_db.users",
                              "learn.users",
                              "learn.user_roles",
                              "service_educator.notes",
                              "service_educator.categories"
                          ])
pathwright_progress = Irontable(schema=reporting_schema,
                                table="pathwright_progress",
                                other_dependencies=[
                                    "pathwright.activities",
                                    "learn.pathwright_accounts",
                                    "learn.users",
                                    "pathwright.activity_types",
                                    "pathwright.path_steps"
                                ])
canvas_roster = Irontable(schema=reporting_schema,
                          table="canvas_roster",
                          other_dependencies=[
                              "canvas.course_dim",
                              "canvas.enrollment_dim",
                              "canvas.user_dim",
                              "canvas.pseudonym_dim",
                              "canvas.course_section_dim",
                              "canvas.account_dim"
                           ])
canvas_lesson_feedback = Irontable(schema=reporting_schema,
                                   table="canvas_lesson_feedback",
                                   other_dependencies=[
                                       "fis.canvas_lesson_ratings",
                                       "formstack.canvas_feedback_submissions",
                                       "formstack.canvas_feedback_responses",
                                       "canvas.user_dim",
                                       "canvas.pseudonym_dim",
                                       "canvas.wiki_page_fact",
                                       "canvas.wiki_page_dim"
                                   ])

# DAGs with DAG dependencies
canvas_progress = Irontable(schema=reporting_schema,
                            table="canvas_progress",
                            dag_dependencies=[
                                canvas_roster,
                                canvas_lesson_feedback
                            ],
                            other_dependencies=[
                                "canvas.module_dim",
                                "canvas.module_item_dim",
                                "canvas.quiz_dim",
                                "canvas.assignment_dim",
                                "canvas.submission_dim",
                                "canvas.user_dim",
                                "canvas.discussion_topic_fact",
                                "canvas.discussion_entry_fact",
                                "canvas.discussion_entry_dim",
                                "canvas.module_progression_dim",
                                "canvas.module_progression_completion_requirement_dim"
                            ])
career_prep_progress = Irontable(schema=reporting_schema,
                                 table="career_prep_progress",
                                 dag_dependencies=[
                                     pathwright_progress
                                 ])
prework_progress = Irontable(schema=reporting_schema,
                             table="prework_progress",
                             dag_dependencies=[
                                 canvas_progress
                             ])
transfers = Irontable(schema=reporting_schema,
                      table="transfers",
                      dag_dependencies=[
                          cohorts
                      ],
                      other_dependencies=[
                          "registrar.roster_actions"
                      ])
leaves_of_absence = Irontable(schema=reporting_schema,
                              table="leaves_of_absence",
                              dag_dependencies=[
                                cohorts
                              ],
                              other_dependencies=[
                                  "registrar.leaves_of_absence",
                                  "registrar.admissions",
                                  "learn.users",
                                  "registrar.roster_actions"
                              ])
rosters = Irontable(schema=reporting_schema,
                    table="rosters",
                    dag_dependencies=[
                        cohorts,
                        graduation_actions,
                        expiration_actions,
                        roster_changes,
                        leaves_of_absence
                    ],
                    other_dependencies=[
                        "registrar.admissions",
                        "learn.users",
                        "registrar.registration_events",
                        "registrar.roster_actions",
                        "registrar.cohort_registrations",
                        "registrar.scheduled_starts",
                        "registrar.compliance_clearances",
                        "registrar.academic_clearances",
                        "registrar.payment_plans",
                        "registrar.payment_options",
                        "registrar.invoices",
                        "registrar.invoice_categories",
                        "stitch_salesforce.opportunity",
                        "stitch_salesforce.account",
                        "stitch_salesforce.user",
                        "registrar.tracking_events",
                        "registrar.external_ids",
                        "registrar.money_back_guarantee_access",
                        "registrar.cohort_registrations"
                    ])
admissions = Irontable(schema=reporting_schema,
                       table="admissions",
                       dag_dependencies=[
                           marketing_sources,
                           rosters,
                           prework_progress
                       ],
                       other_dependencies=[
                           "stitch_salesforce.application__c",
                           "stitch_salesforce.lead",
                           "stitch_salesforce.opportunity",
                           "stitch_salesforce.opportunityhistory",
                           "stitch_salesforce.account",
                           "stitch_salesforce.contact",
                           "stitch_salesforce.form__c",
                           "stitch_salesforce.cohort__c",
                           "stitch_salesforce.user",
                           "data_analytics.campus_zips",
                           "data_analytics.zip_to_msa"
                       ])
daily_student_paces = Irontable(schema=reporting_schema,
                                table="daily_student_paces",
                                dag_dependencies=[
                                    rosters
                                ],
                                other_dependencies=[
                                    "service_milestones.events",
                                    "service_milestones.pacing_templates"
                                ])

marketing_events = Irontable(schema=reporting_schema,
                             table="marketing_events",
                             dag_dependencies=[
                                 admissions
                             ],
                             other_dependencies=[
                                 "stitch_salesforce.campaign",
                                 "stitch_salesforce.campaignmember",
                                 "eventbrite.events",
                                 "eventbrite.attendees",
                                 "splash.events",
                                 "splash.contacts",
                                 "go_to_webinar.webinars",
                                 "go_to_webinar.registrants",
                                 "go_to_webinar.attendees"
                             ])
stripe_transactions = Irontable(schema=reporting_schema,
                                table="stripe_transactions",
                                dag_dependencies=[
                                    rosters
                                ],
                                other_dependencies=[
                                    "stitch_stripe.charges",
                                    "stitch_stripe.invoices",
                                    "stitch_stripe.customers__cards",
                                    "stitch_stripe.subscriptions",
                                    "stitch_stripe.invoice_line_items",
                                    "stitch_stripe.balance_transactions",
                                    "stitch_stripe.customers",
                                    "learn.users",
                                    "registrar_events.invoice_created",
                                    "registrar.external_ids"
                                ])
non_most_recent = {
    'deposit_amount_due': {},
    'deposit_paid_date': {},
    'enrollment_agreement_cleared': {},
    'enrollment_agreement_cleared_date': {},
    'financially_cleared': {},
    'financially_cleared_date': {},
    'academically_cleared_date': {},
    'financing_method': {},
    'financing_method_amount': {},
    'gross_tuition_amount': {},
    'tuition_amount_paid': {},
    'financing_method_status': {},
    'financing_method_status_approved_by': {},
    'financing_method_status_approved_date': {},
    'financing_partner': {},
    'financial_standing': {},
    'marked_delinquent_date': {},
    'opportunity_id' : {'alias': 'most_recent_opportunity_id'},
    'onboarding_agent_name': {'alias': 'most_recent_onboarding_agent'},
    'account_id': {'alias': 'most_recent_account_id'},
    'scheduled_start_date': {'sql': 'most_recent_cohort.cohort_start_date AS most_recent_day_1_start_date'},
    'removed_from_cohort_date': {'alias': 'most_recent_cohort_cancel_date'},
    'expiration_date': {},
    'expiration_updated_at': {},
    'student_status': {},
    'mbg_program': {},
    'added_to_roster_action_type': {},
    'added_to_roster_action_change': {},
    'added_to_roster_action_module': {},
    'added_to_roster_action_details': {},
    'added_to_roster_action_reasons': {},
    'removed_from_roster_action_type': {},
    'removed_from_roster_action_change': {},
    'removed_from_roster_action_module': {},
    'removed_from_roster_action_details': {},
    'removed_from_roster_action_reasons': {},
    'loa_count': {},
    'most_recent_loa_start_date': {},
    'most_recent_loa_end_date': {},
    'graduation_date': {'sql': 'NVL(most_recent_cohort.graduation_date, historical_graduations.graduated_at) AS graduation_date'},
    'graduation_updated_at': {'sql': 'NVL(most_recent_cohort.graduation_updated_at, historical_graduations.updated_at) AS graduation_updated_at'}
}

students = Irontable(schema=reporting_schema,
                     table="students",
                     dag_dependencies=[
                         admissions,
                         cohorts,
                         prework_progress,
                         end_of_phase_grades,
                         rosters,
                         student_notes
                     ],
                     other_dependencies=[
                         "learn.users",
                         "student_home.students",
                         "learn.profiles",
                         "registrar.student_institutions",
                         "service_catalog.institutions",
                         "learn.job_searches",
                         "learn.user_markers",
                         "learn.identities_learn_accounts",
                         "learn.identities_github_accounts",
                         "learn.compliance_requirements",
                         "learn.compliance_documents",
                         "learn.compliance_document_kinds",
                         "registrar_events.identity_verification_request_sent",
                         "registrar_events.identity_verification_succeeded",
                         "registrar.registration_events",
                         "registrar.external_ids",
                         "registrar.historical_graduations",
                         "service_milestones.student_pace_selections",
                         "service_milestones.pacing_templates",
                         "fis.service_educator_versions",
                         "learn.graduations",
                         "stripe.invoices",
                         "stripe.customers"
                     ])
alumni = Irontable(schema=reporting_schema,
                   table="alumni",
                   dag_dependencies=[
                       students,
                       career_prep_progress
                   ],
                   other_dependencies=[
                       "career_services.job_search_feedback"
                   ])
daily_active_rosters = Irontable(schema=reporting_schema,
                                 table="daily_active_rosters",
                                 dag_dependencies=[
                                     rosters,
                                     students,
                                     daily_student_paces
                                 ],
                                 other_dependencies=[
                                     "fis.daily_student_instructor"
                                 ])
admissions_assessment = Irontable(schema=reporting_schema,
                                  table="admissions_assessment",
                                  dag_dependencies=[
                                      admissions,
                                      students
                                  ],
                                  other_dependencies=[
                                      "stitch_salesforce.form__c",
                                      "stitch_salesforce.recordtype"
                                  ])
rep_activities = Irontable(schema=reporting_schema,
                           table="rep_activities",
                           other_dependencies=[
                               "stitch_salesforce.task",
                               "stitch_salesforce.user"
                           ])
funnel_activity = Irontable(schema=reporting_schema,
                            table="funnel_activity",
                            dag_dependencies=[
                                admissions,
                                rep_activities
                            ],
                            other_dependencies=[
                                "stitch_salesforce.lead",
                                "stitch_salesforce.account"
                            ])

dag_dict = {
    "cohorts": cohorts.to_dict(),
    "grades": grades.to_dict(),
    "graduation_actions": graduation_actions.to_dict(),
    "expiration_actions": expiration_actions.to_dict(),
    "end_of_phase_grades": end_of_phase_grades.to_dict(),
    "leaves_of_absence": leaves_of_absence.to_dict(),
    "marketing_sources": marketing_sources.to_dict(),
    "roster_changes": roster_changes.to_dict(),
    "student_notes": student_notes.to_dict(),
    "pathwright_progress": pathwright_progress.to_dict(),
    "canvas_roster": canvas_roster.to_dict(),
    "canvas_lesson_feedback": canvas_lesson_feedback.to_dict(),
    "canvas_progress": canvas_progress.to_dict(),
    "career_prep_progress": career_prep_progress.to_dict(),
    "prework_progress": prework_progress.to_dict(),
    "transfers": transfers.to_dict(),
    "rosters": rosters.to_dict(),
    "admissions": admissions.to_dict(),
    "daily_student_paces": daily_student_paces.to_dict(),
    "marketing_events": marketing_events.to_dict(),
    "stripe_transactions": stripe_transactions.to_dict(),
    "students": students.to_dict(
        non_most_recent=non_most_recent,
        roster_columns=rosters.query_column_names()
        ),
    "alumni": alumni.to_dict(),
    "daily_active_rosters": daily_active_rosters.to_dict(),
    "admissions_assessment": admissions_assessment.to_dict(),
    "rep_activities": rep_activities.to_dict(),
    "funnel_activity": funnel_activity.to_dict()
}

for table, table_params in dag_dict.items():
    sql = f"{table}.sql"

    table_op = PostgresOperator(
        dag=dag,
        task_id=table,
        start_date=start_date,
        postgres_conn_id="redshift_default",
        sql=sql,
        params=table_params,
        autocommit=True)

    for dag_dep in dag_dict[table]["dag_dependencies"]:
        dag_dict[dag_dep]["operator"] >> table_op

    table_params["operator"] = table_op

