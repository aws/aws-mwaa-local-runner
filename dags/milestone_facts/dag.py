from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from shared.irondata import Irondata
from shared.irontable import Irontable
from milestone_facts.helpers import sql_templates_path, load_docs
from shared.alerting import dag_success_handler, dag_failure_handler, task_failure_handler

start_date = datetime(2022, 4, 4)

default_args = {
    'owner': "@data-education",
    'depends_on_past': False,
    'start_date': start_date,
    'email': ['data-engineering@flatironschool.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "on_failure_callback": task_failure_handler,
    "params": {
        "test_env": Irondata.is_test()
    }
}

dag = DAG(
    "milestone_fact",
    catchup=True,
    default_args=default_args,
    template_searchpath=[
        Irondata.sql_templates_path(),
        sql_templates_path()
    ],
    schedule_interval='@daily',
    on_success_callback=dag_success_handler,
    on_failure_callback=dag_failure_handler,
    tags=["Transformation"]
)

dag.doc_md = load_docs()

milestone_fact = Irontable(schema="fis",
                           table="milestone_fact",
                           other_dependencies=[
                               "service_milestones.milestones",
                               "service_milestones.milestone_templates",
                               "service_milestones.programs",
                               "service_milestones.pacing_templates",
                               "service_milestones.milestone_estimates",
                               "service_milestones.milestones_canvas_milestones",
                               "service_milestones.canvas_module_items",
                               "service_milestones.canvas_modules",
                               "service_milestones.canvas_courses",
                               "registrar.courses",
                               ])

milestone_student_schedules = Irontable(schema="fis",
                                        table="milestone_student_schedules",
                                        dag_dependencies=[milestone_fact],
                                        other_dependencies=[
                                            "service_milestones.pacing_templates",
                                            "service_milestones.student_pace_selections",
                                        ])

milestone_student_fact = Irontable(schema="fis",
                                   table="milestone_student_fact",
                                   dag_dependencies=[
                                       milestone_student_schedules,
                                       milestone_fact,
                                       ],
                                   other_dependencies=[
                                       "service_milestones.student_pace_selections",
                                       "service_milestones.pacing_templates",
                                       ])

milestone_canvas_mappings = Irontable(
    schema='fis',
    table='milestone_canvas_mappings',
    other_dependencies=[
        'service_milestones.canvas_courses',
        'service_milestones.canvas_course_mappings',
        'service_milestones.canvas_modules',
        'service_milestones.canvas_module_items',
        'service_milestones.canvas_module_item_mappings',
        'service_milestones.milestones_canvas_milestones',
        'service_milestones.milestones',
        ]
    )

milestone_canvas_module_items = Irontable(
    schema='fis',
    table='milestone_canvas_module_items',
    dag_dependencies=[milestone_canvas_mappings]
    )


dag_dict = {
    "milestone_fact": milestone_fact.to_dict(),
    "milestone_student_schedules": milestone_student_schedules.to_dict(),
    "milestone_student_fact": milestone_student_fact.to_dict(),
    "milestone_canvas_mappings": milestone_canvas_mappings.to_dict(),
    "milestone_canvas_module_items": milestone_canvas_module_items.to_dict()
}

for table, table_params in dag_dict.items():

    table_op = PostgresOperator(
        dag=dag,
        task_id=table,
        sql=f"{table}.sql",
        params=table_params,
        start_date=start_date,
        postgres_conn_id="postgres_default",
        autocommit=True)

    for dag_dep in dag_dict[table]["dag_dependencies"]:
        dag_dict[dag_dep]["operator"] >> table_op

    table_params["operator"] = table_op
