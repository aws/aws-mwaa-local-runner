{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    roster_actions.admission_uuid
    , roster_actions.source_exit_date
    , roster_actions.destination_enter_date
    , roster_actions.type
    , roster_actions.change
    , roster_actions.status
    , JSON_EXTRACT_PATH_TEXT(roster_actions.data, 'module') AS module
    , JSON_EXTRACT_PATH_TEXT(roster_actions.data, 'details') AS details
    , JSON_EXTRACT_PATH_TEXT(roster_actions.data, 'reasons') AS reasons
    , source_cohort.name AS source_cohort_name
    , source_cohort.start_date AS source_cohort_start_date
    , source_cohort.end_date AS source_cohort_end_date
    , source_cohort.campus AS source_cohort_campus
    , source_cohort.modality AS source_cohort_modality
    , source_cohort.discipline AS source_cohort_discipline
    , source_cohort.pacing AS source_cohort_pacing
    , destination_cohort.name AS destination_cohort_name
    , destination_cohort.start_date AS destination_cohort_start_date
    , destination_cohort.end_date AS destination_cohort_end_date
    , destination_cohort.campus AS destination_cohort_campus
    , destination_cohort.modality AS destination_cohort_modality
    , destination_cohort.discipline AS destination_cohort_discipline
    , destination_cohort.pacing AS destination_cohort_pacing
FROM
    {{ params.table_refs["registrar.roster_actions"] }}
JOIN
    {{ params.table_refs["cohorts"] }} source_cohort
    ON roster_actions.source_cohort_uuid = source_cohort.uuid
JOIN 
    {{ params.table_refs["cohorts"] }} destination_cohort
    ON roster_actions.destination_cohort_uuid = destination_cohort.uuid
{% endblock %}