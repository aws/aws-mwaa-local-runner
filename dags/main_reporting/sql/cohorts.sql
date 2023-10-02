{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    cohorts.id AS uuid
    , registrar_cohorts.id
    , learn_batches.uuid AS learn_batch_uuid
    , cohorts.name
    , learn_batches.name AS learn_batch_name
    , cohorts.start_date
    , CASE
        WHEN cohorts.start_date < '1/29/22'::date
        AND paces.name = 'Flex'
        THEN dateadd(month, 15, cohorts.start_date) -- edge case: https://github.com/flatiron-labs/irondata/pull/523#issuecomment-1079420359
        ELSE cohorts.end_date END AS end_date
    , cohorts.end_date conductor_end_date
    , campuses.name AS campus
    , markets.name AS market
    , CASE 
        WHEN campuses.name = 'Online' THEN 'Online'
        WHEN campuses.name IN ('East', 'West', 'National') THEN paces.name
        ELSE 'Campus' END AS modality
    , disciplines.id AS discipline_uuid
    , disciplines.name AS discipline
    , paces.name AS pacing
    , paces.name = 'Self Paced' OR CURRENT_DATE BETWEEN cohorts.start_date AND cohorts.end_date AS cohort_is_active
    , lms_cohorts.canvas_account_id
    , canvas_data.url
FROM
    {{ params.table_refs["service_catalog.cohorts"] }} cohorts
LEFT JOIN 
    {{ params.table_refs["registrar.cohorts"] }} registrar_cohorts
    ON cohorts.id = registrar_cohorts.uuid
LEFT JOIN 
    {{ params.table_refs["course_conductor_deprecated.lms_cohorts"] }} learn_batches 
    ON cohorts.id = learn_batches.cohort_uuid
    AND learn_batches.lms_name = 'learn.co'
LEFT JOIN 
    {{ params.table_refs["service_catalog.campuses"] }} campuses 
    ON cohorts.campus_id = campuses.id
LEFT JOIN 
    {{ params.table_refs["service_catalog.markets"] }} markets 
    ON campuses.market_id = markets.id
LEFT JOIN
    {{ params.table_refs["service_catalog.course_offerings"] }} course_offerings 
    ON cohorts.course_offering_id = course_offerings.id
LEFT JOIN 
    {{ params.table_refs["service_catalog.disciplines"] }} disciplines 
    ON course_offerings.discipline_id = disciplines.id
LEFT JOIN 
    {{ params.table_refs["service_catalog.pacing_options"] }} paces 
    ON course_offerings.pacing_option_id = paces.id
LEFT JOIN 
    easely.lms_cohorts
    ON cohorts.id = lms_cohorts.id
LEFT JOIN
    {{ params.table_refs["registrar.canvas_data"] }} canvas_data
    ON cohorts.id = canvas_data.resource_uuid 
    AND canvas_data.resource_type = 'Cohort'
{% endblock %}