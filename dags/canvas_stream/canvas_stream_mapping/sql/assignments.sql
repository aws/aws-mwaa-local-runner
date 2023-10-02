{% extends "template.sql" %}
{% block query %}
SELECT
    assignment_id - 158020000000000000 AS id
    , body_context_id - 158020000000000000 AS course_id
    , assignment_group_id - 158020000000000000 AS assignment_group_id
    , time_zone
    , title
    , description
    , points_possible::float
    , CONVERT_TIMEZONE(NVL(time_zone, 'UTC'), 'UTC', due_at) AS due_at
    , submission_types
    , workflow_state
    , event_name
    , event_time
FROM {{ params["dest_schema"] }}_streaming.assignment_created
UNION ALL
SELECT
    assignment_id - 158020000000000000
    , body_context_id - 158020000000000000
    , assignment_group_id - 158020000000000000
    , time_zone
    , title
    , description
    , points_possible::float
    , CONVERT_TIMEZONE(NVL(time_zone, 'UTC'), 'UTC', due_at) AS due_at
    , submission_types
    , workflow_state
    , event_name
    , event_time
FROM {{ params["dest_schema"] }}_streaming.assignment_updated
{% endblock %}
