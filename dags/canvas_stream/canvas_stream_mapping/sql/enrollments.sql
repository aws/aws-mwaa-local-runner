{% extends "template.sql" %}
{% block query %}
SELECT
    enrollment_id - 158020000000000000 AS id
    , course_id - 158020000000000000 AS course_id
    , course_section_id - 158020000000000000 AS course_section_id
    , user_id - 158020000000000000 AS user_id
    , type
    , workflow_state
    , event_name
    , event_time
FROM {{ params["dest_schema"] }}_streaming.enrollment_created
UNION ALL
SELECT
    enrollment_id - 158020000000000000
    , course_id - 158020000000000000
    , course_section_id - 158020000000000000
    , user_id - 158020000000000000
    , type
    , workflow_state
    , event_name
    , event_time
FROM {{ params["dest_schema"] }}_streaming.enrollment_updated
{% endblock %}