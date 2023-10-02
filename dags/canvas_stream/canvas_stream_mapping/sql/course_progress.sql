{% extends "template.sql" %}
{% block query %}
SELECT
    course_account_id AS account_id
    , course_id
    , user_id
    , progress_requirement_completed_count
    , progress_requirement_count
    , event_name
    , event_time
FROM {{ params["dest_schema"] }}_streaming.course_progress
{% endblock %}
{% set id = ["account_id", "course_id", "user_id"] %}