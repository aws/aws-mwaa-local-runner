{% extends "template.sql" %}
{% block query %}
SELECT
    submission_id - 158020000000000000 AS submission_id
    , assignment_id - 158020000000000000 AS assignment_id
    , student_id - 158020000000000000 AS student_id
    , grader_id - 158020000000000000 AS grader_id
    , grading_complete
    , grade
    , points_possible
    , score
    , event_name
    , event_time
FROM {{ params["dest_schema"] }}_streaming.grade_change
{% endblock %}
{% set id = ["submission_id", "assignment_id", "student_id"] %}