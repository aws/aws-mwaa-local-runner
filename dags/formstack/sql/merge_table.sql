{% extends "rotate_table.sql" %}
{% block query %}
SELECT s.*, r.submission_id, r.field_id, r.label, r.value, r.type
FROM {{ params["submissions_table"].schema_in_env }}.{{ params["submissions_table"].table_in_env }} s
JOIN {{ params["responses_table"].schema_in_env }}.{{ params["responses_table"].table_in_env }} r
    ON s.id = r.submission_id
{% endblock %}