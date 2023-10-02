{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    learn_uuid 
    , COUNT(completion_timestamp) AS total_lessons_complete
    , COUNT(step_source_id) AS total_lessons
    , total_lessons_complete::FLOAT / NULLIF(total_lessons, 0) * 100 AS perc_complete
FROM {{ params.table_refs["pathwright_progress"] }}
WHERE course_name ~* 'career prep'
GROUP BY learn_uuid
{% endblock %}