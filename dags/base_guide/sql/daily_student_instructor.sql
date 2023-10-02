{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    calendar.date::DATE AS reporting_date
    , TO_CHAR(calendar.date::DATE, 'Mon YYYY') AS reporting_month
    , educator.*
FROM
    fis.calendar
LEFT JOIN 
    {{ params.table_refs["service_educator_versions"] }} educator
    ON educator.created_at::DATE <= calendar.date::DATE
    AND calendar.date::DATE < NVL(educator.removed_at::DATE, CURRENT_TIMESTAMP)
    AND educator.educator_type = 'instructor'
{% endblock %}