{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    student_uuid
    , LISTAGG(
        TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS') || ',' ||
        TO_CHAR(coaching_start_date, 'YYYY-MM-DD') || ',' ||
        NVL(TO_CHAR(coaching_end_date, 'YYYY-MM-DD'), ' ') || ',' ||
        coaching_tier || ',' ||
        coaching_status, '|')
    WITHIN GROUP (ORDER BY updated_at) AS history
FROM {{ params.table_refs["service_operations_versions"] }}
GROUP BY student_uuid
{% endblock %}