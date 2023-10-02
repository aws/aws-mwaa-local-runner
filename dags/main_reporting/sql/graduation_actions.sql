{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    admittee_id AS learn_uuid
    , admission_uuid
    , source_cohort_uuid
    , DECODE(type, 'Graduate', source_exit_date) AS graduation_date
    , DECODE(type, 'Graduate', updated_at) AS graduation_updated_at
FROM 
    (
        SELECT a.admittee_id, ra.*, ROW_NUMBER() OVER (PARTITION BY a.admittee_id ORDER BY ra.updated_at DESC) AS row_num
        FROM registrar.roster_actions ra 
        JOIN registrar.admissions a 
        ON ra.admission_uuid = a.uuid
        WHERE change = 'Graduation'
    ) x
WHERE row_num = 1
{% endblock %}