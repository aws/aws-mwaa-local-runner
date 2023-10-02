{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    a.admittee_id AS learn_uuid
    , ra.admission_uuid
    , ra.source_cohort_uuid
    , ra.source_exit_date AS expiration_date
    , ra.updated_at AS expiration_updated_at
FROM registrar.roster_actions ra 
JOIN registrar.admissions a 
    ON ra.admission_uuid = a.uuid
WHERE
    ra.type = 'Expire' 
    AND ra.status = 'scheduled'
{% endblock %}