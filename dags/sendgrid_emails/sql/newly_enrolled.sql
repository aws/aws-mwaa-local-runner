{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    u.email
    , u.first_name 
    , MAX(te.occurred_at) enrolled_at
FROM registrar.tracking_events te
JOIN learn.users u
    ON JSON_EXTRACT_PATH_TEXT(data, 'student_uuid') = u.learn_uuid
WHERE
    -- Only send to enrolled students
    JSON_EXTRACT_PATH_TEXT(data, 'new_status') = 'enrolled'
    -- Consumer students only
    AND EXISTS (
        SELECT 1
        FROM registrar.student_institutions si
        JOIN service_catalog.institutions i
            ON si.institution_uuid = i.id 
        WHERE i.name = 'Flatiron School'
        AND si.student_uuid = u.learn_uuid
    )
    -- Student has not received this email yet
    AND NOT EXISTS (
        SELECT 1
        FROM fis.sendgrid_email_events AS e
        WHERE u.email = e.email
        AND e.sg_template_id = '{{ params.template_id }}'
    )
GROUP BY 1, 2
HAVING MAX(te.occurred_at) BETWEEN '{{ data_interval_start }}' AND '{{ data_interval_end }}'
{% endblock %}