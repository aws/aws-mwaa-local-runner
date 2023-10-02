{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    s.email
    , s.first_name
    , DATEDIFF(day, pd.last_request_at, '{{ ds }}') AS days_since_login
FROM fis.students s 
JOIN canvas.pseudonym_dim pd 
    ON s.learn_uuid = pd.sis_user_id
WHERE
    s.student_status = 'matriculated'
    AND s.most_recent_cohort_modality = 'Flex'
    AND s.institution_type = 'Consumer'
    -- Inactive for 60, 90, 120, ... days
    AND days_since_login >= 60 -- Begin sending starting at day 60
    AND MOD(days_since_login, 30) = 0 -- Send reminder every 30 days
    -- Student has not received this email in the last 28 days
    AND NOT EXISTS (
        SELECT 1
        FROM fis.sendgrid_email_events AS e
        WHERE s.email = e.email
        AND e.sg_template_id = '{{ params.template_id }}'
        AND NVL(DATEDIFF(day, e.delivered, '{{ ds }}'), 0) < 28
    )
{% endblock %}