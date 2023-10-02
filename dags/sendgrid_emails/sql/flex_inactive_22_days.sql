{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    s.email
    , s.first_name
    , DATEDIFF(day, pd.last_request_at, '{{ ds }}') AS days_since_login
FROM fis.students s 
JOIN canvas.pseudonym_dim pd 
    ON s.learn_uuid = pd.sis_user_id
JOIN 
    (
        SELECT 
            learn_uuid
            , course_name
            , ROW_NUMBER() OVER (PARTITION BY learn_uuid ORDER BY submitted_at DESC NULLS LAST) AS rn
        FROM fis.canvas_progress
    ) cp 
    ON s.learn_uuid = cp.learn_uuid
    AND cp.rn = 1
WHERE
    s.student_status = 'matriculated'
    AND s.most_recent_cohort_modality = 'Flex'
    AND s.institution_type = 'Consumer'
    AND cp.course_name <> 'Phase 5'
    -- Inactive for 22 days
    AND days_since_login = 22
    -- Student has not received this email in the last 21 days
    AND NOT EXISTS (
        SELECT 1
        FROM fis.sendgrid_email_events AS e
        WHERE s.email = e.email
        AND e.sg_template_id = '{{ params.template_id }}'
        AND NVL(DATEDIFF(day, e.delivered, '{{ ds }}'), 0) < 21
    )
{% endblock %}