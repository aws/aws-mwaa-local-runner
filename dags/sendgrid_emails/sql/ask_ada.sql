{% extends "rotate_table.sql" %}
{% block query %}
SELECT DISTINCT
    s.first_name
    , s.learn_uuid
    , s.email
FROM askbot.conversations c
JOIN fis.students s
    ON c.student_id = s.learn_uuid
WHERE
    -- Student had a conversation in the last week
    c.created_at BETWEEN CURRENT_TIMESTAMP - 7 AND CURRENT_TIMESTAMP
    -- Student has not received this email in the last 30 days
    AND NOT EXISTS (
        SELECT 1
        FROM fis.sendgrid_email_events AS e
        WHERE s.email = e.email
        AND e.sg_template_id = '{{ params.template_id }}'
        AND NVL(DATEDIFF(day, e.delivered, '{{ data_interval_end }}'), 0) < 30
    )
{% endblock %}