{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    s.first_name
    , s.full_name
    , s.email
    , s.learn_uuid
    , c.coaching_start_date
    , c.days_seeking
    , c.coaching_status
    , c.coaching_end_date
FROM fis.students AS s
JOIN fis.service_operations_versions AS c
    ON s.learn_uuid = c.student_uuid
WHERE 
    DATEDIFF(day, c.coaching_start_date, '{{ ds }}') = 90
    AND c.coaching_tier IN ('6 Months of Coaching', '6 Months of Coaching - MBG')
    AND c.coaching_status = 'Active in Coaching'
    AND c.is_most_recent
    -- Student has not received this email yet
    AND NOT EXISTS (
        SELECT 1
        FROM fis.sendgrid_email_events AS e
        WHERE s.email = e.email
        AND e.sg_template_id = '{{ params.template_id }}'
    )
{% endblock %}