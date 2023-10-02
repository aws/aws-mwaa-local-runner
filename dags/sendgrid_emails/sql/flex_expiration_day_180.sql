{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    learn_uuid
    , r.email
    , first_name
    , cohort_start_date AS start_date
    , discipline AS program_name
    , expiration_date
    , DATEDIFF(day, '{{ ds }}', expiration_date) AS days_until_expiration
FROM fis.rosters r
WHERE 
    days_until_expiration = 180
    AND modality = 'Flex'
    AND is_most_recent_cohort
    -- Student has not received this email yet
    AND NOT EXISTS (
        SELECT 1
        FROM fis.sendgrid_email_events AS e
        WHERE r.email = e.email
        AND e.sg_template_id = '{{ params.template_id }}'
    )
{% endblock %}