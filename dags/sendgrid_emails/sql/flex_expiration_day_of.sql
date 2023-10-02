{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    learn_uuid
    , r.email
    , first_name
    , cohort_start_date AS start_date
    , discipline AS program_name
    , removed_from_cohort_date::DATE AS expiration_date
FROM fis.rosters r
WHERE 
    student_status = 'expired'
    AND removed_from_cohort_date::DATE = '{{ ds }}' - 1  -- Day after per Ops request
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