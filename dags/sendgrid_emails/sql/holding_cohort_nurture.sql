{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    first_name
    , r.email
    , discipline
    , DATEDIFF(day, added_to_cohort_date, '{{ ds }}') AS days_on_roster
FROM fis.rosters r
WHERE 
    cohort_start_date = '2099-12-25' -- Holding cohorts
    AND removed_from_cohort_date IS NULL -- Still on the roster
    AND days_on_roster >= 30 -- Begin sending starting at day 30
    AND MOD(days_on_roster, 30) = 0 -- Send reminder every 30 days
    -- Student has not received this email in the last 28 days
    AND NOT EXISTS (
        SELECT 1
        FROM fis.sendgrid_email_events AS e
        WHERE r.email = e.email
        AND e.sg_template_id = '{{ params.template_id }}'
        AND NVL(DATEDIFF(day, e.delivered, '{{ ds }}'), 0) < 28
    )
{% endblock %}