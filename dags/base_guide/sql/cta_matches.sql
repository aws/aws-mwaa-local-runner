{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    c.id
    , u.email
    , u.student_uuid
    , SUM(
        decode(
            NVL2(
                decode(
                    c.name,
                    'Application Completed', "application_completed",
                    'Taken CCAT', "taken_ccat",
                    'Scheduled Interview', "scheduled_interview",
                    'Completed Interview', "completed_interview",
                    'Paid Deposit', "paid_deposit",
                    'EA Signed', "ea_signed",
                    'Financially Cleared', "financially_cleared",
                    'Started Prep', "started_prep",
                    'Academic Clearance', "academic_clearance"
                    ), 'true', 'false'
                ) = complete, true, 1, false, 0
            )
        ) AS requirements_satisfied
    , COUNT(1) AS total_requirements
FROM 
    {{ params.table_refs["cta_rulesets"] }} c
JOIN 
    {{ params.table_refs["cta_users"] }} u 
    ON c.student_status = u.student_status
WHERE u.student_uuid IS NOT NULL -- Must have a portal account to see CTAs
GROUP BY 
    c.id
    , c.text
    , u.email
    , u.student_uuid
HAVING requirements_satisfied = total_requirements
{% endblock %}