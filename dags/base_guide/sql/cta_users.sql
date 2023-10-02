{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    email 
    , student_uuid
    , lead_created
    , started_prep
    , application_completed
    , taken_ccat
    , scheduled_interview
    , completed_interview
    , registered
    , admitted
    , paid_deposit
    , committed
    , ea_signed
    , financially_cleared
    , academic_clearance
    , enrolled
    , matriculated
    , graduated
    , cancelled
    , withdrawn
    , CASE 
        WHEN admitted < NVL(withdrawn, cancelled) THEN NULL -- Remove students that cancel or withdrew after most recent admission
        WHEN graduated IS NOT NULL THEN 'alumni'
        WHEN matriculated IS NOT NULL THEN 'active'
        WHEN NVL(registered, admitted, committed) IS NOT NULL THEN 'committed'
        ELSE 'prospective' 
        END AS student_status
FROM
    (
        SELECT 
            e.email
            , u.learn_uuid AS student_uuid
            , e.event_date
            , e.event_type
        FROM {{ params.table_refs["cta_events"] }} e 
        LEFT JOIN learn.users u 
            ON e.email = u.email
    )
PIVOT (
    MAX(event_date) for event_type IN (
        'Lead Created' AS lead_created
        , 'Started Prep' AS started_prep
        , 'Application Completed' AS application_completed
        , 'Taken CCAT' AS taken_ccat
        , 'Scheduled Interview' AS scheduled_interview
        , 'Completed Interview' AS completed_interview
        , 'registered' AS registered
        , 'admitted' AS admitted
        , 'Paid Deposit' AS paid_deposit
        , 'committed' AS committed
        , 'EA Signed' AS ea_signed
        , 'Financially Cleared' AS financially_cleared
        , 'Academic Clearance' AS academic_clearance
        , 'enrolled' AS enrolled
        , 'matriculated' AS matriculated
        , 'graduated' AS graduated
        , 'cancelled' AS cancelled
        , 'withdrawn' AS withdrawn
    )
)
{% endblock %}