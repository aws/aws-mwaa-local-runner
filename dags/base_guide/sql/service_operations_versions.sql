{% extends "rotate_table.sql" %}
{% block query %}
select
    s.student_uuid
    , s.created_at
    , s.updated_at
    , s.coaching_start_date
    , s.coaching_end_date
    , datediff(day, s.coaching_start_date, NVL(s.coaching_end_date, current_date)) AS days_seeking
    , ct.name AS coaching_tier
    , decode(
        s.coaching_status_id,
        1, 'Active in Coaching',
        2, 'Placed',
        3, 'Withdrawn') AS coaching_status
    , decode(
        s.withdrawal_reason_id,
        1, 'Accepted non-qualifying job',
        2, 'Entrepreneur',
        3, 'Military service',
        4, 'Not job seeking in the US',
        5, 'Opted out',
        6, 'Personal',
        7, 'Pursuing further education',
        8, 'Pursuing non-qualifying job',
        9, 'Unresponsive',
        10, 'Failed to maintain eligibility',
        11, 'Code of Conduct violation or delinquent tuition',
        12, 'Services expired') AS withdrawal_reason
    , adt.name AS advising_tier
    , ROW_NUMBER() OVER (PARTITION BY s.student_uuid ORDER BY s.updated_at DESC) = 1 AS is_most_recent
    , CASE 
        WHEN ct.name ~ '6 Months of Coaching'
        THEN ROW_NUMBER() OVER (PARTITION BY s.student_uuid ORDER BY ct.name ~ '6 Months of Coaching' DESC NULLS LAST, s.updated_at DESC) = 1 
        END AS is_most_recent_6mo_tier
    , CASE 
        WHEN ct.name = '3 Coaching Sessions'
        THEN ROW_NUMBER() OVER (PARTITION BY s.student_uuid ORDER BY ct.name = '3 Coaching Sessions' DESC NULLS LAST, s.updated_at DESC) = 1 
        END AS is_most_recent_3_sessions_tier
FROM 
    (
        -- Get current values
        SELECT 
            id
            , student_uuid
            , created_at 
            , updated_at 
            , coaching_start_date
            , coaching_end_date
            , coaching_tier_id
            , coaching_status_id
            , withdrawal_reason_id
            , advising_tier_id
        FROM service_operations.students
        UNION ALL
        -- Get update history
        SELECT 
            item_id AS id 
            , JSON_EXTRACT_PATH_TEXT(object, 'student_uuid') AS student_uuid 
            , JSON_EXTRACT_PATH_TEXT(object, 'created_at')::TIMESTAMP AS created_at
            , JSON_EXTRACT_PATH_TEXT(object, 'updated_at')::TIMESTAMP AS updated_at
            , NULLIF(JSON_EXTRACT_PATH_TEXT(object, 'coaching_start_date'),'')::TIMESTAMP AS coaching_start_date
            , NULLIF(JSON_EXTRACT_PATH_TEXT(object, 'coaching_end_date'),'')::TIMESTAMP AS coaching_end_date
            , NULLIF(JSON_EXTRACT_PATH_TEXT(object, 'coaching_tier_id'),'')::INT AS coaching_tier_id
            , NULLIF(JSON_EXTRACT_PATH_TEXT(object, 'coaching_status_id'),'')::INT AS coaching_status_id
            , NULLIF(JSON_EXTRACT_PATH_TEXT(object, 'withdrawal_reason_id'),'')::INT AS withdrawal_reason_id
            , NULLIF(JSON_EXTRACT_PATH_TEXT(object, 'advising_tier_id'),'')::INT AS advising_tier_id
        FROM service_operations.versions
        /* Per convo with Sam Arthur in Ops, event = 'delete' 
            can be considered errors and be ingnored from version history */
        WHERE event = 'update'
        ORDER BY created_at DESC
    ) s
LEFT JOIN 
    service_operations.coaching_tiers ct
    ON s.coaching_tier_id = ct.id
LEFT JOIN 
    service_operations.advising_tiers adt
    ON s.advising_tier_id = adt.id
{% endblock %}