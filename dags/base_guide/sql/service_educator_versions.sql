{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    *
    , CASE WHEN educator_type = 'instructor' THEN ROW_NUMBER() OVER (PARTITION BY student_uuid ORDER BY educator_type = 'instructor' DESC, created_at ASC) = 1 ELSE false END AS is_first_instructor
    , CASE WHEN educator_type = 'instructor' THEN ROW_NUMBER() OVER (PARTITION BY student_uuid ORDER BY educator_type = 'instructor' DESC, removed_at DESC) = 1 ELSE false END AS is_most_recent_instructor
    , CASE WHEN educator_type = 'coach' THEN ROW_NUMBER() OVER (PARTITION BY student_uuid ORDER BY educator_type = 'coach' DESC, removed_at DESC) = 1 ELSE false END AS is_most_recent_coach
    , CASE WHEN educator_type = 'advisor' THEN ROW_NUMBER() OVER (PARTITION BY student_uuid ORDER BY educator_type = 'advisor' DESC, removed_at DESC) = 1 ELSE false END AS is_most_recent_advisor
FROM 
    (
        SELECT
            es.educator_uuid
            , es.student_uuid
            , CASE
                WHEN es.created_at BETWEEN cm.hire_date::DATE AND NVL(cm.end_date::DATE, CURRENT_TIMESTAMP)
                THEN 'coach'
                WHEN es.created_at BETWEEN sas.start_date::DATE AND NVL(sas.end_date::DATE, CURRENT_TIMESTAMP)
                THEN 'advisor'
                WHEN es.educator_uuid = 'eed113ed-c4a0-4020-823d-2e369e13506e' -- "Unassigned Advisor"
                THEN 'advisor'
                WHEN es.educator_uuid = '5697f36a-0a6b-42c9-a863-f075f2e93a3e' -- "Unassigned Instructor"
                THEN 'instructor'
                ELSE 'instructor' END AS educator_type
            , TRIM(u.first_name) || ' ' || TRIM(u.last_name) AS educator_name
            , u.email AS educator_email
            , avs.url AS availability_schedule
            , es.created_at
            , es.removed_at
        FROM 
            (
                -- Get current values
                SELECT educator_uuid, student_uuid, updated_at AS created_at, NULL::TIMESTAMP AS removed_at
                FROM service_educator.educator_students
                UNION ALL 
                -- Get update history
                SELECT 
                    REGEXP_SUBSTR(object, 'educator_uuid: ''?([\\w\\-]+)''?', 1, 1, 'e') AS educator_uuid
                    , REGEXP_SUBSTR(object, 'student_uuid: ''?([\\w\\-]+)''?', 1, 1, 'e') AS student_uuid
                    , REGEXP_SUBSTR(object, 'updated_at: !ruby/object:ActiveSupport::TimeWithZone.*utc: ([\\w: \\.\\-]+)', 1, 1, 'e')::TIMESTAMP AS created_at 
                    , v.created_at AS removed_at
                FROM service_educator.versions v
                WHERE 
                    event IN ('update', 'destroy')
                    AND item_type = 'EducatorStudent' 
            ) es
        LEFT JOIN 
            career_services.coach_managers cm
            ON es.educator_uuid = cm.coach_learn_uuid
        LEFT JOIN 
            fis.student_advisors_sheet sas
            ON es.educator_uuid = sas.uuid
        LEFT JOIN 
            learn.users u 
            ON es.educator_uuid = u.learn_uuid
        LEFT JOIN 
            service_educator.availability_schedules avs
            ON es.educator_uuid = avs.owner_uuid
    )
{% endblock %}