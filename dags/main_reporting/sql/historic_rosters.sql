{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
	u.learn_uuid
    , c.uuid AS cohort_uuid
	, ur.created_at 
	, ur.archived_at
    , NVL(c.name, b.iteration) AS cohort_name
    , c.start_date AS cohort_start_date
    , c.end_date AS cohort_end_date
    , CASE 
        WHEN c.campus IS NOT NULL THEN c.campus 
        WHEN b.iteration = 'v-000' THEN 'Online'
        END AS campus
    , CASE 
        WHEN c.modality IS NOT NULL THEN c.modality 
        WHEN b.iteration = 'v-000' THEN 'Online'
        END AS modality
    , CASE 
        WHEN c.discipline IS NOT NULL THEN c.discipline 
        WHEN b.iteration = 'v-000' THEN 'Software Engineering'
        END AS discipline
    , CASE 
        WHEN c.pacing IS NOT NULL THEN c.pacing
        WHEN b.iteration = 'v-000' THEN 'Self Paced' 
        END AS pacing
	, NVL(c.start_date, ur.created_at::DATE) <= NVL(ur.archived_at, CURRENT_DATE) AS on_roster_day_1
	, NVL(c.start_date, ur.created_at::DATE) + 14 <= NVL(ur.archived_at, CURRENT_DATE) AS on_roster_day_15
FROM {{ params.table_refs["learn.user_roles"] }} ur
JOIN {{ params.table_refs["learn.users"] }} u 
    ON ur.user_id = u.id
JOIN {{ params.table_refs["learn.batches"] }} b 
    ON ur.roleable_id = b.id
JOIN {{ params.table_refs["learn.batch_settings"] }} bs 
    ON b.id = bs.batch_id 
    AND bs.setting = 'graduatable'
    AND bs.value = 'true'
LEFT JOIN {{ params.table_refs["course_conductor_deprecated.lms_cohorts"] }} lms_cohorts
    ON b.uuid = lms_cohorts.uuid 
    AND lms_cohorts.lms_name = 'learn.co'
LEFT JOIN {{ params.table_refs["cohorts"] }} c
    ON lms_cohorts.cohort_uuid = c.uuid
WHERE 
    ur.role_id = 3
    AND (
        -- Only Legacy SP or an official structured programm with a date
        b.iteration = 'v-000'
        OR (
            LOWER(b.iteration) ~ '^((onl)|(nyc)|(alb)|(lon)|(est)|(sea)|(w?dc)|(hou)|(chi)|(dumbo)|(den)|(atl)|(sfo?)|(wst)|(cos)|(atx)|(web)|(mid)).*\\d{4,8}$'
            AND LOWER(b.iteration) !~ '(fast.?t?rack)|(skill)|(-fe-)'
            AND b.organization_id != 10 -- Free in-school programs
        )
        -- Or a cohort that exists in Course Conductor
        OR (
            c.name IS NOT NULL
            -- Getting rid of "holding cohorts" by filtering to programs 30+ days
            AND (
                NVL(c.pacing, 'Self Paced') = 'Self Paced'
                OR DATEDIFF(day, c.start_date, c.end_date) > 30
            )
        )
    )
    -- Only cohorts where student was on roster for more than a day
    AND DATEDIFF(day, ur.created_at, NVL(ur.archived_at, GETDATE())) > 1
{% endblock %}