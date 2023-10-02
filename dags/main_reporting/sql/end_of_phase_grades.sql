{% extends "rotate_table.sql" %}
{% block query %}
-- Software Engineering and Data Science (final assignment)
SELECT
    learn_uuid 
    , MIN(CASE WHEN module IN ('1', 'Phase 1') THEN submitted_at END) AS phase_1_pass_date
    , MAX(CASE WHEN module IN ('1', 'Phase 1') THEN grade END) AS phase_1_grade
    , MIN(CASE WHEN module IN ('2', 'Phase 2') THEN submitted_at END) AS phase_2_pass_date
    , MAX(CASE WHEN module IN ('2', 'Phase 2') THEN grade END) AS phase_2_grade
    , MIN(CASE WHEN module IN ('3', 'Phase 3') THEN submitted_at END) AS phase_3_pass_date
    , MAX(CASE WHEN module IN ('3', 'Phase 3') THEN grade END) AS phase_3_grade
    , MIN(CASE WHEN module IN ('4', 'Phase 4') THEN submitted_at END) AS phase_4_pass_date
    , MAX(CASE WHEN module IN ('4', 'Phase 4') THEN grade END) AS phase_4_grade
    , MIN(CASE WHEN module IN ('5', 'Phase 5') THEN submitted_at END) AS phase_5_pass_date
    , MAX(CASE WHEN module IN ('5', 'Phase 5') THEN grade END) AS phase_5_grade
FROM {{ params.table_refs["grades"] }}
WHERE 
	pass_fail = 'Pass'
    AND title !~ '(Mock)|(MVP)|(Practice)'
	AND (
	    -- SE
		title ~ 'Instructor Review: Phase \\d (Portfolio )?Project' -- Captures SP, Async Online, and Flex
		OR title ~ 'Phase \\d.*Code Challenge' -- Captures Live
	    
        -- DS
        OR title ~ 'Phase \\d Project - GitHub Repository URL' -- Captures Flex (or Live second attempt)
        OR title ~ 'Capstone Project.*GitHub Repository URL' -- Captures old Flex and Online submissions
        OR title ~ 'Phase \\d Code Challenge' -- Captures Live (first attempt)
        OR title ~ 'Phase \\d Project \\(Online\\)' -- Captures ONL01-DTSC-PT-092820

        -- Legacy Learn grades
        OR assignment_type = 'code_challenge' -- Learn CCs
        OR assignment_type = 'live_assessment' -- Learn Portfolio Projects
    )
GROUP BY 
    learn_uuid
/* TODO: Add PD/CY
UNION ALL
-- Product Design and Cybersecurity Engineering (course grade)
SELECT
    pd.unique_name AS learn_uuid
    , ud.canvas_id as student_id
    , ud.name as student_name
    , cd.canvas_id as course_id
    , cd.name as course_name
    , ad.subaccount1 AS campus
    , ad.subaccount2 AS discipline
    , ad.subaccount3 AS pacing
    , ad.subaccount4 AS cohort_name
    , csf.current_score as course_grade
FROM canvas.user_dim ud
JOIN canvas.enrollment_dim ed
    ON ud.id = ed.user_id
JOIN canvas.course_score_fact csf
    ON ed.id = csf.enrollment_id
JOIN canvas.course_dim cd
    ON csf.course_id = cd.id
JOIN canvas.pseudonym_dim pd
    ON ud.id = pd.user_id
    AND pd.sis_user_id IS NOT NULL
JOIN canvas.account_dim ad
    ON cd.account_id = ad.id
*/
{% endblock %}