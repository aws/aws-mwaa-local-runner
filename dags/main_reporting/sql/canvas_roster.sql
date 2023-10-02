{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    ud.name
    , ud.id AS user_id
	, ud.canvas_id AS user_canvas_id
	, pd.unique_name AS learn_uuid 
	, NULL AS campus
	, d.name AS discipline
	, p.name AS pacing
	, NULL AS cohort_name
	, lc.start_date
	, ad.name AS account_name
	, ad.canvas_id AS account_canvas_id
	, cd.id AS course_id
	, cd.canvas_id AS course_canvas_id
	, cd.name AS course_name 
	, csd.canvas_id AS section_canvas_id
	, csd.name AS section_name
	, ed.canvas_id AS enrollment_canvas_id
    , ed.type AS enrollment_type
    , ed.created_at AS enrollment_date
    , ed.workflow_state AS enrollment_status 
    , ed.completed_at AS enrollment_end_date
    , ed.last_activity_at
	, pd.last_request_at
FROM 
	{{ params.table_refs["canvas.course_dim"] }} cd
JOIN
	(
		SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, course_id ORDER BY created_at DESC) AS row_num
		FROM {{ params.table_refs["canvas.enrollment_dim"] }}
	) ed
    ON ed.course_id = cd.id
	AND ed.row_num = 1
JOIN 
	{{ params.table_refs["canvas.user_dim"] }} ud
    ON ed.user_id = ud.id
JOIN 
	{{ params.table_refs["canvas.pseudonym_dim"] }} pd
	ON ud.id = pd.user_id
	AND pd.sis_user_id IS NOT NULL
LEFT JOIN 
	{{ params.table_refs["canvas.course_section_dim"] }} csd 
	ON ed.course_section_id = csd.id
LEFT JOIN 
	{{ params.table_refs["canvas.account_dim"] }} ad 
	ON cd.account_id = ad.id 
LEFT JOIN 
	(
		SELECT lms_offering_id, canvas_account_id, start_date
		FROM easely.lms_cohorts
		GROUP BY 1, 2, 3
	) lc
	ON ad.canvas_id = lc.canvas_account_id
LEFT JOIN
	easely.lms_offerings lo 
	ON lc.lms_offering_id = lo.id
LEFT JOIN
	service_catalog.disciplines d
	ON lo.discipline_uuid = d.id
LEFT JOIN
	service_catalog.pacing_options p
	ON lo.pace_uuid = p.id
{% endblock %}