{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
	subs.learn_uuid
	, subs.user_first_name 
	, subs.user_last_name 
	, subs.user_email 
	, subs.created_at AS user_created_at
	, subs.resource_name AS course_name
	, subs.resource_slug AS course_slug
	, steps.lesson_order::INT 
	, steps.lesson_name
	, steps.lesson_url
	, steps.step_source_id 
	, steps.step_order::INT 
	, steps.step_name 
	, steps.step_url 
	, steps.step_verb
	, progress.completion_timestamp
	, MAX(CASE WHEN progress.is_most_recent THEN progress.completion_timestamp END) OVER (
		PARTITION BY subs.learn_uuid, subs.resource_name
		) AS most_recent_step_completion_date
	, MAX(CASE WHEN progress.is_most_recent THEN lesson_name END) OVER (
		PARTITION BY subs.learn_uuid, subs.resource_name
		) AS most_recent_lesson
	, MAX(CASE WHEN progress.is_most_recent THEN step_name END) OVER (
		PARTITION BY subs.learn_uuid, subs.resource_name
		) AS most_recent_step
FROM 
	(
		SELECT 
			a.action_type
			, at2.name
			, TO_TIMESTAMP(a.action_dtime , 'YYYY-MM-DD HH24:MI:SS.MS') AS action_timestamp
			, a.group_id 
			, a.resource_name 
			, a.resource_slug 
			, pa.account_id
			, u.learn_uuid
			, a.user_first_name 
			, a.user_last_name 
			, a.user_email 
			, pa.created_at
		FROM {{ params.table_refs["pathwright.activities"] }} a
		JOIN {{ params.table_refs["learn.pathwright_accounts"] }} pa
			ON a.user_id = pa.account_id
		JOIN {{ params.table_refs["learn.users"] }} u 
			ON pa.user_id = u.id
		JOIN {{ params.table_refs["pathwright.activity_types"] }} at2
			ON a.action_type = at2.id
		WHERE at2.name = 'create_subscription'
	) subs
LEFT JOIN 
	(
		SELECT 
			REGEXP_SUBSTR(lesson_url, '/library/[\\w-]+/(\\d*)/', 1, 1, 'e') AS group_id
			, lesson_order 
			, lesson_name 
			, lesson_url
			, step_source_id 
			, step_order  
			, step_name 
			, step_url 
			, step_verb
		FROM {{ params.table_refs["pathwright.path_steps"] }} ps 
		GROUP BY
			REGEXP_SUBSTR(lesson_url, '/library/[\\w-]+/(\\d*)/', 1, 1, 'e')
			, lesson_order 
			, lesson_name 
			, lesson_url
			, step_source_id 
			, step_order  
			, step_name 
			, step_url 
			, step_verb
	) steps
	ON subs.group_id = steps.group_id
LEFT JOIN 
	(
		SELECT 
			a.user_id
            , a.step_id
			, at2.name AS action_taken
			, TO_TIMESTAMP(a.action_dtime , 'YYYY-MM-DD HH24:MI:SS.MS') AS completion_timestamp
			, ROW_NUMBER() OVER (PARTITION BY a.user_id, a.step_id ORDER BY completion_timestamp) AS unique_step
			, MAX(completion_timestamp) OVER (
				PARTITION BY a.user_id, a.resource_name
				) = completion_timestamp AS is_most_recent
		FROM {{ params.table_refs["pathwright.activities"] }} a
		JOIN {{ params.table_refs["pathwright.activity_types"] }} at2
			ON a.action_type = at2.id
		WHERE at2.name = 'complete_step'
	) progress
	ON subs.account_id = progress.user_id 
	AND steps.step_source_id = progress.step_id
	AND unique_step = 1
{% endblock %}