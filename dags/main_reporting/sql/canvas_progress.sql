{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    cr.name 
	, cr.learn_uuid 
	, cr.user_canvas_id
	, cr.course_name
	, cr.course_canvas_id
	, cr.section_name
	, cr.enrollment_canvas_id
	, CONVERT_TIMEZONE('America/New_York', cr.enrollment_date) AS enrollment_date
	, CONVERT_TIMEZONE('America/New_York', cr.enrollment_end_date) AS enrollment_end_date
	, cr.enrollment_status
    , CONVERT_TIMEZONE('America/New_York', cr.last_activity_at) AS last_activity_at
    , CONVERT_TIMEZONE('America/New_York', cr.last_request_at) AS last_request_at
	, cr.account_canvas_id
	, cr.account_name
	, cr.cohort_name
	, cr.campus
	, cr.discipline
	, cr.pacing
	, md.name AS module_name
	-- , DENSE_RANK() OVER (PARTITION BY cr.learn_uuid, cr.account_name, cr.course_name, cr.section_name ORDER BY md.position) AS module_position
	, md.module_position
	, MAX(md.module_position) OVER (PARTITION BY cr.learn_uuid, cr.account_name, cr.course_name, cr.section_name) AS modules_in_course_count
	, CONVERT_TIMEZONE('America/New_York', mpd.created_at) AS module_start_date
	, CONVERT_TIMEZONE('America/New_York', mpd.completed_at) AS module_completed_date
	-- , ROW_NUMBER() OVER (PARTITION BY cr.learn_uuid, cr.account_name, cr.course_name, cr.section_name, md.name ORDER BY mid.position) AS module_item_position
	, mid.module_item_position
	, COUNT(mid.module_item_position) OVER (PARTITION BY cr.learn_uuid, cr.account_name, cr.course_name, cr.section_name, md.name) AS items_in_module_count
	, COUNT(mid.module_item_position) OVER (PARTITION BY cr.learn_uuid, cr.account_name, cr.course_name, cr.section_name) AS items_in_course_count
	, mid.canvas_id AS module_item_canvas_id
	, NVL(mid.content_canvas_id, mid.assignment_id) AS content_canvas_id
	, mid.content_type
	, mid.title AS module_item_name
	, CASE WHEN mpd.completed_at IS NOT NULL THEN 'complete' ELSE mpcrd.completion_status END AS completion_status
	, sd.canvas_id AS submission_canvas_id
	, sd.url AS submission_url
	, sd.grade
	, sd.attempt
	, ad.points_possible
	, sd.workflow_state AS grade_status
	, ad.due_at
	, CONVERT_TIMEZONE('America/New_York', sd.submitted_at) AS submitted_at
	, CONVERT_TIMEZONE('America/New_York', sd.graded_at) AS graded_at
	, i.name AS grader_name
	, clf.rating
 	, clf.submission_timestamp AS feedback_timestamp
 	, clf.feedback
	, dt.total_discussion_topic_entries
	, dt.avg_discussion_topic_entry_length
	, dt.total_discussion_topic_replies
	, dt.avg_discussion_topic_reply_length
FROM 
	{{ params.table_refs["canvas_roster"] }} cr
JOIN
	(
		SELECT *, ROW_NUMBER() OVER (PARTITION BY md.course_id ORDER BY md.position) AS module_position
		FROM {{ params.table_refs["canvas.module_dim"] }} md
		WHERE md.workflow_state = 'active'
	) md
	ON cr.course_id = md.course_id
LEFT JOIN 
	(
		SELECT 
			mid.id
			, mid.canvas_id
			, mid.module_id
			, mid.position
			, NVL(mid.assignment_id
				, qd.assignment_id
				, dtf.assignment_id) - 158020000000000000 AS assignment_id
			, NVL(mid.discussion_topic_id
				, mid.file_id
				, mid.quiz_id
				, mid.wiki_page_id) - 158020000000000000 AS content_canvas_id
			, mid.content_type
			, mid.title
			, mid.workflow_state
			, ROW_NUMBER() OVER (PARTITION BY mid.module_id ORDER BY mid.position) AS module_item_position
		FROM {{ params.table_refs["canvas.module_item_dim"] }} mid
		LEFT JOIN {{ params.table_refs["canvas.quiz_dim"] }} qd
			ON mid.quiz_id = qd.id
		LEFT JOIN {{ params.table_refs["canvas.discussion_topic_fact"] }} dtf
			ON mid.discussion_topic_id = dtf.discussion_topic_id
    	WHERE mid.workflow_state = 'active'
	) mid
    ON md.id = mid.module_id
LEFT JOIN
	{{ params.table_refs["canvas.module_progression_dim"] }} mpd
    ON md.id = mpd.module_id
    and cr.user_id = mpd.user_id 
LEFT JOIN 
	{{ params.table_refs["canvas.module_progression_completion_requirement_dim"] }} mpcrd
    ON mid.id = mpcrd.module_item_id
    AND mpd.id = mpcrd.module_progression_id
LEFT JOIN 
	{{ params.table_refs["canvas_lesson_feedback"] }} clf
	ON cr.learn_uuid = clf.learn_uuid 
	AND mid.canvas_id = clf.module_item_canvas_id
LEFT JOIN 
	{{ params.table_refs["canvas.assignment_dim"] }} ad
	ON mid.assignment_id = ad.canvas_id
LEFT JOIN 
	{{ params.table_refs["canvas.submission_dim"] }} sd
	ON mid.assignment_id = (sd.assignment_id - 158020000000000000)
	AND cr.user_id = sd.user_id
LEFT JOIN 
	{{ params.table_refs["canvas.user_dim"] }} i
	ON sd.grader_id = i.id
LEFT JOIN 
	(
		SELECT
			def.topic_id - 158020000000000000 AS discussion_topic_canvas_id
			, def.user_id 
			, COUNT(CASE WHEN ded.depth = 1 THEN 1 END) AS total_discussion_topic_entries
			, AVG(CASE WHEN ded.depth = 1 THEN def.message_length END) AS avg_discussion_topic_entry_length
			, COUNT(CASE WHEN ded.depth > 1 THEN 1 END) AS total_discussion_topic_replies
			, AVG(CASE WHEN ded.depth > 1 THEN def.message_length END) AS avg_discussion_topic_reply_length
		FROM {{ params.table_refs["canvas.discussion_entry_fact"] }} def
		JOIN {{ params.table_refs["canvas.discussion_entry_dim"] }} ded
			ON def.discussion_entry_id = ded.id
		WHERE ded.workflow_state = 'active'
		GROUP BY
			def.topic_id - 158020000000000000
			, def.user_id 
	) dt
	ON mid.content_canvas_id = dt.discussion_topic_canvas_id
	AND cr.user_id = dt.user_id
WHERE 
	cr.enrollment_type = 'StudentEnrollment'
	AND cr.enrollment_status <> 'deleted'
{% endblock %}