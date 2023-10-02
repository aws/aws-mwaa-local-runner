{% extends "rotate_table.sql" %}
{% block query %}
-- Canvas grades
SELECT
	p.unique_name AS learn_uuid
	, u.canvas_id AS student_id
	, students.first_name
	, students.last_name
	, students.email
	, acc.name AS cohort
	, a.title
	, a.canvas_id AS assignment_id
	, agd.name AS assignment_type
	, CASE 
		WHEN a.title ~* 'Phase \\d'
		THEN 'Phase ' || REGEXP_SUBSTR(a.title, '[Phase|Mod] (\\d)', 1, 1, 'e') 
		ELSE c.name 
		END AS module
	, c.canvas_id AS module_id
	, s.attempt AS round
	, s.grade
	, a.grading_type
	, CASE 
		WHEN s.grade ~* '(^complete)|(^P$)' 
		THEN 'Pass'
		WHEN s.grade ~* '(incomplete)|(^NP$)'
		THEN 'Fail'
		WHEN a.grading_type = 'letter_grade' AND NULLIF(REGEXP_SUBSTR(s.grade, '(\\d)', 1, 1, 'e'),'')::INT >= 3 
		THEN 'Pass' 
		WHEN a.grading_type = 'letter_grade' AND NULLIF(REGEXP_SUBSTR(s.grade, '(\\d)', 1, 1, 'e'),'')::INT < 3 
		THEN 'Fail'
		WHEN a.grading_type = 'letter_grade' AND s.grade ~ '^[A-D]'
		THEN 'Pass'
		WHEN a.grading_type = 'letter_grade' AND s.grade = 'F'
		THEN 'Fail'
		WHEN a.grading_type = 'points' AND a.points_possible = 0
		THEN 'Unknown'
		WHEN a.grading_type = 'points' AND s.grade = a.points_possible::VARCHAR
		THEN 'Pass'
		ELSE 'Unknown'
		END AS pass_fail 
	, a.points_possible
	, a.due_at
	, NVL(s.submitted_at, s.graded_at) AS submitted_at
	, s.workflow_state AS submission_status
	, i.name AS grader_name
FROM 
	{{ params.table_refs["canvas.assignment_dim"] }} a
JOIN 
	{{ params.table_refs["canvas.assignment_group_dim"] }} agd
	ON a.assignment_group_id = agd.id
JOIN
	{{ params.table_refs["canvas.submission_dim"] }} s
	ON a.id = s.assignment_id
JOIN 
	{{ params.table_refs["canvas.user_dim"] }} u 
	ON s.user_id = u.id
JOIN 
	{{ params.table_refs["canvas.pseudonym_dim"] }} p 
	ON u.id = p.user_id
	AND p.sis_user_id IS NOT NULL
JOIN
    {{ params.table_refs["learn.users"] }} students
    ON p.unique_name = students.learn_uuid
LEFT JOIN 
	{{ params.table_refs["canvas.course_dim"] }} c
	ON a.course_id = c.id
	AND c.name !~* '(mock)|(homeroom)'
LEFT JOIN 
	{{ params.table_refs["canvas.account_dim"] }} acc 
	ON c.account_id = acc.id
LEFT JOIN 
	{{ params.table_refs["canvas.user_dim"] }} i 
	ON s.grader_id = i.id
WHERE	
	a.workflow_state = 'published'
	AND a.title != 'Roll Call Attendance'
{% endblock %}