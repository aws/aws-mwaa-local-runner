{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
	dates.action_date 
    , rosters.learn_uuid
	, rosters.first_name
	, rosters.last_name
	, rosters.email
	, instructor.educator_uuid AS instructor_uuid
	, instructor.educator_name AS instructor_name
    , instructor.educator_email AS instructor_email
	, advisor.educator_uuid AS advisor_uuid
	, advisor.educator_name AS advisor_name
    , advisor.educator_email AS advisor_email
	, coach.educator_uuid AS coach_uuuid
	, coach.educator_name AS coach_name
    , coach.educator_email AS coach_email
	, rosters.cohort_uuid
    , rosters.cohort_name 
    , rosters.campus
    , rosters.modality
    , rosters.discipline
    , rosters.pacing
	, paces.pacing_in_weeks
	, CASE 
		WHEN rosters.pacing = 'Flex' THEN 1.0 / (NVL(paces.pacing_in_weeks, 60) * 7)
		ELSE 1 / DATEDIFF(day, rosters.cohort_start_date, rosters.cohort_end_date)::FLOAT
		END AS pct_thru_program
	, CASE WHEN rosters.discipline = students.most_recent_cohort_discipline THEN 1 ELSE 0 END AS is_current_discipline
	, LEAST(
		SUM(pct_thru_program * is_current_discipline) OVER (PARTITION BY rosters.learn_uuid ORDER BY dates.action_date ROWS UNBOUNDED PRECEDING), 
		1) AS cumulative_progress
    , rosters.student_start_date
    , rosters.student_end_date
	, rosters.cohort_start_date
    , rosters.cohort_end_date
	, students.matriculated_cohort_name
	, students.most_recent_cohort_name
	, students.most_recent_cohort_cancel_date AS withdrawal_date
    , students.graduation_date
FROM 
	(  
		SELECT calendar.date::DATE AS action_date
		FROM fis.calendar
		WHERE EXTRACT(YEAR FROM calendar.date) >= 2019
	) dates
JOIN
	(
		
		SELECT 
			learn_uuid
			, first_name
			, last_name
			, email
			, cohort_uuid
			, cohort_name
			, campus
			, modality
			, discipline
			, pacing
			, cohort_start_date
			, cohort_end_date
			, GREATEST(cohort_start_date, added_to_cohort_date, scheduled_start_date)::DATE AS student_start_date
			, LEAST(cohort_end_date, removed_from_cohort_date, admission_canceled_date, CURRENT_TIMESTAMP)::DATE AS student_end_date
		FROM fis.rosters
		WHERE 
			cohort_name !~* '(EXP)|(v.?000)'
			AND (
				on_roster_day_1 -- Student started on day 1 of cohort
				OR scheduled_start_date < CURRENT_TIMESTAMP -- Scheduled start date is in the past
				OR cohort_start_date <= added_to_cohort_date -- Caputures transfers that were added to roster after cohort started
			)
	) rosters
	ON rosters.student_start_date <= dates.action_date AND dates.action_date <= student_end_date
JOIN 
    fis.students
	ON rosters.learn_uuid = students.learn_uuid
	AND (
		dates.action_date <= students.graduation_date 
		OR students.graduation_date IS NULL 
	)
LEFT JOIN fis.daily_student_paces paces 
	ON rosters.learn_uuid = paces.learn_uuid
	AND dates.action_date = paces.reporting_date
LEFT JOIN 
    fis.service_educator_versions instructor
    ON rosters.learn_uuid = instructor.student_uuid
	AND dates.action_date BETWEEN instructor.created_at AND NVL(instructor.removed_at, CURRENT_TIMESTAMP)
    AND instructor.educator_type = 'instructor'
LEFT JOIN 
	fis.service_educator_versions advisor
    ON rosters.learn_uuid = advisor.student_uuid
	AND dates.action_date BETWEEN advisor.created_at AND NVL(advisor.removed_at, CURRENT_TIMESTAMP)
    AND advisor.educator_type = 'advisor'
LEFT JOIN 
	fis.service_educator_versions coach
    ON rosters.learn_uuid = coach.student_uuid
	AND dates.action_date BETWEEN coach.created_at AND NVL(coach.removed_at, CURRENT_TIMESTAMP)
    AND coach.educator_type = 'coach'
{% endblock %}