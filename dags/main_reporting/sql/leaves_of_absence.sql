{% extends "rotate_table.sql" %}
{% block query %}
SELECT
	loa_uuid
	, learn_uuid
	, admission_uuid
	, effective_date
	, return_date
	, source_cohort_uuid
	, destination_cohort_uuid
	, loa_reason
	, status
	, source_cohort.campus AS source_cohort_campus
	, source_cohort.discipline AS source_cohort_discipline
	, source_cohort.name AS source_cohort_name
	, destination_cohort.campus AS destination_cohort_campus
	, destination_cohort.discipline AS destination_cohort_discipline
	, destination_cohort.name AS destination_cohort_name
FROM
	(
		SELECT
			loa.uuid AS loa_uuid
			, u.learn_uuid
			, a.uuid AS admission_uuid
			, loa.effective_date
			, loa.return_date
			, source_cohort_uuid
			, destination_cohort_uuid
			, '\[\"'||loa.reason ||'\"\]' AS loa_reason
			, CASE WHEN loa.status ~* 'cancell?ed' THEN 'canceled' ELSE loa.status END AS status
		FROM {{ params.table_refs["registrar.leaves_of_absence"] }} loa
		LEFT JOIN {{ params.table_refs["registrar.admissions"] }} a
			ON loa.admission_uuid = a.uuid
		LEFT JOIN {{ params.table_refs["learn.users"] }} u
			ON a.admittee_id = u.learn_uuid
		WHERE loa.status != 'migrated to roster action'

	UNION ALL

		SELECT
			NULL AS loa_uuid
			, a.admittee_id AS learn_uuid
			, ra.admission_uuid
			, ra.source_exit_date AS effective_date
			, ra.destination_enter_date AS return_date
			, source_cohort_uuid
			, destination_cohort_uuid
			, JSON_EXTRACT_PATH_TEXT(ra.data, 'reasons') AS loa_reason
			, ra.status
		FROM {{ params.table_refs["registrar.roster_actions"] }} ra
		JOIN {{ params.table_refs["registrar.admissions"] }} a
			ON ra.admission_uuid = a.uuid
		WHERE destination_cohort_uuid IS NOT NULL
			AND ra.type = 'Leave Of Absence'
	)
JOIN {{ params.table_refs["cohorts"] }} source_cohort
	ON source_cohort_uuid = source_cohort.uuid
JOIN {{ params.table_refs["cohorts"] }} destination_cohort
	ON destination_cohort_uuid = destination_cohort.uuid
{% endblock %}
