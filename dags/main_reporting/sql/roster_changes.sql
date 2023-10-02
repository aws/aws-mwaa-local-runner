{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    NVL(cr.admission_uuid, ra.admission_uuid) AS admission_uuid
    , NVL(cr.cohort_uuid, ra.destination_cohort_uuid) AS cohort_uuid
    , NVL(cr.occurred_at, ra.destination_enter_date) AS added_to_cohort_date
    , LEAST(cr.cancelled_at, LEAD(NVL(cr.occurred_at, ra.source_exit_date)) OVER (PARTITION BY NVL(cr.admission_uuid, ra.admission_uuid) ORDER BY NVL(cr.occurred_at, ra.source_exit_date))) AS removed_from_cohort_date
    , NVL(cr.occurred_at, ra.inserted_at) AS occurred_at
    , ra.type AS added_to_roster_action_type
    , ra.change AS added_to_roster_action_change
    , JSON_EXTRACT_PATH_TEXT(ra.data, 'module') AS added_to_roster_action_module
    , JSON_EXTRACT_PATH_TEXT(ra.data, 'details') AS added_to_roster_action_details
    , JSON_EXTRACT_PATH_TEXT(ra.data, 'reasons') AS added_to_roster_action_reasons
    , LEAD(ra.type) OVER (PARTITION BY NVL(cr.admission_uuid, ra.admission_uuid) ORDER BY NVL(cr.occurred_at, ra.inserted_at)) AS removed_from_roster_action_type
    , LEAD(ra.change) OVER (PARTITION BY NVL(cr.admission_uuid, ra.admission_uuid) ORDER BY NVL(cr.occurred_at, ra.inserted_at)) AS removed_from_roster_action_change
    , LEAD(added_to_roster_action_module) OVER (PARTITION BY NVL(cr.admission_uuid, ra.admission_uuid) ORDER BY NVL(cr.occurred_at, ra.inserted_at)) AS removed_from_roster_action_module
    , LEAD(added_to_roster_action_details) OVER (PARTITION BY NVL(cr.admission_uuid, ra.admission_uuid) ORDER BY NVL(cr.occurred_at, ra.inserted_at)) AS removed_from_roster_action_details
    , LEAD(added_to_roster_action_reasons) OVER (PARTITION BY NVL(cr.admission_uuid, ra.admission_uuid) ORDER BY NVL(cr.occurred_at, ra.inserted_at)) AS removed_from_roster_action_reasons
FROM
    (
        SELECT
            CASE WHEN cohort_registrations.inserted_at < NVL(cohort_registrations.cancelled_at, CURRENT_TIMESTAMP) THEN cohort_registrations.admission_uuid ELSE admissions.uuid END AS admission_uuid
            , CASE WHEN cohort_registrations.inserted_at < NVL(cohort_registrations.cancelled_at, CURRENT_TIMESTAMP) THEN cohort_registrations.cohort_uuid ELSE admissions.cohort_uuid END AS cohort_uuid
            , CASE WHEN cohort_registrations.inserted_at < NVL(cohort_registrations.cancelled_at, CURRENT_TIMESTAMP) THEN cohort_registrations.inserted_at ELSE admissions.occurred_at END AS occurred_at
            , CASE WHEN cohort_registrations.inserted_at < NVL(cohort_registrations.cancelled_at, CURRENT_TIMESTAMP) THEN cohort_registrations.cancelled_at ELSE admissions.cancelled_at END AS cancelled_at
        FROM
            (   
                SELECT 
                    admissions.id 
                    , admissions.uuid
                    , JSON_EXTRACT_PATH_TEXT(registration_events.properties, 'cohort_uuid') AS cohort_uuid
                    , registration_events.occurred_at
                    , admissions.cancelled_at
                FROM {{ params.table_refs["registrar.admissions"] }} admissions
                JOIN {{ params.table_refs["registrar.registration_events"] }} registration_events
                    ON admissions.id = registration_events.resource_id
                    AND registration_events.action IN ('Student Admitted', 'Admission Cohort Changed')
            ) admissions
        FULL OUTER JOIN
            (
                SELECT
                    cr.admission_id
                    , a.uuid AS admission_uuid
                    , c.uuid AS cohort_uuid
                    , cr.inserted_at
                    , cr.cancelled_at
                FROM {{ params.table_refs["registrar.cohort_registrations"] }} cr
                JOIN {{ params.table_refs["cohorts"] }} c
                    ON cr.cohort_id = c.id
                JOIN {{ params.table_refs["registrar.admissions"] }} a 
                    ON cr.admission_id = a.id 
            ) AS cohort_registrations
            ON admissions.id = cohort_registrations.admission_id
            AND admissions.cohort_uuid = cohort_registrations.cohort_uuid
            AND TO_CHAR(admissions.occurred_at, 'YYYY-MM-DD HH24:MI') = TO_CHAR(cohort_registrations.inserted_at, 'YYYY-MM-DD HH24:MI')
    ) cr
FULL OUTER JOIN
    (
        SELECT * 
        FROM {{ params.table_refs["registrar.roster_actions"] }} 
        WHERE 
            change <> 'Graduation' -- Exlude graduations - we don't want this to appear as a roster removal, but it's own action
            AND status <> 'scheduled' -- Excludes future roster actions such as expirations
            AND status <> 'cancelled' -- Exclude cancelled roster actions
    ) ra
    ON cr.admission_uuid = ra.admission_uuid
    AND cr.cohort_uuid = ra.destination_cohort_uuid
    AND TO_CHAR(cr.occurred_at, 'YYYY-MM-DD HH24:MI') = TO_CHAR(ra.inserted_at, 'YYYY-MM-DD HH24:MI')
{% endblock %}