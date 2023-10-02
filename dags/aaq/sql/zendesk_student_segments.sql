{% extends "rotate_table.sql" %}
{% block query %}
select
    zt.id AS ticket_id
    , zt.created_at
    , s.learn_uuid
    , s.matriculation_date
    , s.graduation_date
    , s.most_recent_cohort_cancel_date
    , r.cohort_name 
    , r.cohort_start_date
    , r.cohort_end_date
    , r.campus
    , r.modality
    , r.discipline
    , r.pacing
    , CASE
        WHEN zt.created_at < NVL(s.matriculation_date, s.most_recent_cohort_start_date) THEN '0. Pre-start'
        WHEN zt.created_at BETWEEN s.matriculation_date AND NVL(s.graduation_date, s.most_recent_cohort_cancel_date, CURRENT_TIMESTAMP) THEN '1. Matriculated / Active'
        WHEN zt.created_at > s.most_recent_cohort_cancel_date THEN '2. Withdrawn'
        WHEN zt.created_at > s.graduation_date THEN '3. Graduated'
        END AS student_segment
    , CASE
        WHEN zt.created_at BETWEEN s.graduation_date AND a.job_search_start_date AND NVL(a.placement_date::DATE, a.withdrawal_date::DATE) IS NULL THEN 'Pre-job search'
        WHEN zt.created_at BETWEEN a.job_search_start_date AND NVL(a.placement_date::DATE, a.withdrawal_date::DATE, CURRENT_TIMESTAMP) THEN 'Job seeking'
        WHEN zt.created_at > a.placement_date THEN 'Placed'
        WHEN zt.created_at > a.withdrawal_date THEN 'Withdrawn'
        END AS graduate_segment
FROM {{ params.table_refs["zendesk_tickets"] }} zt
LEFT JOIN {{ params.table_refs["zendesk_ss_ticket_forms"] }} tf 
    ON zt.id = tf.ticket_id
LEFT JOIN {{ params.table_refs["fis.students"] }} s
    ON zt.submitter_email = s.email
LEFT JOIN {{ params.table_refs["fis.rosters"] }} r 
    ON s.learn_uuid = r.learn_uuid
    AND zt.created_at BETWEEN r.added_to_cohort_date AND NVL(r.removed_from_cohort_date, CURRENT_TIMESTAMP)
LEFT JOIN {{ params.table_refs["fis.alumni"] }} a
    ON s.learn_uuid = a.learn_uuid
{% endblock %}