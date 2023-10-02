SELECT
     learn_uuid
   , ud.canvas_id::varchar
FROM
    "fis"."students" AS "students"
LEFT JOIN canvas.pseudonym_dim pd
    ON students.learn_uuid = pd.sis_user_id
LEFT JOIN canvas.user_dim ud
    ON pd.user_id = ud.id
WHERE (( CASE
        WHEN CURRENT_DATE BETWEEN students.most_recent_day_1_start_date::DATE 
            AND students.most_recent_cohort_end_date::DATE
        THEN 'Active'
        WHEN students.student_status = 'matriculated'
        THEN 'Active'
        WHEN students.most_recent_day_1_start_date::DATE < CURRENT_DATE 
            AND students.most_recent_cohort_pacing = 'Self Paced'
        THEN 'Active'
        ELSE 'Other'
        END
       ) ILIKE  'Active')