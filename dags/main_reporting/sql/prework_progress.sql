{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    learn_uuid
    , enrollment_date
    , prework_started_at
    , last_prework_activity_date
    , current_account
    , current_course
    , current_module
    , last_lesson_completed
    , completed_prework_lessons
    , total_prework_lessons
    , completed_hashketball_date
    , pre_test_grade
    , pre_test_submission_date
    , post_test_grade
    , post_test_submission_date
    , MAX(last_prework_activity_date) OVER (PARTITION BY learn_uuid) = last_prework_activity_date AS is_most_recent
FROM
    (
        SELECT 
            learn_uuid
            , MIN(enrollment_date) AS enrollment_date
            , MIN(module_completed_date) AS prework_started_at
            , MAX(last_activity_at) AS last_prework_activity_date
            , MAX(CASE WHEN is_last_active_course = 1 THEN account_name END) AS current_account
            , MAX(CASE WHEN is_last_active_course = 1 THEN course_name END) AS current_course
            , MAX(CASE WHEN is_last_completed_module_item = 1 AND completion_status = 'complete' THEN module_name END) AS current_module
            , MAX(CASE WHEN is_last_completed_module_item = 1 AND completion_status = 'complete' THEN module_item_name END) AS last_lesson_completed
            , COUNT(CASE WHEN is_last_active_course = 1 THEN module_completed_date END) AS completed_prework_lessons
            , COUNT(CASE WHEN is_last_active_course = 1 THEN 1 END) AS total_prework_lessons
            , NULL AS completed_hashketball_date
            , MAX(CASE WHEN module_item_name ~ '(Pre.?(Test|Assessment))' THEN grade::FLOAT/NULLIF(points_possible,0) END) pre_test_grade
            , MAX(CASE WHEN module_item_name ~ '(Pre.?(Test|Assessment))' THEN submitted_at END) AS pre_test_submission_date
            , MAX(CASE WHEN module_item_name ~ '((Post|(Pre.?work)).?(Test|Assessment))' THEN grade::FLOAT/NULLIF(points_possible,0) END) AS post_test_grade
            , MAX(CASE WHEN module_item_name ~ '((Post|(Pre.?work)).?(Test|Assessment))' THEN submitted_at END) AS post_test_submission_date
        FROM 
            (
                SELECT 
                    *
                    , MAX(last_activity_at) OVER (PARTITION BY learn_uuid) = MAX(last_activity_at) OVER (PARTITION BY learn_uuid, course_canvas_id) AS is_last_active_course
                    , 1 = ROW_NUMBER() OVER (
                        PARTITION BY learn_uuid 
                        ORDER BY module_completed_date DESC NULLS LAST, module_position DESC, module_item_position DESC
                        ) AS is_last_completed_module_item
                FROM {{ params.table_refs["canvas_progress"] }}
                WHERE course_name ~* '(Pre.?work)|(Prep)' -- Should capture all Pre-work on Canvas
                    AND enrollment_status IN ('active', 'completed', 'inactive')
            )
        GROUP BY 
            learn_uuid
    ) x
{% endblock %}