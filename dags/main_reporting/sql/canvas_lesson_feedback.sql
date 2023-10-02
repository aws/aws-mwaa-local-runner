{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    ratings.course_id
    , ratings.lesson_id AS content_canvas_id
    , ratings.content_type
    , mid.canvas_id AS module_item_canvas_id
    , mid.title AS module_item_title
    , md.name AS module_name
    , pseudonyms.unique_name AS learn_uuid 
    , ratings.feedback AS rating
 	, feedback.submission_id
 	, feedback.submission_timestamp
 	, feedback.response AS feedback
FROM 
    (
        -- Make lesson rating unique
        -- No timestamp, so if student ever gave 'Good' rating, that trumps 'Bad' rating
        SELECT
            clr._row
            , clr.course_id::INT
            , NVL(pages.canvas_id, NULLIF(REGEXP_SUBSTR(clr.lesson_id, '(\\d+)', 1, 1, 'e'),'')::INT) AS lesson_id
            , DECODE(clr.lesson_type,
                'pages', 'WikiPage',
                'assignments', 'Assignment',
                'quizzes', 'Quiz',
                'discussion_topics', 'DiscussionTopic') AS content_type
            , clr.user_id
            , clr.feedback
            , ROW_NUMBER() OVER (PARTITION BY clr.user_id, clr.lesson_id ORDER BY clr.feedback DESC) AS rating_order
        FROM {{ params.table_refs["fis.canvas_lesson_ratings"] }} clr
        LEFT JOIN 
            (
                SELECT 
                    wpf.parent_course_id - 158020000000000000 AS course_id
                    , wpd.canvas_id
                    , wpd.url AS url
                FROM {{ params.table_refs["canvas.wiki_page_fact"] }} wpf 
                JOIN {{ params.table_refs["canvas.wiki_page_dim"] }} wpd
                    ON wpf.wiki_page_id = wpd.id
            ) pages
            ON clr.lesson_id = pages.url 
            AND clr.course_id = pages.course_id
        WHERE clr.lesson_id != 'None'
    ) ratings
LEFT JOIN canvas.module_item_dim mid 
    ON ratings.course_id + 158020000000000000 = mid.course_id
    AND ratings.lesson_id + 158020000000000000 = NVL(mid.assignment_id, discussion_topic_id, quiz_id, wiki_page_id)
LEFT JOIN canvas.module_dim md 
    ON mid.module_id = md.id
LEFT JOIN
    (
        SELECT
            r.submission_id 
            , s.submission_timestamp  
            , r.course_id
            , r.course
            , r.lesson_id
            , r.lesson_type
            , r.user_id
            , r.response
            , ROW_NUMBER() OVER (PARTITION BY r.user_id, r.lesson_id ORDER BY s.submission_timestamp DESC) AS feedback_order
        FROM {{ params.table_refs["formstack.canvas_feedback_submissions"] }} s
        JOIN 
            (
                -- Convert from long to wide data
                SELECT 
                    submission_id
                    , MAX(CASE WHEN label = 'CourseID' THEN value END) AS course_id
                    , MAX(CASE WHEN label = 'Course' THEN value END) AS course
                    , MAX(CASE WHEN label = 'LessonID' THEN value END) AS lesson_id
                    , MAX(CASE WHEN label = 'LessonType' THEN value END) AS lesson_type
                    , MAX(CASE WHEN label = 'CanvasUserID' THEN value END) AS user_id
                    , MAX(CASE WHEN label = 'How can we improve this content?' THEN value END) AS response
                FROM {{ params.table_refs["formstack.canvas_feedback_responses"] }}
                GROUP BY 
                    submission_id 
            ) r
            ON s.id = r.submission_id 
    ) feedback
    ON ratings.lesson_id = feedback.lesson_id 
    AND ratings.user_id = feedback.user_id
    AND feedback.feedback_order = 1
LEFT JOIN 
    {{ params.table_refs["canvas.user_dim"] }} users
    ON ratings.user_id = users.canvas_id
LEFT JOIN 
    {{ params.table_refs["canvas.pseudonym_dim"] }} pseudonyms
    ON users.id = pseudonyms.user_id
	AND pseudonyms.sis_user_id IS NOT NULL
WHERE 
    ratings.rating_order = 1
{% endblock %}