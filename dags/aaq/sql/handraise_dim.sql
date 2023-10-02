{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    hq.id
    , CONVERT_TIMEZONE('America/New_York', hq.created_at) AS created_at
    , CONVERT_TIMEZONE('America/New_York', hq.resolved_at) AS resolved_at
    , hq.resolver_id
    , u2.learn_uuid AS resolver_learn_uuid
    , u2.email AS resolver_email
    , u2.first_name || ' ' || u2.last_name AS resolver_name
    , hq.batch_id
    , hq.lesson_id
    , hq.track_id
    , hq.user_id
    , u.learn_uuid AS student_learn_uuid
    , u.first_name || ' ' || u.last_name AS student_name
    , u.email AS student_email
    , hq.content AS handraise_message_title
    , lesson.title AS lesson_title
    , unit.title AS unit_title
    , CASE
        WHEN track.title ~* '(intro)|(prep)|(fundamental)|(foundation)' THEN 'Free Courses'
        ELSE track.title
        END AS track_title
    , CASE 
        WHEN b.iteration = 'v-000' THEN 'Online'
        ELSE c.campus 
        END AS campus
    , CASE 
        WHEN b.iteration = 'v-000' THEN 'Online'
        ELSE c.modality
        END AS modality
    , CASE 
        WHEN b.iteration = 'v-000' THEN 'Software Engineering'
        WHEN track.title ~* '(web)|(software)' THEN 'Software Engineering'
        ELSE c.discipline
        END AS discipline
    , CASE 
        WHEN b.iteration = 'v-000' THEN 'Self Paced' 
        ELSE c.pacing
        END AS pacing
    , hq.feedback
    , CONVERT_TIMEZONE('America/New_York', hq.last_expert_reply_at) AS last_expert_reply_at
    , CONVERT_TIMEZONE('America/New_York', hq.last_student_reply_at) AS last_student_reply_at
    , CONVERT_TIMEZONE('America/New_York', hq.response_received_at) AS response_received_at
FROM 
    {{ params.table_refs["learn.handraise_questions"] }} hq
JOIN 
    {{ params.table_refs["learn.users"] }} u
    ON hq.user_id = u.id
JOIN 
    {{ params.table_refs["learn.users"] }} u2
    ON hq.resolver_id = u2.id
JOIN 
    {{ params.table_refs["learn.curriculums"] }} lesson 
    ON hq.lesson_id = lesson.id
JOIN 
    {{ params.table_refs["learn.curriculums"] }} unit 
    ON lesson.parent_id = unit.id
JOIN 
    {{ params.table_refs["learn.curriculums"] }} track 
    ON lesson.track_id = track.id
JOIN 
    {{ params.table_refs["learn.batches"] }} b 
    ON hq.batch_id = b.id 
LEFT JOIN 
    {{ params.table_refs["staging.cohorts"] }} c
    ON b.uuid = c.learn_batch_uuid
{% endblock %}