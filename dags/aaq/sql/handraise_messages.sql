{% extends "rotate_table.sql" %}
{% block query %}
WITH rooms AS (
    SELECT 
        r.id AS room_id 
        , JSON_EXTRACT_PATH_TEXT(r.policy, 'handraise_question_id') AS hq_id
    FROM {{ params.table_refs["learn.rooms"] }} r 
    WHERE LOWER(r.policy) LIKE '%handraise_question_id%'
)  
SELECT 
    r.room_id 
    , r.hq_id
    , m.id AS message_id
    , ROW_NUMBER() OVER (PARTITION BY r.room_id ORDER BY m.created_at) AS row_num
    , CONVERT_TIMEZONE('America/New_York', m.created_at) AS created_at
    , u.id AS user_id
    , u.learn_uuid 
    , m.content 
    , u.first_name || ' ' || u.last_name AS messenger_name
FROM 
    {{ params.table_refs["learn.messages"] }} m
LEFT JOIN 
    rooms r 
    ON m.room_id = r.room_id
LEFT JOIN 
    {{ params.table_refs["learn.users"] }} u 
    ON m.user_id = u.id
{% endblock %}