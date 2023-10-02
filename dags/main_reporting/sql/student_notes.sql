{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    'Learn' AS note_source
    , COALESCE(ib.created_at, ia.date) AS created_date 
    , COALESCE(ib.updated_at, ia.updated_at) AS updated_date
    , COALESCE(ib.student_id, ia.user_id) AS user_id
    , COALESCE(pu2.learn_uuid, ia.student_learn_uuid) AS student_learn_uuid
    , COALESCE(pu2.first_name || ' ' || pu2.last_name, ia.student_name) AS student_name
    , COALESCE(ib.body, ia.body) AS body
    , COALESCE(pu.learn_uuid, ia.admin_learn_uuid) AS admin_learn_uuid
    , COALESCE(pu.first_name || ' ' || pu.last_name, ia.admin_name) AS admin_name
    , LOWER(COALESCE(ib.noteable_type, ia.note_type)) AS note_type
    , current_date AS run_date
    , NULL AS attitude_score
    , NULL AS aptitude_score
FROM 
    {{ params.table_refs["learn.admin_student_notes"] }} ib
FULL OUTER JOIN 
    (
        SELECT 
            notes.date
            , notes.updated_at
            , students.learn_id AS user_id
            , students.learn_uuid AS student_learn_uuid
            , students.first_name || ' ' || students.last_name AS student_name
            , notes.body
            , u.first_name || ' ' || u.last_name AS admin_name
            , u.learn_uuid AS admin_learn_uuid
            , note_type.title AS note_type
            , ROW_NUMBER() OVER (PARTITION BY students.learn_id, notes.body ORDER BY notes.date) row_num
        FROM 
            {{ params.table_refs["instructor_app_db.notes"] }} notes 
        LEFT JOIN 
            {{ params.table_refs["instructor_app_db.note_types"] }} note_type 
            ON notes.note_type_id = note_type.id
        LEFT JOIN 
            {{ params.table_refs["instructor_app_db.students"] }} students 
            ON notes.student_id = students.id
        LEFT JOIN
            {{ params.table_refs["instructor_app_db.users"] }} u
            ON notes.author_id = u.id
        WHERE 
            note_type.title <> 'Imported'
    ) ia 
    ON ib.body = ia.body 
    AND ib.student_id = ia.user_id 
    AND ia.row_num = 1
LEFT JOIN 
    {{ params.table_refs["learn.users"] }} pu 
    ON ib.admin_id = pu.id 
LEFT JOIN 
    {{ params.table_refs["learn.users"] }} pu2
    ON ib.student_id = pu2.id
-- LEFT JOIN 
--     (
--         SELECT DISTINCT user_id 
--         FROM {{ params.table_refs["learn.user_roles"] }} 
--         WHERE role_id = 4
--     ) ur
--     ON ib.admin_id = ur.user_idUNION ALL
UNION ALL 
SELECT 
    'Guide' AS note_source
    , n.created_at AS created_date
    , n.updated_at AS updated_date
    , NULL AS user_id
    , n.student_uuid AS student_learn_uuid
    , lu.first_name || ' ' || lu.last_name AS student_name
    , n.content_text AS body 
    , n.author_uuid AS admin_learn_uuid 
    , lu2.first_name || ' ' || lu2.last_name AS admin_name
    , c."name" AS note_type 
    , current_date AS run_date
    , n.attitude AS attitude_score
    , n.aptitude AS aptitude_score
FROM {{ params.table_refs["service_educator.notes"] }} n 
LEFT JOIN 
	{{ params.table_refs["service_educator.categories"] }} c
	ON n.category_id =c.id 
LEFT JOIN 
    {{ params.table_refs["learn.users"] }} lu 
    ON n.student_uuid = lu.learn_uuid 
LEFT JOIN 
    {{ params.table_refs["learn.users"] }} lu2
    ON n.author_uuid = lu2.learn_uuid 
{% endblock %}
