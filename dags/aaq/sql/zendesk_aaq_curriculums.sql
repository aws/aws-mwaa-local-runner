{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    t.ticket_id
    , t.content_url
    , CASE 
        WHEN t.canvas_course_id IS NOT NULL THEN 'Canvas'
        END AS lms_name
    , t.canvas_course_id AS course_id
    , c.name AS course_name
    , t.canvas_module_item_id AS lesson_id
    , mi.title AS lesson_name
FROM 
    (
        SELECT 
            t.id AS ticket_id
            , NULLIF(REGEXP_SUBSTR(t.description, '(https://[\%\?\=\.\/\\w-]*)', 1, 1, 'e'), '') AS content_url
            , NULLIF(REGEXP_SUBSTR(t.description, 'courses/(\\d+)', 1, 1, 'e'), '') AS canvas_course_id
            , NULLIF(REGEXP_SUBSTR(t.description, 'module_item_id=(\\d+)', 1, 1, 'e'), '') AS canvas_module_item_id
        FROM zendesk.tickets t
        WHERE t.via__channel = 'chat'
    ) AS t
LEFT JOIN 
    canvas.course_dim c 
    ON t.canvas_course_id = c.canvas_id
LEFT JOIN 
    canvas.module_item_dim mi
    ON t.canvas_module_item_id = mi.canvas_id
{% endblock %}