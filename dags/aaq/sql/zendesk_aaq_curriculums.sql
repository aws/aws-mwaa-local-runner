{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    t.ticket_id
    , t.content_url
    , CASE 
        WHEN a.subaccount1 NOT IN ('Live', 'Flex') THEN a.subaccount1
        END AS campus
	, a.subaccount2 AS discipline
	, CASE 
        WHEN a.subaccount3 IN ('Full Time', 'Part Time', 'Self Paced') THEN a.subaccount3
        END AS pacing
    , CASE
        WHEN a.subaccount1 = 'Online' THEN 'Online'
        WHEN a.subaccount1 IN ('Live', 'Flex') THEN a.subaccount1
        WHEN a.subaccount1 IS NOT NULL THEN 'Campus'
        END AS modality
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
LEFT JOIN 
    canvas.account_dim a 
    ON c.account_id = a.id 
{% endblock %}