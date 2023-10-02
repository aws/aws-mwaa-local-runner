{% extends "rotate_table.sql" %}
{% block query %}
WITH numbers AS (
    SELECT
        p0.n
        + p1.n*2
        + p2.n * POWER(2,2)
        + p3.n * POWER(2,3)
        + p4.n * POWER(2,4)
        + p5.n * POWER(2,5)
        + p6.n * POWER(2,6)
        AS n
    FROM
        -- create cartesian join to make list from 0 to 127 using binary format
        (SELECT 0 AS n UNION SELECT 1) p0,
        (SELECT 0 AS n UNION SELECT 1) p1,
        (SELECT 0 AS n UNION SELECT 1) p2,
        (SELECT 0 AS n UNION SELECT 1) p3,
        (SELECT 0 AS n UNION SELECT 1) p4,
        (SELECT 0 AS n UNION SELECT 1) p5,
        (SELECT 0 AS n UNION SELECT 1) p6
),
messages AS (
    SELECT
        tc.ticket_id AS ticket_id
        , tc.id AS ticket_comment_id 
        , tc.created_at AS ticket_comment_created_at
        , n.n::INT AS message_order
        , NULLIF(REGEXP_INSTR(body, '\\(\\d{1,2}:\\d{2}:\\d{2}.\\w{2}\\)\\s', 1, n.n::INT, 0), 0) AS n_0
        , NVL(NULLIF(REGEXP_INSTR(body, '\\(\\d{1,2}:\\d{2}:\\d{2}.\\w{2}\\)\\s', 1, n.n::INT + 1, 0), 0), LEN(body) + 1) AS n_1
        , SUBSTRING(body, n_0-1, n_1 - n_0) AS message_body
        , CAST(REGEXP_SUBSTR(message_body, '(\\d{1,2}:\\d{2}:\\d{2})',1,1,'e')||' '||REGEXP_SUBSTR(message_body, '\\d{1,2}:\\d{2}:\\d{2}.(AM|PM)',1,1,'e') AS TIME) AS message_time
    FROM 
        numbers AS n,
        zendesk.ticket_comments AS tc
    WHERE 
        tc.via__channel = 'chat_transcript'
        AND message_body != ''
)
SELECT 
	ticket_id 
    , ticket_comment_id
    , message_order
    , CONVERT_TIMEZONE(
        'America/New_York',
        CAST(
            CASE 
            WHEN CAST(ticket_comment_created_at AS TIME) < message_time
            THEN ticket_comment_created_at::DATE - 1 
            ELSE ticket_comment_created_at::DATE 
            END || ' ' || message_time AS TIMESTAMP)
        ) AS message_timestamp
    , TRIM(SUBSTRING(message_body, CHARINDEX(')', message_body)+1)) AS message_text
    , SUBSTRING(message_text, 1, NULLIF(CHARINDEX(':', message_text), 0) - 1) AS author_alias
    , MAX(SUBSTRING(message_text, 5, NULLIF(CHARINDEX('joined the chat', message_text), 0) - 6)) OVER (PARTITION BY ticket_id) != author_alias AS is_tc
FROM messages
{% endblock %}