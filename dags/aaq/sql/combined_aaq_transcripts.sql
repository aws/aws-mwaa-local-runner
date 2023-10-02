{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    z.ticket_id AS ticket_id
    , 'Zendesk' AS source_system
    , z.ticket_comment_id AS ticket_comment_id
    , z.message_order AS message_order
    , z.message_timestamp AS message_timestamp
    , z.message_text AS message_text
    , z.author_alias AS message_author
    , z.is_tc
FROM
    {{ params.table_refs["zendesk_aaq_transcripts"] }} z 
{% endblock %}