{% extends "rotate_table.sql" %}
{% block query %}
SELECT  
	ticket_id 
    , MIN(message_timestamp) AS first_message_sent_at
    , MIN(CASE WHEN is_tc THEN message_timestamp END) AS first_tc_message_sent_at
    , MAX(message_timestamp) AS last_message_sent_at
FROM {{ params.table_refs["zendesk_aaq_transcripts"] }} messages
GROUP BY 
    ticket_id
{% endblock %}