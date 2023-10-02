{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    le.id AS live_event_id
    , za.participant_id
    , za.name 
    , za.user_email
    , za.join_time
    , za.leave_time
    , za.duration
    , za.learn_uuid
FROM 
    {{ params.table_refs["live_events"] }} le 
LEFT JOIN 
    {{ params.table_refs["zoom_attendees"] }} za 
    ON le.zoom_meeting_id = za.meeting_id
    AND le.scheduled_date::DATE = za.join_time::DATE
{% endblock %}
