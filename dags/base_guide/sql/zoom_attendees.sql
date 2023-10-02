{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    rmp.meeting_id 
    , rmp.id AS participant_id
    , rmp.name 
    , rmp.user_email
    , CONVERT_TIMEZONE('America/New_York', rmp.join_time) AS join_time
    , CONVERT_TIMEZONE('America/New_York', rmp.leave_time) AS leave_time
    , rmp.duration
    , u.learn_uuid
FROM 
    {{ params.table_refs["stitch_zoom.report_meeting_participants"] }} rmp 
LEFT JOIN 
    {{ params.table_refs["learn.users"] }} u 
    ON rmp.user_email = u.email
{% endblock %}
