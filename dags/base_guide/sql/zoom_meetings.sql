{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    m.id 
    , m.join_url
    , CONVERT_TIMEZONE('America/New_York', m.start_time) AS scheduled_datetime
    , m.timezone
    , m.type 
    , DECODE(m.type, 1, 'Instant meeting', 2, 'Scheduled meeting', 3, 'Recurring w/o fixed time', 8, 'Recurring w/ fixed time') AS type_description
    , rm.topic
    , CONVERT_TIMEZONE('America/New_York', rm.start_time) AS actual_start_time
    , CONVERT_TIMEZONE('America/New_York', rm.end_time) AS actual_end_time
    , rm.duration
    , rm.participants_count AS reported_participant_count
    , COUNT(DISTINCT za.learn_uuid) AS identified_participant_count
    , rm.user_email
    , rm.user_name
FROM 
    {{ params.table_refs["stitch_zoom.meetings"] }} m 
LEFT JOIN 
    {{ params.table_refs["stitch_zoom.report_meetings"] }} rm 
    ON m.id = rm.meeting_id
LEFT JOIN 
    {{ params.table_refs["zoom_attendees"] }} za 
    ON m.id = za.meeting_id
    AND CONVERT_TIMEZONE('America/New_York', rm.start_time)::DATE = CONVERT_TIMEZONE('America/New_York', za.join_time)::DATE
GROUP BY 
    m.id 
    , m.join_url
    , CONVERT_TIMEZONE('America/New_York', m.start_time)
    , m.timezone
    , m.type 
    , DECODE(m.type, 1, 'Instant meeting', 2, 'Scheduled meeting', 3, 'Recurring w/o fixed time', 8, 'Recurring w/ fixed time')
    , rm.topic
    , CONVERT_TIMEZONE('America/New_York', rm.start_time)
    , CONVERT_TIMEZONE('America/New_York', rm.end_time)
    , rm.duration
    , rm.participants_count
    , rm.user_email
    , rm.user_name
{% endblock %}
