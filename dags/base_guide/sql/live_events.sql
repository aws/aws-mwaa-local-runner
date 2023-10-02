{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    le.id
    , le.created_at
    , le.title 
    , le.description
    , le.slug
    , le.speaker
    , CONVERT_TIMEZONE('America/New_York', le.scheduled_date) AS scheduled_date
    , le.scheduled_duration_seconds
    , le.location
    , le.remote_meeting_link
    , REGEXP_SUBSTR(le.remote_meeting_link, 'flatironschool\.zoom\.us/j/(\\d+)',1,1,'e') AS zoom_meeting_id
    , le.in_person_location
    , pd.name AS discipline
    , LISTAGG(DISTINCT pp.name, ', ') AS phase
    , LISTAGG(DISTINCT pm.name, ', ') AS module
    , i.name AS institution
    , let.name AS event_type
    , zm.reported_participant_count
    , zm.identified_participant_count
FROM 
    {{ params.table_refs["service_content.live_events"] }} le 
LEFT JOIN 
    {{ params.table_refs["service_content.live_events_program_disciplines"] }} lepd 
    ON le.id = lepd.live_event_id 
LEFT JOIN 
    {{ params.table_refs["service_content.program_disciplines"] }} pd 
    ON lepd.program_discipline_id = pd.id 
LEFT JOIN 
    {{ params.table_refs["service_content.live_events_program_phases"] }} lepp
    ON le.id = lepp.live_event_id 
LEFT JOIN 
    {{ params.table_refs["service_content.program_phases"] }} pp 
    ON lepp.program_phase_id = pp.id
LEFT JOIN 
    {{ params.table_refs["service_content.live_events_program_modules"] }} lepm 
    ON le.id = lepm.live_event_id 
LEFT JOIN 
    {{ params.table_refs["service_content.program_modules"] }} pm
    ON lepm.program_module_id  = pm.id 
LEFT JOIN 
    {{ params.table_refs["service_content.institutions_live_events"] }} ile 
    ON le.id = ile.live_event_id 
LEFT JOIN 
    {{ params.table_refs["service_content.institutions"] }} i 
    ON ile.institution_id = i.id 
LEFT JOIN 
    {{ params.table_refs["service_content.live_event_locations"] }} lel 
    ON le.id = lel.live_event_id 
LEFT JOIN 
    {{ params.table_refs["service_content.locations"] }} l
    ON lel.location_id = l.id
LEFT JOIN 
    {{ params.table_refs["service_content.live_events_live_event_types"] }} lelet
    ON le.id = lelet.live_event_id
LEFT JOIN 
    {{ params.table_refs["service_content.live_event_types"] }} let
    ON lelet.live_event_type_id = let.id
LEFT JOIN
    {{ params.table_refs["zoom_meetings"] }} zm 
    ON REGEXP_SUBSTR(le.remote_meeting_link, 'flatironschool\.zoom\.us/j/(\\d+)',1,1,'e') = zm.id
    AND CONVERT_TIMEZONE('America/New_York', le.scheduled_date)::DATE = zm.actual_start_time::DATE
GROUP BY 
    le.id
    , le.created_at
    , le.title 
    , le.description
    , le.slug
    , le.speaker
    , CONVERT_TIMEZONE('America/New_York', le.scheduled_date)
    , le.scheduled_duration_seconds
    , le.location
    , le.remote_meeting_link
    , le.in_person_location
    , pd.name 
    , i.name 
    , let.name
    , zm.reported_participant_count
    , zm.identified_participant_count
{% endblock %}
