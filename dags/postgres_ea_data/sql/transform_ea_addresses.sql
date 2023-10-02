{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    compliance_event_id
    , envelope_id
    , requirement_uuid
    , actor_uuid
    , recipient_uuid 
    , MAX(CASE WHEN tabLabel = 'Home Street Address' THEN value END) AS home_address
    , MAX(CASE WHEN tabLabel = 'Home City' THEN value END) AS home_city
    , MAX(CASE WHEN tabLabel = 'Home State' THEN value END) AS home_state
    , MAX(CASE WHEN tabLabel = 'Home Zip' THEN value END) AS home_zip
    , updated_at
FROM {{ params["raw_data"].schema_in_env }}.{{ params["raw_data"].table_in_env }}
GROUP BY 
    compliance_event_id 
    , envelope_id
    , requirement_uuid
    , actor_uuid
    , recipient_uuid
    , updated_at
{% endblock %}