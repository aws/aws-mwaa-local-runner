{% extends "template.sql" %}
{% block query %}
SELECT
    module_item_id::integer AS id
    , module_id::integer
    , body_context_id::integer AS course_id
    , NULL AS title
    , position
    , workflow_state
    , event_name
    , event_time
FROM {{ params["dest_schema"] }}_streaming.module_item_created
UNION ALL
SELECT
    module_item_id::integer
    , module_id::integer
    , body_context_id::integer
    , NULL AS title
    , position
    , workflow_state
    , event_name
    , event_time
FROM {{ params["dest_schema"] }}_streaming.module_item_updated
{% endblock %}