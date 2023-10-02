{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    sg_template_name
    , sg_template_id
    , email
    , MAX(CASE WHEN event = 'processed' THEN TIMESTAMP 'epoch' + "timestamp"  * INTERVAL '1 second' END) processed
    , MAX(CASE WHEN event = 'delivered' THEN TIMESTAMP 'epoch' + "timestamp"  * INTERVAL '1 second' END) delivered
    , MAX(CASE WHEN event = 'open' THEN TIMESTAMP 'epoch' + "timestamp"  * INTERVAL '1 second' END) opened
    , MAX(CASE WHEN event = 'click' THEN TIMESTAMP 'epoch' + "timestamp"  * INTERVAL '1 second' END) clicked
FROM stitch_sendgrid.events e
GROUP BY 1, 2, 3
{% endblock %}