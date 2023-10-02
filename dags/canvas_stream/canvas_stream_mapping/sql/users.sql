{% extends "template.sql" %}
{% block query %}
select body_user_id - 158020000000000000 id
     , body_user_login student_uuid
     , name 
     , event_name
     , time_zone
     , event_time
     , workflow_state
from {{ params["dest_schema"] }}_streaming.user_created uc 
union all
select body_user_id - 158020000000000000  id
     , body_user_login student_uuid
     , name 
     , event_name
     , time_zone
     , event_time
     , workflow_state
from {{ params["dest_schema"] }}_streaming.user_updated
{% endblock %}
{% set id = ["id"] %}