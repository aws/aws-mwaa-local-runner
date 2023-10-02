{% extends "template.sql" %}
{% block query %}
select body_user_id user_id
     , body_user_login student_uuid
     , name
     , nvl(referrer, url) url
     , event_time as updated_at
     , time_zone
     , user_agent
from {{ params["dest_schema"] }}_streaming.user_updated uu 
{% endblock %}
{% set id = ["user_id", "student_uuid", "updated_at", "url"] %}