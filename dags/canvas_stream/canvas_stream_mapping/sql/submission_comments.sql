{% extends "template.sql" %}
{% block query %}
select 
     submission_comment_id::bigint as id 
     , submission_id::bigint
     , body_user_id::bigint as author_id
     , null as author_name
     , body as comment
     , time_zone
     , created_at
     , event_time
from {{ params["dest_schema"] }}_streaming.submission_comment_created
{% endblock %}
{% set id = ["id"] %}