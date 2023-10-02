{% extends "template.sql" %}
{% block query %}
select submission_id - 158020000000000000 id 
     , assignment_id - 158020000000000000 assignment_id
     , user_id - 158020000000000000 user_id
     , attempt
     , grade
     , graded_at
     , null grader_id
     , late
     , missing
     , score::integer
     , submission_type
     , submitted_at
     , event_time
     , url
     , body_url
     , workflow_state
     , null time_zone
from {{ params["dest_schema"] }}_streaming.submission_created sc
union all
select submission_id - 158020000000000000 id 
     , assignment_id - 158020000000000000 assignment_id
     , body_user_id - 158020000000000000 user_id
     , attempt
     , grade
     , graded_at
     , user_id - 158020000000000000 grader_id
     , late
     , missing
     , score::integer
     , submission_type
     , submitted_at
     , event_time
     , url
     , body_url
     , workflow_state
     , time_zone
from {{ params["dest_schema"] }}_streaming.submission_updated su
{% endblock %}
{% set id = ["id", "assignment_id", "user_id"] %}