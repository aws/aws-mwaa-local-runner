{% extends "template.sql" %}
{% block query %}
select id
     , account_id 
     , "name" 
     , first_value(created_at) 
       over(partition by id, account_id 
            order by created_at nulls last 
            rows between unbounded preceding and unbounded following) created_at
     , event_time
     , event_name
     , first_value(time_zone) 
       over(partition by id, account_id 
            order by time_zone nulls last 
            rows between unbounded preceding and unbounded following) time_zone
     , workflow_state 
from (
     select course_id  - 158020000000000000 id
          , account_id - 158020000000000000 account_id
          , name
          , CONVERT_TIMEZONE (time_zone, 'UTC', created_at) created_at
          , event_time
          , event_name 
          , time_zone
          , workflow_state
     from {{ params["dest_schema"] }}_streaming.course_created cc
     union all
     select course_id  - 158020000000000000 id
          , account_id - 158020000000000000 account_id
          , name
          , null created_at
          , event_time
          , event_name 
          , null time_zone
          , workflow_state
     from {{ params["dest_schema"] }}_streaming.course_updated cu
)
{% endblock %}
{% set id = ["id", "account_id"] %}