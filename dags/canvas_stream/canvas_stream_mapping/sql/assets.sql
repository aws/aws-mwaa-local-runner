{% extends "template.sql" %}
{% block query %}
select user_id
     , event_time updated_at
     , asset_type
     , asset_subtype
     , asset_name
     , asset_id - 158020000000000000 asset_id
     , context_account_id - 158020000000000000 account_id 
     , context_id - 158020000000000000 context_id
     , case when regexp_substr(url, 'module_item_id=(\\d+)', 1, 1, 'e') != ''
            then regexp_substr(url, 'module_item_id=(\\d+)', 1, 1, 'e')
       end module_item_id
     , user_sis_id student_uuid
     , asset_subtype is not null content_list
     , category
     , role
     , level
     , url
     , user_agent
     , session_id
     , request_id
     , client_ip
     , time_zone
from {{ params["dest_schema"] }}_streaming.asset_accessed
{% endblock %}
{% set id = ["user_id", "updated_at", "asset_type", "asset_subtype"] %}
{% set derived = True %}