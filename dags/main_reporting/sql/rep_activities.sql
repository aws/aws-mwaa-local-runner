{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    u.name AS owner_name
    , t.accountid AS account_id 
    , t.whatid AS what_id
    , t.whoid AS who_id 
    , t.id AS task_id
    , t.createddate AS activity_date 
    , t.calldurationinseconds AS call_duration_in_seconds 
    , t.calltype AS call_type
    , t.description 
    , t.status 
    , t.subject 
    , t.tasksubtype AS task_subtype
    , t.type
FROM 
    {{ params.table_refs["stitch_salesforce.task"] }} t 
LEFT JOIN 
    {{ params.table_refs["stitch_salesforce.user"] }} u
    ON t.ownerid = u.id
WHERE 
    NOT t.isdeleted
{% endblock %}