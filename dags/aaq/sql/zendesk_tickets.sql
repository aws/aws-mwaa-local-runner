{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    t.id
    , CONVERT_TIMEZONE('America/New_York', t.created_at) AS created_at
    , CONVERT_TIMEZONE('America/New_York', t.updated_at) AS updated_at
    , t.assignee_id
    , groups.name AS group_name
    , forms.name AS ticket_form
    , assignee.name AS assignee_name
    , assignee.email AS assignee_email
    , t.requester_id
    , requester.name AS requester_name
    , requester.email AS requester_email
    , t.submitter_id
    , submitter.name AS submitter_name
    , submitter.email AS submitter_email
    , t.recipient
    , t.via__channel
    , t.priority
    , t.status
    , t.subject
    , t.description
    , t.satisfaction_rating__score
    , t.satisfaction_rating__comment
FROM zendesk.tickets t
LEFT JOIN zendesk.users submitter 
    ON t.submitter_id = submitter.id
LEFT JOIN zendesk.users requester 
    ON t.requester_id = requester.id
LEFT JOIN zendesk.users assignee 
    ON t.assignee_id = assignee.id
LEFT JOIN zendesk.ticket_forms forms 
    ON t.ticket_form_id = forms.id
LEFT JOIN zendesk.groups
    ON t.group_id = groups.id
{% endblock %}
