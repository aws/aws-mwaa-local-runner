{% extends "rotate_table.sql" %}
{% block query %}
SELECT
    form_id
    , submission_id
    , submission_timestamp
    , 'Community Event' AS survey_name
    , learn_uuid
    , email
    , discipline
    , pace
    , start_date
    , speaker
    , event_title
    , event_csat
    , change_about_event_text
    , main_takeaway_text
FROM 
    (
        SELECT form_id, submission_id, 1 AS phase, submission_timestamp, label, value 
        FROM {{ params.table_refs["formstack.community_event_merged"] }}
	) PIVOT (
        MAX(value) for label IN (
            'Learn UUID' AS learn_uuid
            , 'Email' AS email
            , 'Discipline' AS discipline
            , 'Pace' AS pace
            , 'Start Date' AS start_date
            , 'Speaker' AS speaker
            , 'Title' AS event_title
            , 'I am satisfied with the quality of the event I just attended.' AS event_csat
            , 'What is one thing you would change about the event?' AS change_about_event_text
            , 'What was your main takeaway from this event?' AS main_takeaway_text
        )
    )
{% endblock %}
