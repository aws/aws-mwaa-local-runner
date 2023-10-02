{% extends "rotate_table.sql" %}
{% block query %}
SELECT 
    zt.id AS ticket_id
    , 'Zendesk' AS source_system
    , zt.created_at
    , zt.assignee_name AS tc_name
    , zt.assignee_email AS tc_email
    , zt.submitter_name AS student_name
    , zt.submitter_email AS student_email
    , zss.campus
	, zss.discipline
	, zss.pacing
    , zss.modality
    , zss.student_segment
    , zss.graduate_segment
    , zac.content_url
    , zac.lms_name
    , zac.course_id
    , zac.course_name
    , zac.lesson_id
    , zac.lesson_name
    , zaf.first_message_sent_at 
    , zaf.first_tc_message_sent_at 
    , zaf.last_message_sent_at  
FROM
    {{ params.table_refs["zendesk_tickets"] }} zt
LEFT JOIN 
    {{ params.table_refs["zendesk_aaq_facts"] }} zaf 
    ON zt.id = zaf.ticket_id
LEFT JOIN 
    {{ params.table_refs["zendesk_student_segments"] }} zss
    ON zt.id = zss.ticket_id
LEFT JOIN 
    {{ params.table_refs["zendesk_aaq_curriculums"] }} zac
    ON zt.id = zac.ticket_id
WHERE
    zt.via__channel = 'chat'
    AND zt.submitter_email <> 'dominique.a.deleon@gmail.com'
{% endblock %}