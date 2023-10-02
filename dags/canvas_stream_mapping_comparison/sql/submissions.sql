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
from canvas_consumer_submissions.submission_created sc
union all
select submission_id - 158020000000000000 id 
     , assignment_id - 158020000000000000 assignment_id
     , body_user_id - 158020000000000000 user_id
     , attempt
     , grade
     , graded_at
     , first_value(case when nvl(context_role, '') = 'TeacherEnrollment' then user_id else null end) 
       over(partition by submission_id, body_user_id 
            order by case 
                       when nvl(context_role, '') = 'TeacherEnrollment' 
                       then event_time 
                       else null end
            desc nulls last
            rows between unbounded preceding and unbounded following) - 158020000000000000 grader_id
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
from canvas_consumer_submissions.submission_updated su
{% endblock %}
{% set id = ["id", "assignment_id", "user_id"] %}