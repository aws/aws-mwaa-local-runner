{% extends "rotate_table.sql" %}
{% block query %}
-- ASSIGNMENTS AND QUIZZES
select pd.sis_user_id student_uuid
       -- names
     , fmcm.blueprint_name
     , fmcm.course_name
     , fmcm.module_name
     , fmcm.module_item_name
     , fmcm.milestone_name 
     ,    fmcm.milestone_name ~ 'Instructor Review: Phase \\d Project' -- software engineering
       or fmcm.milestone_name ~ 'Phase \\d Project Instructor Review' -- software engineering v2
       or fmcm.milestone_name ~ 'Phase \\d Project$' -- data science
       or fmcm.milestone_name in (
                'Module 3 Assessment',  -- product design
                'Module 6 Assessment',  -- product design
                'Module 9 Assessment',  -- product design
                'Module 12 Assessment', -- product design
                'Module 15 Assessment', -- product design
                'Final Capstone Deadline' -- data science
                ) is_project
     -- assignment / quiz info
     , fmcm.module_item_type content_type
     , fmcm.module_position
     , fmcm.module_item_position
     , fmcm.module_item_url
     , fmcm.milestone_ordinality
     , nvl(mid_a.workflow_state, mid_q.workflow_state) item_workflow_state
     , nvl(ad.due_at, qd.due_at) due_at
     , nvl(ad.grading_type, qd.scoring_policy) grading_policy
     , nvl(ad.points_possible, qd.points_possible) points_possible
     -- submission info
     , sd.grade
     , sd.submitted_at 
     , sd.submission_type 
     , sd.workflow_state submission_workflow_state
     , sd.processed is not null includes_attachment
     -- ids
     , fmcm.course_canvas_id
     , fmcm.module_canvas_id 
     , fmcm.module_item_canvas_id
     , ad.canvas_id assignment_canvas_id
     , qd.canvas_id quiz_canvas_id
     , sd.canvas_id submission_canvas_id
     , fmcm.milestone_id
     , fmcm.milestone_uuid
from canvas.submission_dim sd 
left join canvas.assignment_dim ad 
    on sd.assignment_id = ad.id
left join canvas.quiz_dim qd 
    on ad.id = qd.assignment_id
left join canvas.quiz_submission_dim qsd
    on sd.quiz_submission_id = qsd.id 
left join canvas.module_item_dim mid_a 
    on mid_a.assignment_id = ad.id 
left join canvas.module_item_dim mid_q 
    on mid_q.quiz_id = qd.id
left join canvas.user_dim ud
    on sd.user_id = ud.id
left join canvas.pseudonym_dim pd 
    on ud.id = pd.user_id 
left join canvas.course_dim cd 
    on nvl(mid_a.course_id, mid_q.course_id) = cd.id
left join {{ params.table_refs["milestone_canvas_mappings"] }} fmcm  
    on nvl(mid_a.canvas_id, mid_q.canvas_id) = fmcm.module_item_canvas_id
    and cd.canvas_id = fmcm.course_canvas_id 
where fmcm.course_name is not null
union all    
-- PAGES
select pd.sis_user_id student_uuid
       -- names
     , fmcm.blueprint_name
     , fmcm.course_name
     , fmcm.module_name
     , fmcm.module_item_name
     , fmcm.milestone_name 
     ,    fmcm.milestone_name ~ 'Phase \\d-Milestone 4' 
       or fmcm.milestone_name ~ 'Phase \\d Milestone 4' is_project -- cybersecurity engineering 
     -- assignment / quiz info
     , fmcm.module_item_type content_type
     , fmcm.module_position
     , fmcm.module_item_position
     , fmcm.module_item_url
     , fmcm.milestone_ordinality
     , mid.workflow_state item_workflow_state
     , null due_at
     , mcrd.requirement_type grading_policy
     , 1 points_possible
     -- submission info
     , case when mpd.workflow_state not in ('locked', 'unlocked') then '1' else '0' end grade
     , mpd.completed_at submitted_at
     , null submission_type
     , mpcrd.completion_status submission_workflow_state
     , null includes_attachment
     -- ids
     , fmcm.course_canvas_id
     , fmcm.module_canvas_id 
     , fmcm.module_item_canvas_id
     , null assignment_canvas_id
     , null quiz_canvas_id
     , null submission_canvas_id
     , fmcm.milestone_id
     , fmcm.milestone_uuid 
from {{ params.table_refs["milestone_canvas_mappings"] }} fmcm 
left join canvas.course_dim cd 
    on fmcm.course_canvas_id = cd.canvas_id 
left join canvas.module_dim md 
    on fmcm.module_canvas_id = md.canvas_id 
    and cd.id = md.course_id
left join canvas.module_item_dim mid 
    on fmcm.module_item_canvas_id = mid.canvas_id 
left join canvas.module_progression_dim mpd 
    on md.id = mpd.module_id
left join canvas.module_progression_completion_requirement_dim mpcrd
    on mpd.id = mpcrd.module_progression_id 
    and mid.id = mpcrd.module_item_id
left join canvas.module_completion_requirement_dim mcrd 
    on md.id = mcrd.module_id
    and mid.id = mcrd.module_item_id 
left join canvas.user_dim ud 
    on mpd.user_id = ud.id
left join canvas.pseudonym_dim pd 
    on ud.id = pd.user_id 
where fmcm.module_item_type = 'Page'
{% endblock %}
