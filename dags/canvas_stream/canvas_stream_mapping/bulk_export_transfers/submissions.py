from utils import hook 

query = """
create table canvas_consumer.submissions as (
select sd.canvas_id id
     , sd.assignment_id - 158020000000000000 assignment_id
     , ud.canvas_id user_id
     , sd.attempt
     , sd.grade
     , sd.graded_at
     , gd.canvas_id grader_id
     , false late
     , false missing
     , null::int score
     , sd.submission_type
     , sd.submitted_at
     , sd.updated_at
     , url::varchar(4000)
     , null::varchar(4000) body_url
     , sd.workflow_state
     , null::varchar(24) time_zone
from canvas.submission_dim sd
join canvas.user_dim ud
     on sd.user_id = ud.id
left join canvas.user_dim gd
     on sd.grader_id = gd.id
)
"""

hook.run(query)