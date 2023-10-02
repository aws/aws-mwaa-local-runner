from utils import hook 

query = """
create table canvas_consumer.users as (
select ud.canvas_id id
     , pd.sis_user_id student_uuid
     , ud.name::varchar(4000)
     , 'user_created' event_name
     , ud.time_zone
     , pd.updated_at
     , ud.workflow_state
from canvas.user_dim ud 
left join canvas.pseudonym_dim pd 
on pd.user_id = ud.id
and pd.sis_user_id is not null
and pd.workflow_state <> 'deleted'
)
"""

hook.run(query)