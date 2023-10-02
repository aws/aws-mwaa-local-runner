from utils import hook 

query = """
create table canvas_consumer.courses as (
select canvas_id id
     , account_id - 158020000000000000 account_id
     , name 
     , created_at created_at
     , created_at updated_at
     , 'course_created' event_name
     , null::VARCHAR(64) time_zone
     , workflow_state
from canvas.course_dim 
)
"""

hook.run(query)