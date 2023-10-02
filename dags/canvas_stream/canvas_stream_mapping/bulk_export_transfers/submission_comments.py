from utils import hook 

query = """
create table canvas_consumer.submission_comments as (
select scd.canvas_id id
     , scd.submission_id - 158020000000000000 submission_id
     , ud.canvas_id author_id
     , scd.author_name
     , scd.comment
     , null as time_zone
     , scd.created_at
     , scd.updated_at
from canvas.submission_comment_dim scd
join canvas.user_dim ud
     on scd.author_id = ud.id
);
alter table canvas_consumer.submission_comments owner to airflow;
"""

hook.run(query)