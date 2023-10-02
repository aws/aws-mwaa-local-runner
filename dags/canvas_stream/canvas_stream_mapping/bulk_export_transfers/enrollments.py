from utils import hook 

query = """
CREATE TABLE canvas_consumer.enrollments AS
SELECT
    ed.canvas_id AS id
    , ed.course_id - 158020000000000000 AS course_id
    , ed.course_section_id - 158020000000000000 AS course_section_id
    , ud.canvas_id AS user_id
    , ed.type
    , ed.workflow_state
    , 'enrollment_created' AS event_name
    , ed.updated_at
FROM canvas.enrollment_dim ed
join canvas.user_dim ud
on ed.user_id = ud.id
"""

hook.run(query)