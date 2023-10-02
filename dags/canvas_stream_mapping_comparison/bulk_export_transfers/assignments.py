from utils import hook 

query = """
CREATE TABLE canvas_consumer.assignments AS
SELECT
    canvas_id AS id
    , course_id - 158020000000000000 AS course_id
    , assignment_group_id - 158020000000000000 AS assignment_group_id
    , null::varchar(40) time_zone
    , title
    , description
    , points_possible::float
    , due_at
    , submission_types
    , workflow_state
    , 'assignment_created' AS event_name
    , updated_at
FROM canvas.assignment_dim
"""

hook.run(query)