from utils import hook 

query = """
CREATE TABLE canvas_consumer.module_items AS
SELECT
    canvas_id::integer AS id
    , (module_id - 158020000000000000)::integer AS module_id
    , (course_id - 158020000000000000)::integer AS course_id
    , title
    , position
    , workflow_state
    , 'module_item_created' AS event_name
    , updated_at
FROM canvas.module_item_dim
"""

hook.run(query)