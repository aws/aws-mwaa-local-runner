create table if not exists {{ params.schema }}.{{ params.table }} (
    learn_uuid      VARCHAR(240)
  , course_name     VARCHAR(240)
  , points_possible VARCHAR(12)
  , grading_type    VARCHAR(240)
  , assignment_name VARCHAR(240)
  , submission_id   VARCHAR(240)
  , workflow_state  VARCHAR(40)
  , submitted_at    TIMESTAMP
  , user_id         VARCHAR(240)
  , submission_type VARCHAR(240)
  , attempt         INTEGER 
  , late            BOOLEAN
  , speedgrader_url VARCHAR(1240)
);