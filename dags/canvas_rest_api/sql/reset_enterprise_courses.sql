DROP TABLE IF EXISTS {{ params.schema }}.{{ params.table }};
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
    id                  VARCHAR(32)
    , account_id        VARCHAR(32)
    , name              VARCHAR(256)
    , created_at        DATETIME
    , workflow_state    VARCHAR(32)
)
