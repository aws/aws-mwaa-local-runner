DROP TABLE IF EXISTS {{ params.schema }}.{{ params.table }};
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
    id                      VARCHAR(32)
    , name                  VARCHAR(256)
    , workflow_state        VARCHAR(32)
    , parent_account_id     VARCHAR(32)
    , root_account_id       VARCHAR(32)
)
