DROP TABLE IF EXISTS {{ params.schema }}.{{ params.table }};
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
    month           VARCHAR(32)
    , overall_goal  DECIMAL(8, 2)
    , se_goal       DECIMAL(8, 2)
    , ds_goal       DECIMAL(8, 2)
    , cyber_goal    DECIMAL(8, 2)
    , pd_goal       DECIMAL(8, 2)
)
