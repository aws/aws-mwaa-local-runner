DROP TABLE IF EXISTS {{ params.schema }}.{{ params.table }};
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
    id                      VARCHAR(32)
    , course_id             VARCHAR(32)
    , course_section_id     VARCHAR(32)
    , user_id               VARCHAR(32)
    , type                  VARCHAR(64)
    , created_at            DATETIME
    , updated_at            DATETIME
    , last_activity_at      DATETIME
    , enrollment_state      VARCHAR(32)
    , name                  VARCHAR(256)
    , email                 VARCHAR(256)
    , short_name            VARCHAR(256)
    , sis_user_id           VARCHAR(256)
    , login_id              VARCHAR(256)
)
