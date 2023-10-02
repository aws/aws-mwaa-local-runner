CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    id                      VARCHAR(512)
    , form_id               VARCHAR(512)
    , submission_timestamp  TIMESTAMP
    , user_agent            VARCHAR(512)
    , remote_addr           VARCHAR(256)
    , latitude              VARCHAR(256)
    , longitude             VARCHAR(256)
    , approval_status       VARCHAR(16)
    , PRIMARY KEY (id))
