CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    submission_id   VARCHAR(512)
    , form_id       VARCHAR(512)
    , field_id      VARCHAR(512)
    , label         VARCHAR(1024)
    , value         VARCHAR(MAX)
    , type          VARCHAR(256)
)
