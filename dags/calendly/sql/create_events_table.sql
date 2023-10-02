CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
      id                VARCHAR(512)
    , event_type_id       VARCHAR(512)
    , created_at          VARCHAR(128)
    , start_time          VARCHAR(128)
    , end_time            VARCHAR(128)
    , invitee_total       INTEGER
    , invitee_active      INTEGER
    , host_name           VARCHAR(512)
    , host_email          VARCHAR(512)
    , host_timezone       VARCHAR(128)
    , zoom_url            VARCHAR(128)
    , name                VARCHAR(512)
    , status              VARCHAR(32)
    , invitee_id          VARCHAR(512)
    , PRIMARY KEY(id)
);