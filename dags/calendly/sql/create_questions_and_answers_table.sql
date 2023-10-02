CREATE TABLE IF NOT EXISTS {{ params["schema"] }}.{{ params["table"] }} (
    question          VARCHAR(32768)
    , answer            VARCHAR(32768)
    , position          INTEGER
    , event_id          VARCHAR(512)
    , invitee_id        VARCHAR(512)
    , PRIMARY KEY(event_id, invitee_id, position)
);
